# Analysis of Daemon Synchronization Races & MCP Interaction Anomalies

**Status:** TECHNICAL REPORT · Non-master investigation  
**Branch:** `investigate/hermes-mcp-synchronization-race`  
**Target Repository:** [`~/src/tricorder`](file:///home/nyc/src/tricorder)  
**Related Modules:**  
- `Tricorder.Socket.Server` (`packages/tricorder/src/Tricorder/Socket/Server.hs`)
- `Tricorder.Waiters` (`packages/tricorder/src/Tricorder/Waiters.hs`)
- `Tricorder.Daemon.Core` (`packages/tricorder/src/Tricorder/Daemon/Core.hs`)
- `Tricorder.MCP.Tools` (`packages/tricorder-mcp/src/Tricorder/MCP/Tools.hs`)

---

## 1. Problem Statement & Context

When `tricorder` and its Model Context Protocol daemon bridge (`tricorder-mcp`) are consumed by modern autonomous AI coding assistant REPLs (such as Nous Research's Hermes Agent, Anthropic's Claude Code, or IDE agents like Cursor):

1. Tool invocations querying build status with blocking wait (`status(wait: true)` via MCP, or `tricorder status --wait --json` via CLI) occasionally return stale build results, hang indefinitely, or report spurious non-zero exit codes.
2. In multi-turn assistant loops where code edits and diagnostic queries happen rapidly, race conditions between file-watcher reload triggers, broadcast event channels, and socket handlers cause synchronization errors.
3. This report documents the exact concurrency defects identified in the codebase, explains why they manifest under high-frequency assistant turn programs, and outlines a concrete remediation plan.

---

## 2. Identified Concurrency Defects

### Defect 1: The Missing Subscription Barrier in `respondWhenDone` (`Tricorder.Socket.Server`)

In `packages/tricorder/src/Tricorder/Socket/Server.hs`:

```haskell
respondWhenDone :: Handle -> Eff es ()
respondWhenDone h = awaitResult >>= mkBuildState >>= sendJson h
  where
    awaitResult = Conc.scoped do
        progressP <- Conc.fork $ Sub.listenUntil_ \case
            failed@(Build.Failed _) -> Just failed
            finished@(Build.Finished _ _) -> Just finished
            _ -> Nothing
        s <- State.get
        waitOrStart progressP s

    waitOrStart p = \case
        finished@(Build.Finished _ _) -> pure finished
        failed@(Build.Failed _) -> pure failed
        _ -> Conc.await p
```

#### The Race Mechanism:
1. **Asynchronous Subscription Gap:** `Conc.fork` creates a new green thread for `Sub.listenUntil_`. However, `Conc.fork` returns *immediately* before the new thread is actually scheduled by the runtime and registers its subscription with the Pub/Sub bus.
2. **Stale State Fast-Path:** Concurrently, the parent thread reads `s <- State.get`. If a previous build recently completed, `s` is already `Build.Finished`. If a file edit occurred just milliseconds prior, the watcher or builder may have queued a `ReloadBuilder` event that has not yet transitioned `State` to `Build.Building`. The parent thread immediately enters `waitOrStart _ (Build.Finished ...)` and returns the **stale** build outcome from the previous turn!
3. **Lost Broadcast Wakeup:** If `s` was `Build.Building` when read, but the background GHCi worker finishes the build *before* `progressP`'s thread has attached its listener to the broadcast channel, the `Build.Finished` event is emitted into the channel and discarded. `Conc.await progressP` then blocks forever, causing the agent's MCP call to hit its 180s timeout.

*(Note: A similar subscription race was previously resolved in `Atelier.Effects.Iterator.hs` line 40 with a synchronization barrier, but `respondWhenDone` lacks this protection).*

---

### Defect 2: Time-of-Check to Time-of-Use (TOCTOU) in `Waiters.without` (`Tricorder.Waiters`)

In `packages/tricorder/src/Tricorder/Waiters.hs`:

```haskell
run act = do
    waiters <- newTVarIO (0 :: Int)
    interpretWith act \env -> \case
        With m -> localSeqUnlift env \unlift -> do
            bracket_
                (atomically $ modifyTVar' waiters (+ 1))
                (atomically $ modifyTVar' waiters (max 0 . subtract 1))
                $ unlift m
        Without m -> localSeqUnlift env \unlift -> do
            noWaiters <- atomically do
                n <- readTVar waiters
                pure (n <= 0)
            when noWaiters do
                void $ unlift m
        Wait m -> localSeqUnlift env \unlift -> do
            atomically do
                n <- readTVar waiters
                when (n > 0) retry
            unlift m
```

#### The Race Mechanism:
In `Without m`:
- The thread queries `n <- readTVar waiters` inside an atomic transaction and exits the transaction.
- If `n <= 0`, it proceeds to run `unlift m`.
- Between the moment the transaction commits and `unlift m` executes in IO, a concurrent thread can invoke `With m'`, incrementing `waiters`.
- As a result, the invariant that `Without m` runs strictly in isolation when no waiters exist is broken.

In `Tricorder.Daemon.Core.runSession`:
```haskell
Conc.fork_ $ Sub.listen_ @ReloadBuilder \event -> do
    atomically $ writeTMVar newestReloadEvent event
    Waiters.without do
        Pub.publish Build.Starting
        Log.debug "Cancelling current build"
        requestCancel
```
Because of the TOCTOU gap, `requestCancel` and `Build.Starting` can fire concurrently with active waiters or client socket operations, leading to corrupted state transitions.

---

### Defect 3: Exit-Code Masking & Error Misclassification (`Tricorder.MCP.Tools`)

In `packages/tricorder-mcp/src/Tricorder/MCP/Tools.hs`:

```haskell
reportsBuildOutcome :: Tool -> Bool
reportsBuildOutcome (Status _) = True
reportsBuildOutcome (TestResults _) = True
reportsBuildOutcome (EvalComments _) = True
reportsBuildOutcome _ = False

handleTool _ tool = do
    ...
    outcome <- try @IOException $ readProcess $ setWorkingDir directory $ proc "tricorder" args
    pure $ case outcome of
        Left ex -> toolError $ "Failed to run tricorder: " <> show ex
        Right (ExitSuccess, out, _) -> toolResult [ContentText (decodeUtf8 out)]
        Right (ExitFailure _, out, _) | reportsBuildOutcome tool ->
            toolResult [ContentText (decodeUtf8 out)]
        Right (ExitFailure _, out, err) ->
            toolError $ "tricorder failed: " <> decodeUtf8 (if BSL.null err then out else err)
```

#### The Race & Protocol Failures:
1. **Socket Connection Failure Masking:** When `tricorder status` fails because the background daemon socket does not exist or connection was refused, `tricorder` prints an error message to stderr or stdout and exits with code 1. Because `reportsBuildOutcome (Status _)` returns `True`, `handleTool` treats the failure as a **successful build result** containing build diagnostics! The agent receives raw connection failure text inside a success payload, causing hallucinated diagnostic handling.
2. **Narrow Project Root Discovery:**
   ```haskell
   getProjectRoot = do
       claudeDir <- lookupEnv "CLAUDE_PROJECT_DIR"
       copilotDir <- lookupEnv "COPILOT_CWD"
       workingDir <- getWorkingDirectory
       let dir = fromMaybe workingDir $ claudeDir <|> copilotDir
   ```
   `getProjectRoot` only checks Claude and Copilot environment variables. In Hermes, Cursor, or containerized environments (like Fedora Toolbox / Distrobox `ws-nadia` where process working directory might differ from workspace roots), `getProjectRoot` fails to locate the project root containing `.cabal` / `cabal.project`, causing tool errors.

---

## 3. Remediation & Architecture Plan

When implementing the synchronization overhaul:

### Step 1: Replace Stale Status Polling with Monotonic Generation Tickets
Instead of querying boolean `wait: Bool`, introduce a monotonic `BuildGeneration` or `BuildId`:
```haskell
data WaitMode
    = ShowCurrent
    | WaitForNextGeneration
    | WaitForGeneration !BuildId
```
When an agent or client requests a build after an edit, it passes `WaitForNextGeneration`. The daemon tracks `currentGeneration :: TVar Word64`. If `currentGeneration` has not advanced past the baseline ticket, `respondWhenDone` is guaranteed to block until the *new* build finishes, entirely eliminating stale cache returns.

### Step 2: Synchronous Registration Barrier for `Sub.listenUntil_`
In `respondWhenDone`, replace the un-synchronized fork with an explicit MVar or STM registration handshake:
```haskell
registered <- newEmptyMVar
progressP <- Conc.fork do
    Sub.listenUntilWithBarrier_ registered \case
        failed@(Build.Failed _) -> Just failed
        finished@(Build.Finished _ _) -> Just finished
        _ -> Nothing
takeMVar registered -- Barrier: ensures subscriber is attached before checking state
```

### Step 3: Atomic Lock for `Waiters`
Make `Waiters` take-and-execute atomic using an MVar or transactional lock:
```haskell
Without m -> localSeqUnlift env \unlift -> do
    runWithoutLock waiters (unlift m)
```

### Step 4: Robust MCP Result Classification & Environment Fallback
In `Tricorder.MCP.Tools`:
1. Parse the stdout of `tricorder status --json` with `Aeson.decode`. Only treat as `toolResult` if valid JSON matching `BuildState` is decoded.
2. If decoding fails and exit code is non-zero, treat as `toolError` (socket missing / daemon crashed).
3. Expand `getProjectRoot` to inspect generic agent working directory variables:
   ```haskell
   hermesDir <- lookupEnv "TERMINAL_CWD"
   workspaceDir <- lookupEnv "WORKSPACE_ROOT"
   ```

---

*Authored for local record and future synchronization implementation.*
