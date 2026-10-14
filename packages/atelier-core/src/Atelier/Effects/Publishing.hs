module Atelier.Effects.Publishing (runPubSub) where

import Atelier.Effects.Chan (Chan)
import Atelier.Effects.Clock (Clock)
import Atelier.Effects.PubSub.Pub (Pub)
import Atelier.Effects.PubSub.Sub (Sub)

import Atelier.Effects.PubSub qualified as PubSub


-- | Runs 'Pub' and 'Sub' effects with an internal channel for a specific event
-- type.
runPubSub
    :: forall event es a
     . (Chan :> es, Clock :> es)
    => Eff (Pub event : Sub event : es) a -> Eff es a
runPubSub = PubSub.run
{-# DEPRECATED runPubSub "Use 'Atelier.Effects.PubSub.run' instead." #-}
