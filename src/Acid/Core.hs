{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Acid.Core where
import RIO

import Generics.SOP

import Acid.Core.Segment
import Acid.Core.State
import Acid.Core.Serialise
import Acid.Core.Backend
import Conduit

data AcidWorld  ss nn t where
  AcidWorld :: (
                 AcidWorldBackend b
               , AcidWorldState uMonad ss
               , AcidSerialiseEvent t
               , AcidSerialiseT t ~ AWBSerialiseT b
               , AcidSerialiseConstraintAll t ss nn
               , ValidSegmentNames ss
               , ValidEventNames ss nn
               ) => {
    acidWorldBackendConfig :: AWBConfig b,
    acidWorldStateConfig :: AWConfig uMonad ss,
    acidWorldBackendState :: AWBState b,
    acidWorldState :: AWState uMonad ss,
    acidWorldSerialiserOptions :: AcidSerialiseEventOptions t
    } -> AcidWorld ss nn t



openAcidWorld :: forall m ss nn b uMonad t.
               ( MonadIO m
               , MonadThrow m
               , AcidWorldBackend b
               , AcidWorldState uMonad ss
               , AcidSerialiseEvent t
               , AcidSerialiseT t ~ AWBSerialiseT b
               , AcidSerialiseConstraintAll t ss nn
               , ValidSegmentNames ss
               , ValidEventNames ss nn
               ) => Maybe (SegmentsState ss) -> AWBConfig b -> AWConfig uMonad ss -> AcidSerialiseEventOptions t ->  m (Either Text (AcidWorld ss nn t))
openAcidWorld mDefSt acidWorldBackendConfig acidWorldStateConfig acidWorldSerialiserOptions = do
  let defState = fromMaybe (defaultSegmentsState (Proxy :: Proxy ss)) mDefSt
  (eAcidWorldBackendState) <- initialiseBackend (Proxy :: Proxy ss) acidWorldBackendConfig defState
  case eAcidWorldBackendState of
    Left err -> pure . Left $ err
    Right acidWorldBackendState -> do
      let parsers = makeDeserialiseParsers acidWorldSerialiserOptions (Proxy :: Proxy ss) (Proxy :: Proxy nn)
      let handles = BackendHandles {
              bhLoadEvents = (loadEvents (deserialiseEventStream acidWorldSerialiserOptions parsers) acidWorldBackendState) :: m (ConduitT i (WrappedEvent ss nn) (ResourceT IO) ()),
              bhGetLastCheckpointState = getLastCheckpointState (Proxy :: Proxy ss) acidWorldBackendState
            }

      (eAcidWorldState) <- initialiseState acidWorldStateConfig handles defState
      pure $ do
        acidWorldState <- eAcidWorldState
        pure AcidWorld{..}

closeAcidWorld :: (MonadIO m) => AcidWorld ss nn t -> m ()
closeAcidWorld (AcidWorld {..}) = do
  closeBackend acidWorldBackendState
  closeState acidWorldState

reopenAcidWorld :: (MonadIO m, MonadThrow m) => AcidWorld ss nn t -> m (Either Text (AcidWorld ss nn t))
reopenAcidWorld (AcidWorld {..}) = do
  openAcidWorld Nothing (acidWorldBackendConfig) (acidWorldStateConfig) acidWorldSerialiserOptions

update :: forall ss nn n m t. (IsValidEvent ss nn n, MonadIO m, AcidSerialiseConstraint t ss n) => AcidWorld ss nn t -> Event n -> m (EventResult n)
update (AcidWorld {..}) = handleUpdateEvent ((serialiseEvent acidWorldSerialiserOptions) :: StorableEvent ss nn n -> AcidSerialiseT t)  acidWorldBackendState acidWorldState


query ::forall ss nn t m a. MonadIO m => AcidWorld ss nn t -> (forall i. AcidWorldState i ss => AWQuery i ss a) -> m a
query (AcidWorld {..}) q = runQuery acidWorldState q



{-
AcidWorldBackend
-}





{-
AcidWorld Inner monad
-}



















{-
 attempt at composing events
instance (Eventable a, Eventable b) => Eventable (a, b) where
  type EventArgs (a, b) = '[(V.HList (EventArgs a), V.HList (EventArgs b))]
  type EventResult (a, b) = (EventResult a, EventResult b)
  type EventS (a, b) = (EventS a) V.++ (EventS b)
  --type EventC (a, b) = ()
  runEvent _ args = do
    let (argsA, argsB) = rHead args
    res1 <- runEvent (Proxy :: Proxy a) argsA
    res2 <- runEvent (Proxy :: Proxy b) argsB
    return (res1, res2)
  rHead :: V.Rec V.Identity (a ': xs) -> a
  rHead (ir V.:& _) = V.getIdentity ir

-}







