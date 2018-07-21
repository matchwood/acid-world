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
               , ValidAcidWorldState uMonad ss
               , AcidSerialiseEvent t
               , AcidSerialiseT t ~ AWBSerialiseT b
               , AcidSerialiseConduitT t ~ AWBSerialiseConduitT b
               , AcidSerialiseConstraintAll t ss nn
               , ValidEventNames ss nn
               , ValidSegmentsSerialise t ss
               ) => {
    acidWorldBackendConfig :: AWBConfig b,
    acidWorldStateConfig :: AWConfig uMonad ss,
    acidWorldBackendState :: AWBState b,
    acidWorldState :: AWState uMonad ss,
    acidWorldSerialiserOptions :: AcidSerialiseEventOptions t
    } -> AcidWorld ss nn t



openAcidWorld :: forall m ss nn b uMonad t.
               ( MonadUnliftIO m
               , PrimMonad m
               , MonadThrow m
               , AcidWorldBackend b
               , ValidAcidWorldState uMonad ss
               , AcidSerialiseEvent t
               , AcidSerialiseT t ~ AWBSerialiseT b
               , AcidSerialiseConduitT t ~ AWBSerialiseConduitT b
               , AcidSerialiseConstraintAll t ss nn
               , ValidEventNames ss nn
               , ValidSegmentsSerialise t ss
               ) => Maybe (SegmentsState ss) -> AWBConfig b -> AWConfig uMonad ss -> AcidSerialiseEventOptions t ->  m (Either Text (AcidWorld ss nn t))
openAcidWorld mDefSt acidWorldBackendConfig acidWorldStateConfig acidWorldSerialiserOptions = do
  let defState = fromMaybe (defaultSegmentsState (Proxy :: Proxy ss)) mDefSt
  (eAcidWorldBackendState) <- initialiseBackend (Proxy :: Proxy ss) acidWorldBackendConfig defState
  case eAcidWorldBackendState of
    Left err -> pure . Left $ err
    Right acidWorldBackendState -> do
      let parsers = makeDeserialiseParsers acidWorldSerialiserOptions (Proxy :: Proxy ss) (Proxy :: Proxy nn)
      let handles = BackendHandles {
              bhLoadEvents = (loadEvents (deserialiseEventStream acidWorldSerialiserOptions parsers) acidWorldBackendState) :: m (ConduitT i (Either Text (WrappedEvent ss nn)) (ResourceT IO) ()),
              bhGetLastCheckpointState = getLastCheckpointState (Proxy :: Proxy ss) acidWorldBackendState acidWorldSerialiserOptions
            }

      (eAcidWorldState) <- initialiseState acidWorldStateConfig handles defState
      pure $ do
        acidWorldState <- eAcidWorldState
        pure AcidWorld{..}

closeAcidWorld :: (MonadIO m) => AcidWorld ss nn t -> m ()
closeAcidWorld (AcidWorld {..}) = do
  closeBackend acidWorldBackendState
  closeState acidWorldState

reopenAcidWorld :: (MonadUnliftIO m, PrimMonad m, MonadThrow m) => AcidWorld ss nn t -> m (Either Text (AcidWorld ss nn t))
reopenAcidWorld (AcidWorld {..}) = do
  openAcidWorld Nothing (acidWorldBackendConfig) (acidWorldStateConfig) acidWorldSerialiserOptions

update :: forall ss nn n m t. (IsValidEvent ss nn n, MonadIO m, AcidSerialiseConstraint t ss n) => AcidWorld ss nn t -> Event n -> m (EventResult n)
update (AcidWorld {..}) = handleUpdateEvent ((serialiseEvent acidWorldSerialiserOptions) :: StorableEvent ss nn n -> AcidSerialiseT t)  acidWorldBackendState acidWorldState

updateC :: forall ss nn firstN ns m t. (All (IsValidEvent ss nn) (firstN ': ns), All (ValidEventName ss) (firstN ': ns), MonadIO m, AcidSerialiseConstraintAll t ss (firstN ': ns)) => AcidWorld ss nn t -> EventC (firstN ': ns) -> m (EventResult firstN)
updateC (AcidWorld {..}) = handleUpdateEventC ((serialiseEventNP acidWorldSerialiserOptions) :: NP (StorableEvent ss nn) (firstN ': ns) -> AcidSerialiseT t)  acidWorldBackendState acidWorldState


query ::forall ss nn t m a. MonadIO m => AcidWorld ss nn t -> (forall i. ValidAcidWorldState i ss => AWQuery i ss a) -> m a
query (AcidWorld {..}) q = runQuery acidWorldState q


createCheckpoint ::forall ss nn t m. (MonadUnliftIO m, MonadThrow m, PrimMonad m) => AcidWorld ss nn t -> m ()
createCheckpoint (AcidWorld {..}) = createCheckpointBackend acidWorldBackendState acidWorldState acidWorldSerialiserOptions
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







