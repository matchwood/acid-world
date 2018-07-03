{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Acid.Core where
import RIO

import Generics.SOP

import Acid.Core.Segment
import Acid.Core.Event
import Acid.Core.Serialise
import Acid.Core.Backend
import Acid.Core.Inner

data AcidWorld  ss nn t where
  AcidWorld :: (
                 AcidWorldBackend bMonad ss nn
               , AcidWorldUpdate uMonad ss
               , AcidSerialiseEvent t
               , AcidSerialiseT t ~ AWBSerialiseT bMonad ss nn
               , AcidSerialiseConstraintAll t ss nn
               ) => {
    acidWorldBackendConfig :: AWBConfig bMonad ss nn,
    acidWorldUpdateMonadConfig :: AWUConfig uMonad ss,
    acidWorldBackendState :: AWBState bMonad ss nn,
    acidWorldUpdateState :: AWUState uMonad ss,
    acidWorldSerialiserOptions :: AcidSerialiseEventOptions t
    } -> AcidWorld ss nn t



openAcidWorld :: forall m ss nn bMonad uMonad t.
               ( MonadIO m
               , MonadThrow m
               , AcidWorldBackend bMonad ss nn
               , AcidWorldUpdate uMonad ss
               , AcidSerialiseEvent t
               , AcidSerialiseT t ~ AWBSerialiseT bMonad ss nn
               , AcidSerialiseConstraintAll t ss nn
               ) => Maybe (SegmentsState ss) -> AWBConfig bMonad ss nn -> AWUConfig uMonad ss -> AcidSerialiseEventOptions t ->  m (Either Text (AcidWorld ss nn t))
openAcidWorld mDefSt acidWorldBackendConfig acidWorldUpdateMonadConfig acidWorldSerialiserOptions = do
  let defState = fromMaybe (defaultSegmentsState (Proxy :: Proxy ss)) mDefSt
  (eAcidWorldBackendState) <- initialiseBackend acidWorldBackendConfig defState
  case eAcidWorldBackendState of
    Left err -> pure . Left $ err
    Right acidWorldBackendState -> do
      let parsers = makeDeserialiseParsers acidWorldSerialiserOptions (Proxy :: Proxy ss) (Proxy :: Proxy nn)
      let handles = BackendHandles {
              bhLoadEvents = loadEvents (deserialiseEventStream acidWorldSerialiserOptions parsers) acidWorldBackendState,
              bhGetLastCheckpointState = getLastCheckpointState acidWorldBackendState
            }

      (eAcidWorldUpdateState) <- initialiseUpdate acidWorldUpdateMonadConfig handles defState
      pure $ do
        acidWorldUpdateState <- eAcidWorldUpdateState
        pure AcidWorld{..}

closeAcidWorld :: (MonadIO m) => AcidWorld ss nn t -> m ()
closeAcidWorld (AcidWorld {..}) = do
  closeBackend acidWorldBackendState
  closeUpdate acidWorldUpdateState

reopenAcidWorld :: (MonadIO m, MonadThrow m) => AcidWorld ss nn t -> m (Either Text (AcidWorld ss nn t))
reopenAcidWorld (AcidWorld {..}) = do
  openAcidWorld Nothing (acidWorldBackendConfig) (acidWorldUpdateMonadConfig) acidWorldSerialiserOptions

update :: (IsValidEvent ss nn n, MonadIO m, AcidSerialiseConstraint t ss n) => AcidWorld ss nn t -> Event n -> m (EventResult n)
update (AcidWorld {..}) = handleUpdateEvent (serialiseEvent acidWorldSerialiserOptions)  acidWorldBackendState acidWorldUpdateState



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







