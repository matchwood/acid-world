{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Acid.Core where
import RIO

import Generics.SOP

import Acid.Core.Utils
import Acid.Core.Segment
import Acid.Core.State
import Acid.Core.Serialise
import Acid.Core.Backend

data AcidWorld  ss nn t where
  AcidWorld :: (
                 AcidWorldBackend b
               , ValidAcidWorldState uMonad ss
               , AcidSerialiseEvent t
               , AcidSerialiseT t ~ AWBSerialiseT b
               , AcidSerialiseConduitT t ~ AWBSerialiseConduitT b
               , AcidSerialiseSegmentT t ~ AWBSerialiseSegmentT b
               , AcidDeserialiseSegmentT t ~ AWBDeserialiseSegmentT b
               , AcidSerialiseConstraintAll t ss nn
               , ValidEventNames ss nn
               , ValidSegmentsSerialise t ss
               ) => {
    acidWorldBackendConfig :: AWBConfig b,
    acidWorldStateConfig :: AWConfig uMonad ss,
    acidWorldBackendState :: AWBState b,
    acidWorldState :: AWState uMonad ss,
    acidWorldSerialiserOptions :: AcidSerialiseEventOptions t,
    acidWorldDefaultState :: SegmentsState ss,
    acidWorldInvariants :: Invariants ss
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
               , AcidSerialiseSegmentT t ~ AWBSerialiseSegmentT b
               , AcidDeserialiseSegmentT t ~ AWBDeserialiseSegmentT b
               , AcidSerialiseConstraintAll t ss nn
               , ValidEventNames ss nn
               , ValidSegmentsSerialise t ss
               ) => SegmentsState ss -> Invariants ss -> AWBConfig b -> AWConfig uMonad ss -> AcidSerialiseEventOptions t ->  m (Either AWException (AcidWorld ss nn t))
openAcidWorld acidWorldDefaultState acidWorldInvariants acidWorldBackendConfig acidWorldStateConfig acidWorldSerialiserOptions = do
  eBind (initialiseBackend acidWorldBackendConfig acidWorldSerialiserOptions) $ \acidWorldBackendState -> do

    eBind (makeBackendHandles acidWorldBackendState) $ \handles -> do

      eAcidWorldState <- initialiseState acidWorldStateConfig handles acidWorldInvariants
      case eAcidWorldState of
        Left err -> do
          closeBackend acidWorldBackendState
          pure . Left $ err
        Right acidWorldState ->  pure . pure $ AcidWorld{..}
  where
    makeBackendHandles :: AWBState b -> m (Either AWException (BackendHandles m ss nn))
    makeBackendHandles acidWorldBackendState = (fmap . fmap) (\lEvents -> BackendHandles lEvents (getInitialState acidWorldDefaultState acidWorldBackendState acidWorldSerialiserOptions)) $ (loadEvents (deserialiseEventStream acidWorldSerialiserOptions parsers) acidWorldBackendState acidWorldSerialiserOptions)

    parsers = makeDeserialiseParsers acidWorldSerialiserOptions (Proxy :: Proxy ss) (Proxy :: Proxy nn)

closeAcidWorld :: (MonadIO m) => AcidWorld ss nn t -> m ()
closeAcidWorld (AcidWorld {..}) = do
  closeBackend acidWorldBackendState
  closeState acidWorldState

reopenAcidWorld :: (MonadUnliftIO m, PrimMonad m, MonadThrow m) => AcidWorld ss nn t -> m (Either AWException (AcidWorld ss nn t))
reopenAcidWorld (AcidWorld {..}) = do
  openAcidWorld acidWorldDefaultState acidWorldInvariants acidWorldBackendConfig acidWorldStateConfig acidWorldSerialiserOptions

update :: forall ss nn n m t. (IsValidEvent ss nn n, MonadUnliftIO m, AcidSerialiseConstraint t ss n) => AcidWorld ss nn t -> Event n -> m (Either AWException (EventResult n))
update aw e = updateC aw (EventC e)

updateWithIO :: forall ss nn n m t ioRes. (IsValidEvent ss nn n, MonadUnliftIO m, AcidSerialiseConstraint t ss n) => AcidWorld ss nn t -> Event n -> (EventResult n -> m ioRes) -> m (Either AWException (EventResult n, ioRes))
updateWithIO aw e = updateCWithIO aw (EventC e)

updateC :: forall ss nn firstN ns m t. (All (IsValidEvent ss nn) (firstN ': ns), All (ValidEventName ss) (firstN ': ns), MonadUnliftIO m, AcidSerialiseConstraintAll t ss  (firstN ': ns)) => AcidWorld ss nn t -> EventC (firstN ': ns) ->  m (Either AWException (EventResult firstN))
updateC aw ec = (fmap  . fmap) fst $ updateCWithIO aw ec (const $ pure ())

updateCWithIO :: forall ss nn firstN ns m t ioRes. (All (IsValidEvent ss nn) (firstN ': ns), All (ValidEventName ss) (firstN ': ns), MonadUnliftIO m, AcidSerialiseConstraintAll t ss  (firstN ': ns)) => AcidWorld ss nn t -> EventC (firstN ': ns) -> (EventResult firstN -> m ioRes) -> m (Either AWException (EventResult firstN, ioRes))
updateCWithIO (AcidWorld {..}) ec = handleUpdateEventC ((serialiseEventNP acidWorldSerialiserOptions) :: NP (StorableEvent ss nn) (firstN ': ns) -> AcidSerialiseT t)  acidWorldBackendState acidWorldState acidWorldSerialiserOptions ec

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







