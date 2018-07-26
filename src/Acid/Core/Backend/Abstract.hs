{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}


module Acid.Core.Backend.Abstract where
import RIO
import qualified RIO.Text as T
import Data.Typeable
import Data.Proxy(Proxy(..))
import Acid.Core.Segment
import Acid.Core.State
import Acid.Core.Utils
import Conduit
import Acid.Core.Serialise.Abstract
import Generics.SOP.NP
import Generics.SOP

class AcidWorldBackend (b :: k) where
  data AWBState b
  data AWBConfig b
  type AWBSerialiseT b :: *
  type AWBSerialiseConduitT b :: *
  backendName :: Proxy b -> Text
  default backendName :: (Typeable b) => Proxy b -> Text
  backendName p = T.pack $ (showsTypeRep . typeRep $ p) ""
  backendConfigInfo :: AWBConfig b -> Text
  default backendConfigInfo :: (Show (AWBConfig b)) => AWBConfig b -> Text
  backendConfigInfo = showT
  initialiseBackend :: (MonadIO m, AcidSerialiseEvent t) => AWBConfig b -> AcidSerialiseEventOptions t -> m (Either AWException (AWBState b))
  closeBackend :: (MonadIO m) => AWBState b -> m ()
  closeBackend _ = pure ()


  createCheckpointBackend :: (AcidSerialiseEvent t, AcidSerialiseConduitT t ~ AWBSerialiseConduitT b, MonadUnliftIO m, MonadThrow m, PrimMonad m, ValidAcidWorldState u ss, ValidSegmentsSerialise t ss ) =>  AWBState b -> AWState u ss -> AcidSerialiseEventOptions t -> m ()
  createCheckpointBackend _ _ _ = pure ()

  -- should return the most recent checkpoint state, if any
  getInitialState :: (AcidSerialiseEvent t, MonadUnliftIO m, PrimMonad m, MonadThrow m, AcidSerialiseConduitT t ~ AWBSerialiseConduitT b, ValidSegmentsSerialise t ss ) => SegmentsState ss -> AWBState b -> AcidSerialiseEventOptions t -> m (Either AWException (SegmentsState ss))
  getInitialState defState _ _ = pure . pure $ defState
  -- return events since the last checkpoint, if any
  loadEvents :: (MonadUnliftIO m, AcidSerialiseEvent t) => (ConduitT (AWBSerialiseConduitT b) (Either Text (WrappedEvent ss nn)) (ResourceT IO) ()) ->  AWBState b -> AcidSerialiseEventOptions t ->  m (Either AWException (LoadEventsConduit m ss nn))
  loadEvents _ _ _ = pure . pure $ LoadEventsConduit $ \rest -> liftIO $ runConduitRes $ yieldMany [] .| rest
  handleUpdateEventC :: (
        AcidSerialiseEvent t
      , All (IsValidEvent ss nn) (firstN ': ns)
      , All (ValidEventName ss) (firstN ': ns)
      , MonadUnliftIO m
      , ValidAcidWorldState u ss) =>
             (NP (StorableEvent ss nn) (firstN ': ns) -> AWBSerialiseT b)
          -> (AWBState b) -> (AWState u ss)
          -> AcidSerialiseEventOptions t
          -> EventC (firstN ': ns)
          -> (EventResult firstN -> m ioRes)
          -> m (Either AWException  (EventResult firstN, ioRes))

