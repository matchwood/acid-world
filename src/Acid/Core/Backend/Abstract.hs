{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}


module Acid.Core.Backend.Abstract where
import RIO
import qualified RIO.Text as T
import Data.Typeable
import Data.Proxy(Proxy(..))
import Acid.Core.Segment
import Acid.Core.State
import Conduit
import Acid.Core.Serialise.Abstract

class AcidWorldBackend (b :: k) where
  data AWBState b
  data AWBConfig b
  type AWBSerialiseT b :: *
  type AWBSerialiseConduitT b :: *
  backendName :: Proxy b -> Text
  default backendName :: (Typeable b) => Proxy b -> Text
  backendName p = T.pack $ (showsTypeRep . typeRep $ p) ""
  initialiseBackend :: (MonadIO m) => Proxy ss -> AWBConfig b -> (SegmentsState ss) -> m (Either Text (AWBState b))
  closeBackend :: (MonadIO m) => AWBState b -> m ()
  closeBackend _ = pure ()


  createCheckpointBackend :: (AcidSerialiseConduitT t ~ AWBSerialiseConduitT b, MonadUnliftIO m, ValidAcidWorldState u ss, ValidSegmentsSerialise t ss ) =>  AWBState b -> AWState u ss -> AcidSerialiseEventOptions t -> m ()
  createCheckpointBackend _ _ _ = pure ()

  -- should return the most recent checkpoint state, if any
  getLastCheckpointState :: (MonadUnliftIO m, AcidSerialiseConduitT t ~ AWBSerialiseConduitT b, ValidSegmentsSerialise t ss ) => Proxy ss -> AWBState b -> AcidSerialiseEventOptions t -> m (Either Text (Maybe (SegmentsState ss)))
  getLastCheckpointState _ _ _ = pure . pure $ Nothing
  -- return events since the last checkpoint, if any
  loadEvents :: (MonadIO m) => (ConduitT (AWBSerialiseConduitT b) (Either Text (WrappedEvent ss nn)) (ResourceT IO) ()) ->  AWBState b -> m (ConduitT i (Either Text (WrappedEvent ss nn)) (ResourceT IO) ())
  loadEvents _ _ = pure $ yieldMany []
  handleUpdateEvent :: (IsValidEvent ss nn n, MonadIO m, ValidAcidWorldState u ss) => (StorableEvent ss nn n -> AWBSerialiseT b) ->  (AWBState b) -> (AWState u ss) -> Event n -> m (EventResult n)

