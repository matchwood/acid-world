{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}


module Acid.Core.Backend.Abstract where
import RIO

import Data.Proxy(Proxy(..))

import Acid.Core.Segment
import Acid.Core.State
import Conduit

class AcidWorldBackend b where
  data AWBState b
  data AWBConfig b
  type AWBSerialiseT b :: *
  backendName :: Proxy b -> Text
  initialiseBackend :: (MonadIO m) => Proxy ss -> AWBConfig b -> (SegmentsState ss) -> m (Either Text (AWBState b))
  closeBackend :: (MonadIO m) => AWBState b -> m ()
  closeBackend _ = pure ()

  -- should return the most recent checkpoint state, if any
  getLastCheckpointState :: (MonadIO m) => Proxy ss -> AWBState b -> m (Maybe (SegmentsState ss))
  getLastCheckpointState _ _ = pure Nothing
  -- return events since the last checkpoint, if any
  loadEvents :: (MonadIO m) => (ConduitT (AWBSerialiseT b) (Either Text (WrappedEvent ss nn)) (ResourceT IO) ()) ->  AWBState b -> m (ConduitT i (WrappedEvent ss nn) (ResourceT IO) ())
  loadEvents _ _ = pure $ yieldMany []
  handleUpdateEvent :: (IsValidEvent ss nn n, MonadIO m, AcidWorldState u ss) => (StorableEvent ss nn n -> AWBSerialiseT b) ->  (AWBState b) -> (AWState u ss) -> Event n -> m (EventResult n)

