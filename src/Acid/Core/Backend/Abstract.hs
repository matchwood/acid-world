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
  initialiseBackend :: (MonadIO z) => Proxy ss -> AWBConfig b -> (SegmentsState ss) -> z (Either Text (AWBState b))
  closeBackend :: (MonadIO z) => AWBState b -> z ()
  closeBackend _ = pure ()

  -- should return the most recent checkpoint state, if any
  getLastCheckpointState :: (MonadIO z) => Proxy ss -> AWBState b -> z (Maybe (SegmentsState ss))
  getLastCheckpointState _ _ = pure Nothing
  -- return events since the last checkpoint, if any
  loadEvents :: (MonadIO z) => (ConduitT (AWBSerialiseT b) (Either Text (WrappedEvent ss nn)) (ResourceT IO) ()) ->  AWBState b -> z (ConduitT i (WrappedEvent ss nn) (ResourceT IO) ())
  loadEvents _ _ = pure $ yieldMany []
  handleUpdateEvent :: (IsValidEvent ss nn n, MonadIO z, AcidWorldState u ss) => (StorableEvent ss nn n -> AWBSerialiseT b) ->  (AWBState b) -> (AWState u ss) -> Event n -> z (EventResult n)


{-class ( Monad (m ss nn)
      , MonadIO (m ss nn)
      , MonadThrow (m ss nn)
      , ValidSegmentNames ss
      , ValidEventNames ss nn
      ) =>
  AcidWorldBackend (m :: [Symbol] -> [Symbol] -> * -> *) (ss :: [Symbol]) (nn :: [Symbol]) where
  data AWBState m ss nn
  data AWBConfig m ss nn
  type AWBSerialiseT m ss nn :: *
  initialiseBackend :: (MonadIO z) => AWBConfig m ss nn -> (SegmentsState ss) -> z (Either Text (AWBState m ss nn))
  closeBackend :: (MonadIO z) => AWBState m ss nn -> z ()
  closeBackend _ = pure ()

  -- should return the most recent checkpoint state, if any
  getLastCheckpointState :: (MonadIO z) => AWBState m ss nn -> z (Maybe (SegmentsState ss))
  getLastCheckpointState _ = pure Nothing
  -- return events since the last checkpoint, if any
  loadEvents :: (MonadIO z) => (ConduitT (AWBSerialiseT m ss nn) (Either Text (WrappedEvent ss nn)) (ResourceT IO) ()) ->  AWBState m ss nn -> z (ConduitT i (WrappedEvent ss nn) (ResourceT IO) ())
  loadEvents _ _ = pure $ yieldMany []
  handleUpdateEvent :: (IsValidEvent ss nn n, MonadIO z, AcidWorldState u ss) => (StorableEvent ss nn n -> AWBSerialiseT m ss nn) ->  (AWBState m ss nn) -> (AWState u ss) -> Event n -> z (EventResult n)-}