{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}


module Acid.Core.Backend.Abstract where
import RIO
import qualified  RIO.Vector.Boxed as VB

import GHC.TypeLits


import Acid.Core.Segment
import Acid.Core.Event
import Acid.Core.Inner.Abstract

class ( Monad (m ss nn)
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
  loadEvents :: (MonadIO z) => (AWBSerialiseT m ss nn -> Either Text (WrappedEvent ss nn)) ->  AWBState m ss nn -> z (Either Text (VB.Vector (WrappedEvent ss nn)))
  loadEvents _ _ = pure . pure $ VB.empty
  handleUpdateEvent :: (IsValidEvent ss nn n, MonadIO z, AcidWorldUpdate u ss) => (StorableEvent ss nn n -> AWBSerialiseT m ss nn) ->  (AWBState m ss nn) -> (AWUState u ss) -> Event n -> z (EventResult n)