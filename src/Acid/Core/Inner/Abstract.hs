module Acid.Core.Inner.Abstract where


import RIO
import qualified  RIO.Vector.Boxed as VB


import Acid.Core.Segment
import Acid.Core.Event

data BackendHandles m ss nn = BackendHandles {
    bhLoadEvents :: MonadIO m => m (Either Text (VB.Vector (WrappedEvent ss nn))),
    bhGetLastCheckpointState :: MonadIO m => m (Maybe (SegmentsState ss))
  }


class (AcidWorldUpdateInner m ss) => AcidWorldUpdate m ss where
  data AWUState m ss
  data AWUConfig m ss
  initialiseUpdate :: (MonadIO z, MonadThrow z) => AWUConfig m ss -> (BackendHandles z ss nn) -> (SegmentsState ss) -> z (Either Text (AWUState m ss))
  closeUpdate :: (MonadIO z) => AWUState m ss -> z ()
  closeUpdate _ = pure ()
  runUpdateEvent :: ( ValidEventName ss n
                    , MonadIO z) =>
    AWUState m ss -> Event n -> z (EventResult n)