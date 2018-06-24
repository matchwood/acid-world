module Acid.Core.Inner.Abstract where


import RIO


import Acid.Core.Segment
import Acid.Core.Event
import Conduit

data BackendHandles m ss nn = BackendHandles {
    bhLoadEvents :: forall i. MonadIO m => m (ConduitT i (WrappedEvent ss nn) (ResourceT IO) ()),
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