
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.FS where
import RIO
import System.IO (openFile, IOMode(..))
import qualified RIO.Directory as Dir
import qualified  RIO.ByteString.Lazy as BL
import qualified  RIO.ByteString as BS
import qualified Control.Concurrent.STM.TMVar as  TMVar
import qualified Control.Concurrent.STM  as STM
import qualified System.FileLock as FileLock

import Acid.Core.Utils
import Acid.Core.State
import Acid.Core.Backend.Abstract
import Conduit

data AcidWorldBackendFS


-- @todo add exception handling?
withTMVar :: TMVar a -> (a -> IO b) -> IO b
withTMVar m io = do
  a <- atomically $ TMVar.takeTMVar m
  b <- io a
  atomically $ TMVar.putTMVar m a
  pure b


instance AcidWorldBackend AcidWorldBackendFS where
  data AWBState AcidWorldBackendFS = AWBStateFS {
    aWBStateFSConfig :: AWBConfig AcidWorldBackendFS,
    aWBStateFSEventsHandle :: TMVar.TMVar Handle
  }
  data AWBConfig AcidWorldBackendFS = AWBConfigFS {
    aWBConfigFSStateDir :: FilePath
  }
  type AWBSerialiseT AcidWorldBackendFS  = BS.ByteString
  initialiseBackend _ c _  = do
    stateP <- Dir.makeAbsolute (aWBConfigFSStateDir c)
    Dir.createDirectoryIfMissing True stateP
    let eventPath = makeEventPath stateP
    traceM $ "Opening file"
    hdl <- liftIO $ openFile eventPath ReadWriteMode
    traceM $ "Opened file!"
    hdlV <- liftIO $ STM.atomically $ newTMVar hdl
    pure . pure $ AWBStateFS c{aWBConfigFSStateDir = stateP} hdlV
  loadEvents deserialiseConduit s =
    liftIO $ withTMVar (aWBStateFSEventsHandle s) $ \hdl -> do
      pure $
           sourceHandle hdl .|
           deserialiseConduit


  -- this should be bracketed and so forth @todo
  handleUpdateEvent serializer awb awu (e :: Event n) = do
    let eventPath = makeEventPath (aWBConfigFSStateDir . aWBStateFSConfig $ awb)
    stE <- mkStorableEvent e
    --BL.appendFile eventPath (BL.fromStrict $ serializer stE)
    runUpdate awu e


makeEventPath :: FilePath -> FilePath
makeEventPath fp = fp <> "/" <> "events"