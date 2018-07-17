
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.FS where
import RIO
import System.IO (openBinaryFile, IOMode(..))
import qualified RIO.Directory as Dir
import qualified  RIO.ByteString as BS
import qualified Control.Concurrent.STM.TMVar as  TMVar
import qualified Control.Concurrent.STM  as STM

import Acid.Core.State
import Acid.Core.Backend.Abstract
import Conduit

data AcidWorldBackendFS


-- @todo add exception handling?
withTMVar :: (MonadIO m) => TMVar a -> (a -> m b) -> m b
withTMVar m io = do
  a <- liftIO $ atomically $ TMVar.takeTMVar m
  b <- io a
  liftIO $ atomically $ TMVar.putTMVar m a
  pure b

modifyTMVar :: (MonadIO m) => TMVar a -> (a -> m (a, b)) -> m b
modifyTMVar m io = do
  a <- liftIO $ atomically $ TMVar.takeTMVar m
  (a', b) <- io a
  liftIO $ atomically $ TMVar.putTMVar m a'
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
    hdl <- liftIO $ openBinaryFile eventPath ReadWriteMode
    hdlV <- liftIO $ STM.atomically $ newTMVar hdl
    pure . pure $ AWBStateFS c{aWBConfigFSStateDir = stateP} hdlV

  createCheckpoint _ s awu = do
    sToWrite <- modifyTMVar (aWBStateFSEventsHandle s) $ \hdl -> do
      let eventPath = makeEventPath (aWBConfigFSStateDir . aWBStateFSConfig $ s)
          nextEventFile = eventPath <> "1"
      liftIO $ hClose hdl
      Dir.renameFile eventPath nextEventFile
      hdl <- liftIO $ openBinaryFile eventPath ReadWriteMode
      st <- runQuery awu askState
      pure (hdl, st)
    undefined


  closeBackend s = modifyTMVar (aWBStateFSEventsHandle s) $ \hdl -> do
    liftIO $ hClose hdl
    pure (error "AcidWorldBackendFS has been closed", ())

  loadEvents deserialiseConduit s = withTMVar (aWBStateFSEventsHandle s) $ \hdl -> do
      pure $
           sourceHandle hdl .|
           deserialiseConduit


  -- this should be bracketed and the runUpdate should return the initial state, so we can rollback if necessary @todo
  handleUpdateEvent serializer s awu (e :: Event n) = withTMVar (aWBStateFSEventsHandle s) $ \hdl -> do
    stE <- mkStorableEvent e
    BS.hPut hdl $ serializer stE
    hFlush hdl
    runUpdate awu e



makeEventPath :: FilePath -> FilePath
makeEventPath fp = fp <> "/" <> "events"