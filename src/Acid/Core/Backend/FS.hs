
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.FS where
import RIO
import qualified RIO.Text as T
import System.IO (openBinaryFile, IOMode(..))
import qualified RIO.Directory as Dir
import qualified  RIO.ByteString as BS
import qualified Control.Concurrent.STM.TMVar as  TMVar
import qualified Control.Concurrent.STM  as STM

import Data.Proxy(Proxy(..))


import Generics.SOP
import Generics.SOP.NP
import qualified  Data.Vinyl.Derived as V
import qualified  Data.Vinyl.TypeLevel as V

import Acid.Core.State
import Acid.Core.Utils
import Acid.Core.Backend.Abstract
import Acid.Core.Serialise.Abstract
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
  createCheckpointBackend s awu t = do
    sToWrite <- modifyTMVar (aWBStateFSEventsHandle s) $ \hdl -> do
      let eventPath = makeEventPath (aWBConfigFSStateDir . aWBStateFSConfig $ s)
          nextEventFile = eventPath <> "1"
      liftIO $ hClose hdl
      Dir.renameFile eventPath nextEventFile
      hdl' <- liftIO $ openBinaryFile eventPath ReadWriteMode
      st <- runQuery awu askStateNp
      pure (hdl', st)
    writeCheckpoint s t sToWrite


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


writeCheckpoint :: forall t sFields m. (AcidSerialiseT t ~ BS.ByteString, MonadIO m,  All (AcidSerialiseSegmentFieldConstraint t) sFields)  => AWBState AcidWorldBackendFS -> AcidSerialiseEventOptions t -> NP V.ElField sFields -> m ()
writeCheckpoint s t np = do
  let cpFolder = (aWBConfigFSStateDir . aWBStateFSConfig $ s) <> "/checkpoint"
  Dir.createDirectoryIfMissing True cpFolder
  ctraverse__NP (Proxy :: Proxy (AcidSerialiseSegmentFieldConstraint t)) (writeSegment t cpFolder) np


writeSegment :: forall t m fs. (AcidSerialiseT t ~ BS.ByteString, MonadIO m, AcidSerialiseSegmentFieldConstraint t fs) => AcidSerialiseEventOptions t -> FilePath -> V.ElField fs -> m ()
writeSegment t cpFolder ((V.Field seg)) = do
  BS.writeFile (cpFolder <> "/" <> T.unpack (toUniqueText (Proxy :: Proxy (V.Fst fs)))) (serialiseSegment t seg)

makeEventPath :: FilePath -> FilePath
makeEventPath fp = fp <> "/" <> "events"