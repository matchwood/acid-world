
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
import Acid.Core.Segment
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


  getLastCheckpointState ps s t = do
    let cpFolder = (aWBConfigFSStateDir . aWBStateFSConfig $ s) <> "/checkpoint"
    doesExist <- Dir.doesDirectoryExist cpFolder
    if doesExist
      then do
        readLastCheckpointState cpFolder ps s t
      else pure . pure $ Nothing

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


writeCheckpoint :: forall t sFields m. (AcidSerialiseT t ~ BS.ByteString, MonadUnliftIO m, All (AcidSerialiseSegmentFieldConstraint t) sFields)  => AWBState AcidWorldBackendFS -> AcidSerialiseEventOptions t -> NP V.ElField sFields -> m ()
writeCheckpoint s t np = do
  let cpFolder = (aWBConfigFSStateDir . aWBStateFSConfig $ s) <> "/checkpoint"
  Dir.createDirectoryIfMissing True cpFolder
  let acts =  cfoldMap_NP (Proxy :: Proxy (AcidSerialiseSegmentFieldConstraint t)) ((:[]) . writeSegment t cpFolder) np
  mapConcurrently_ id acts

writeSegment :: forall t m fs. (AcidSerialiseT t ~ BS.ByteString, MonadIO m, AcidSerialiseSegmentFieldConstraint t fs) => AcidSerialiseEventOptions t -> FilePath -> V.ElField fs -> m ()
writeSegment t cpFolder ((V.Field seg)) = do
  BS.writeFile (cpFolder <> "/" <> T.unpack (toUniqueText (Proxy :: Proxy (V.Fst fs)))) (serialiseSegment t seg)

readLastCheckpointState :: forall ss m t. (ValidSegments ss,  MonadIO m, AcidSerialiseT t ~ BS.ByteString, All (AcidSerialiseSegmentFieldConstraint t) (ToSegmentFields ss)) => FilePath -> Proxy ss -> AWBState AcidWorldBackendFS -> AcidSerialiseEventOptions t -> m (Either Text (Maybe (SegmentsState ss)))
readLastCheckpointState sPath _ _ t = (fmap . fmap) (Just . npToSegmentsState) segsNpE

  where
    segsNpE :: m (Either Text (NP V.ElField (ToSegmentFields ss)))
    segsNpE = unComp $ sequence'_NP segsNp
    segsNp :: NP ((m :.: Either Text) :.: V.ElField) (ToSegmentFields ss)
    segsNp = trans_NP (Proxy :: Proxy (SegmentFieldToSegmentField ss)) readSegmentFromProxy proxyNp
    readSegmentFromProxy :: forall sField. (SegmentFetching ss sField) => Proxy sField -> ((m :.: Either Text) :.: V.ElField) sField
    readSegmentFromProxy _ =  Comp $  fmap V.Field $  Comp $ readSegment (Proxy :: Proxy (V.Fst sField))
    readSegment :: (SegmentFetching ss sName) => Proxy sField -> m (Either Text (SegmentS sName))
    readSegment ps = do
      let fPath = sPath <> "/" <> T.unpack (toUniqueText ps)
      bs <- BS.readFile fPath
      pure $ deserialiseSegment t bs

    proxyNp :: NP Proxy (ToSegmentFields ss)
    proxyNp = pure_NP Proxy


makeEventPath :: FilePath -> FilePath
makeEventPath fp = fp <> "/" <> "events"