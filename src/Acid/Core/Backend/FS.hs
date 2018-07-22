
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.FS where
import RIO
import qualified RIO.Text as T
import System.IO (openBinaryFile, IOMode(..))
import qualified RIO.Directory as Dir
import qualified  RIO.ByteString as BS
import qualified  RIO.ByteString.Lazy as BL
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
import Data.Conduit.Zlib
import Control.Monad.ST.Trans
import Control.Arrow (left)

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
    aWBConfigFSStateDir :: FilePath,
    aWBConfigGzip :: Bool
  }
  backendConfigInfo c = "Gzip: " <> showT (aWBConfigGzip c)
  type AWBSerialiseT AcidWorldBackendFS = BL.ByteString
  type AWBSerialiseConduitT AcidWorldBackendFS = BS.ByteString
  initialiseBackend c  = do
    stateP <- Dir.makeAbsolute (aWBConfigFSStateDir c)
    Dir.createDirectoryIfMissing True stateP
    let eventPath = makeEventPath c
    hdl <- liftIO $ openBinaryFile eventPath ReadWriteMode
    hdlV <- liftIO $ STM.atomically $ newTMVar hdl
    pure . pure $ AWBStateFS c{aWBConfigFSStateDir = stateP} hdlV
  createCheckpointBackend s awu t = do
    sToWrite <- modifyTMVar (aWBStateFSEventsHandle s) $ \hdl -> do
      let eventPath = makeEventPath (aWBStateFSConfig s)
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
        let middleware = if (aWBConfigGzip  . aWBStateFSConfig $ s) then ungzip else awaitForever $ yield
        fmap (left AWExceptionSegmentDeserialisationError) $ readLastCheckpointState middleware ps (aWBStateFSConfig s) t
      else pure . pure $ Nothing

  closeBackend s = modifyTMVar (aWBStateFSEventsHandle s) $ \hdl -> do
    liftIO $ hClose hdl
    pure (error "AcidWorldBackendFS has been closed", ())

  loadEvents deserialiseConduit s = withTMVar (aWBStateFSEventsHandle s) $ \hdl -> do
      pure $
           sourceHandle hdl .|
           deserialiseConduit


  -- this should be bracketed and restored correctly, including bracketing the io action. Also the IO action should perhaps be restricted (if it loops to this then there will be trouble!). we also need a way to undo writing the event to the file if the io action fails
  handleUpdateEventC serializer s awu ec act = withTMVar (aWBStateFSEventsHandle s) $ \hdl -> do
    eBind (runUpdateC awu ec) $ \(es, r) -> do
        stEs <- mkStorableEvents es
        BL.hPut hdl $ serializer stEs
        hFlush hdl
        ioR <- act r
        pure . Right $ (r, ioR)



writeCheckpoint :: forall t sFields m. (AcidSerialiseConduitT t ~ BS.ByteString, MonadUnliftIO m, MonadThrow m, PrimMonad m, All (AcidSerialiseSegmentFieldConstraint t) sFields)  => AWBState AcidWorldBackendFS -> AcidSerialiseEventOptions t -> NP V.ElField sFields -> m ()
writeCheckpoint s t np = do
  let cpFolder = (aWBConfigFSStateDir . aWBStateFSConfig $ s) <> "/checkpoint"
  Dir.createDirectoryIfMissing True cpFolder
  let middleware = if (aWBConfigGzip  . aWBStateFSConfig $ s) then gzip else awaitForever $ yield
  let acts =  cfoldMap_NP (Proxy :: Proxy (AcidSerialiseSegmentFieldConstraint t)) ((:[]) . writeSegment middleware (aWBStateFSConfig s) t) np
  mapConcurrently_ id acts

writeSegment :: forall t m fs. (AcidSerialiseConduitT t ~ BS.ByteString, MonadUnliftIO m, AcidSerialiseSegmentFieldConstraint t fs) => ConduitT ByteString ByteString (ResourceT m) () -> AWBConfig AcidWorldBackendFS -> AcidSerialiseEventOptions t -> V.ElField fs -> m ()
writeSegment middleware c t ((V.Field seg)) = runConduitRes $
  serialiseSegment t seg .|
  middleware .|
  sinkFileCautious (makeSegmentPath c (Proxy :: Proxy (V.Fst fs)))




readLastCheckpointState :: forall ss m t. (ValidSegmentsSerialise t ss,  MonadUnliftIO m, AcidSerialiseConduitT t ~ BS.ByteString) =>  ConduitT ByteString ByteString (ResourceT m) () -> Proxy ss -> AWBConfig AcidWorldBackendFS -> AcidSerialiseEventOptions t -> m (Either Text (Maybe (SegmentsState ss)))
readLastCheckpointState middleware _ c t = (fmap . fmap) (Just . npToSegmentsState) segsNpE

  where
    segsNpE :: m (Either Text (NP V.ElField (ToSegmentFields ss)))
    segsNpE = runConcurrently $ unComp $ sequence'_NP segsNp
    segsNp :: NP ((Concurrently m  :.: Either Text) :.: V.ElField) (ToSegmentFields ss)
    segsNp = cmap_NP (Proxy :: Proxy (SegmentFieldSerialise ss t)) readSegmentFromProxy proxyNp
    readSegmentFromProxy :: forall a b. (AcidSerialiseSegmentFieldConstraint t '(a, b), b ~ SegmentS a) => Proxy '(a, b) -> ((Concurrently m :.: Either Text) :.: V.ElField) '(a, b)
    readSegmentFromProxy _ =  Comp $  fmap V.Field $  Comp $ readSegment (Proxy :: Proxy a)
    readSegment :: forall sName. (AcidSerialiseSegmentNameConstraint t sName) => Proxy sName -> Concurrently m (Either Text (SegmentS sName))
    readSegment ps = Concurrently $ runResourceT $ runSTT $ runConduit $
        transPipe lift (sourceFile $ makeSegmentPath c ps ) .|
        transPipe lift middleware .|
        deserialiseSegment t

    proxyNp :: NP Proxy (ToSegmentFields ss)
    proxyNp = pure_NP Proxy





makeSegmentPath :: (Segment sName) => AWBConfig AcidWorldBackendFS ->  Proxy sName -> FilePath
makeSegmentPath c ps = (aWBConfigFSStateDir c) <> "/checkpoint/" <> T.unpack (toUniqueText ps) <> if aWBConfigGzip c then ".gz" else ""


makeEventPath :: AWBConfig AcidWorldBackendFS -> FilePath
makeEventPath c = (aWBConfigFSStateDir c) <> "/" <> "events"