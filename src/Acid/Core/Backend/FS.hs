
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
import qualified Crypto.Hash as Hash
import qualified Data.ByteArray.Encoding as BA

data AcidWorldBackendFS

sha256 :: BL.ByteString -> Hash.Digest Hash.SHA256
sha256 = Hash.hashlazy

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
    let finalConf = c{aWBConfigFSStateDir = stateP}
    let currentS = currentStateFolder finalConf
    Dir.createDirectoryIfMissing True currentS
    let eventPath = makeEventPath finalConf
    hdl <- liftIO $ openBinaryFile eventPath ReadWriteMode
    hdlV <- liftIO $ STM.atomically $ newTMVar hdl
    pure . pure $ AWBStateFS finalConf hdlV
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
    let cpFolder = currentCheckpointFolder (aWBStateFSConfig $ s)
    doesExist <- Dir.doesDirectoryExist cpFolder
    if doesExist
      then do
        let middleware = if (aWBConfigGzip  . aWBStateFSConfig $ s) then ungzip else awaitForever $ yield
        fmap (left AWExceptionSegmentDeserialisationError) $ readLastCheckpointState middleware ps s t
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
  let cpFolder = currentCheckpointFolder (aWBStateFSConfig $ s)
  Dir.createDirectoryIfMissing True cpFolder
  let middleware = if (aWBConfigGzip  . aWBStateFSConfig $ s) then gzip else awaitForever $ yield
  let acts =  cfoldMap_NP (Proxy :: Proxy (AcidSerialiseSegmentFieldConstraint t)) ((:[]) . writeSegment middleware s t) np
  mapConcurrently_ id acts

writeSegment :: forall t m fs. (AcidSerialiseConduitT t ~ BS.ByteString, MonadUnliftIO m, AcidSerialiseSegmentFieldConstraint t fs) => ConduitT ByteString ByteString (ResourceT m) () -> AWBState AcidWorldBackendFS -> AcidSerialiseEventOptions t -> V.ElField fs -> m ()
writeSegment middleware s t ((V.Field seg)) = do
  let segPath = makeSegmentPath (aWBStateFSConfig $ s) (Proxy :: Proxy (V.Fst fs))
  runConduitRes $
    serialiseSegment t seg .|
    middleware .|
    sinkFileCautious segPath
  writeSegmentCheckFile
  where
    writeSegmentCheckFile :: m ()
    writeSegmentCheckFile = do
      let segPath = makeSegmentPath (aWBStateFSConfig $ s) (Proxy :: Proxy (V.Fst fs))
          segCheckPath = makeSegmentCheckPath (aWBStateFSConfig $ s) (Proxy :: Proxy (V.Fst fs))
      hash <- fmap sha256 $ BL.readFile segPath
      -- @ todo redo this with the hashing process inside the conduit reading from the file
      runConduitRes $
        sourceLazy (BL.fromStrict $ BA.convertToBase BA.Base64 hash ) .|
        sinkFile segCheckPath




readLastCheckpointState :: forall ss m t. (ValidSegmentsSerialise t ss,  MonadUnliftIO m, AcidSerialiseConduitT t ~ BS.ByteString) =>  ConduitT ByteString ByteString (ResourceT m) () -> Proxy ss -> AWBState AcidWorldBackendFS -> AcidSerialiseEventOptions t -> m (Either Text (Maybe (SegmentsState ss)))
readLastCheckpointState middleware _ s t = (fmap . fmap) (Just . npToSegmentsState) segsNpE

  where
    segsNpE :: m (Either Text (NP V.ElField (ToSegmentFields ss)))
    segsNpE = runConcurrently $ unComp $ sequence'_NP segsNp
    segsNp :: NP ((Concurrently m  :.: Either Text) :.: V.ElField) (ToSegmentFields ss)
    segsNp = cmap_NP (Proxy :: Proxy (SegmentFieldSerialise ss t)) readSegmentFromProxy proxyNp
    readSegmentFromProxy :: forall a b. (AcidSerialiseSegmentFieldConstraint t '(a, b), b ~ SegmentS a) => Proxy '(a, b) -> ((Concurrently m :.: Either Text) :.: V.ElField) '(a, b)
    readSegmentFromProxy _ =  Comp $  fmap V.Field $  Comp $ readSegment (Proxy :: Proxy a)
    readSegment :: forall sName. (AcidSerialiseSegmentNameConstraint t sName) => Proxy sName -> Concurrently m (Either Text (SegmentS sName))
    readSegment ps = Concurrently $ do
      -- @ todo redo this with the hashing process inside a conduit reading from the file

      let segPath = makeSegmentPath (aWBStateFSConfig $ s) ps
          segCheckPath = makeSegmentCheckPath (aWBStateFSConfig $ s) ps
      hash <- fmap (BA.convertToBase BA.Base64 . sha256) $ BL.readFile segPath
      checkHash <- BS.readFile segCheckPath
      if hash /= (checkHash)
        then pure . Left $ "Invalid checkpoint - check file ("  <> showT segCheckPath <> ": " <> showT checkHash <> ") did not match hash of segment file" <> "(" <> showT segPath <> ": " <>  showT hash <> ")" <> " when reading segment *" <> toUniqueText ps <> "*"
        else runResourceT $ runSTT $ runConduit $
        transPipe lift (sourceFile $ makeSegmentPath (aWBStateFSConfig $ s) ps ) .|
        transPipe lift middleware .|
        deserialiseSegment t

    proxyNp :: NP Proxy (ToSegmentFields ss)
    proxyNp = pure_NP Proxy


currentStateFolder :: AWBConfig AcidWorldBackendFS -> FilePath
currentStateFolder c = (aWBConfigFSStateDir $ c) <> "/current"

currentCheckpointFolder :: AWBConfig AcidWorldBackendFS -> FilePath
currentCheckpointFolder c = currentStateFolder c <> "/checkpoint"

makeSegmentPath :: (Segment sName) => AWBConfig AcidWorldBackendFS ->  Proxy sName -> FilePath
makeSegmentPath c ps = currentCheckpointFolder c <> "/" <> T.unpack (toUniqueText ps) <> if aWBConfigGzip c then ".gz" else ""

makeSegmentCheckPath :: (Segment sName) => AWBConfig AcidWorldBackendFS ->  Proxy sName -> FilePath
makeSegmentCheckPath c ps = makeSegmentPath c ps <> ".check"



makeEventPath :: AWBConfig AcidWorldBackendFS -> FilePath
makeEventPath c = currentStateFolder c <> "/" <> "events"