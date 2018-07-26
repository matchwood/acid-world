
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.FS where
import RIO
import qualified RIO.Text as T
import System.IO (openBinaryFile, IOMode(..), SeekMode(..))
import qualified RIO.Directory as Dir
import qualified RIO.ByteString as BS
import qualified RIO.ByteString.Lazy as BL
import qualified RIO.Time as Time

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
import qualified Data.ByteArray as BA
import Paths_acid_world (getDataFileName)



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
    aWBStateFSEventsHandle :: TMVar.TMVar (Handle, Handle)
  }
  data AWBConfig AcidWorldBackendFS = AWBConfigFS {
    aWBConfigFSStateDir :: FilePath,
    aWBConfigGzip :: Bool
  }
  backendConfigInfo c = "Gzip: " <> showT (aWBConfigGzip c)
  type AWBSerialiseT AcidWorldBackendFS = BL.ByteString
  type AWBSerialiseConduitT AcidWorldBackendFS = BS.ByteString
  initialiseBackend c t = do
    stateP <- Dir.makeAbsolute (aWBConfigFSStateDir c)
    let finalConf = c{aWBConfigFSStateDir = stateP}
    -- create an archive directory for later
    hdls <- startOrResumeCurrentEventsLog finalConf t

    hdlV <- liftIO $ STM.atomically $ newTMVar hdls
    pure . pure $ AWBStateFS finalConf hdlV
  createCheckpointBackend s awu t = do

    let currentS = currentStateFolder (aWBStateFSConfig s)
        previousS = previousStateFolder (aWBStateFSConfig s)
    -- close the current events log and return current state - as soon as this completes updates can start runnning again
    sToWrite <- modifyTMVar (aWBStateFSEventsHandle s) $ \(eHdl, cHdl) -> do
      liftIO $ hClose eHdl
      liftIO $ hClose cHdl
      -- move the current state directory
      Dir.renameDirectory currentS previousS
      -- state a new log in a new current folder
      hdl' <- startOrResumeCurrentEventsLog (aWBStateFSConfig s) t
      st <- runQuery awu askStateNp
      pure (hdl', st)
    writeCheckpoint s t sToWrite
    -- at this point the current state directory contains everything needed, so we can archive the previous state
    now <- Time.getCurrentTime
    let archiveS = archiveStateFolder (aWBStateFSConfig s)
    Dir.createDirectoryIfMissing True archiveS
    let newArchivePath = archiveS <> "/" <> Time.formatTime Time.defaultTimeLocale archiveTimeFormat now
    Dir.renameDirectory previousS newArchivePath

  getInitialState defState s t = do
    let cpFolder = currentCheckpointFolder (aWBStateFSConfig $ s)
    doesExist <- Dir.doesDirectoryExist cpFolder
    if doesExist
      then do
        let middleware = if (aWBConfigGzip  . aWBStateFSConfig $ s) then ungzip else awaitForever $ yield
        fmap (left AWExceptionSegmentDeserialisationError) $ readLastCheckpointState middleware defState s t cpFolder
      else pure . pure $ defState

  closeBackend s = modifyTMVar (aWBStateFSEventsHandle s) $ \(eHdl, cHdl) -> do
    liftIO $ hClose eHdl
    liftIO $ hClose cHdl
    pure (error "AcidWorldBackendFS has been closed", ())

  loadEvents deserialiseConduit s _ =  do
    expectedLastEventId <- liftIO $ withTMVar (aWBStateFSEventsHandle s) $ \(_, cHdl) -> do
      liftIO $ hSeek cHdl AbsoluteSeek 0
      empty <- hIsEOF cHdl
      if empty
        then pure . Right $ Nothing
        else fmap ((fmap Just  . eventIdFromText) . decodeUtf8Lenient) $ BS.hGetLine cHdl

    case expectedLastEventId of
      Left err -> pure . Left $ AWExceptionEventSerialisationError err
      Right lastEventId -> pure . Right  $ LoadEventsConduit $ \restConduit -> liftIO $
        withTMVar (aWBStateFSEventsHandle s) $ \(eHdl, _) -> runConduitRes $
          sourceHandle eHdl .|
          deserialiseConduit .|
          checkLastEventIdConduit lastEventId .|
          restConduit

    where
      checkLastEventIdConduit :: Maybe EventId -> (ConduitT (Either Text (WrappedEvent ss nn)) (Either Text (WrappedEvent ss nn)) (ResourceT IO) ())
      checkLastEventIdConduit inEId = await >>= (\f -> await >>= loop inEId f)
        where
          loop :: Maybe EventId -> Maybe (Either Text (WrappedEvent ss nn)) -> Maybe (Either Text (WrappedEvent ss nn)) -> (ConduitT (Either Text (WrappedEvent ss nn)) (Either Text (WrappedEvent ss nn)) (ResourceT IO) ())
          loop Nothing Nothing _ = pure ()
          loop (Just eId) Nothing _ = yield (Left $ "Expected event with id " <> showT eId <> " to be the last entry in this log but it was never encountered")
          loop Nothing (Just _) _ = pure () -- @essential @todo we encountered an event (or multiple events?) that weren't fully persisted - anything after this point should be thrown away - also we should logWarn this (@log)
          loop e (Just we1) (Just we2) = yield we1 >> await >>= loop e (Just we2)
          loop (Just _) (Just (Left err)) Nothing = yield (Left err) -- the last entry is already an error
          -- this is the last entry
          loop (Just eId) (Just (Right we)) Nothing =
            if eId == wrappedEventId we
              then yield $ Right we
              else yield (Left $ "Events log check failed - expected final event with id " <> showT eId <> " but got " <> showT (wrappedEventId we))
  -- @todo this should be bracketed including bracketing the io action. Also the IO action should perhaps be restricted (if it loops to this then there will be trouble!). The internal state also needs to be rolled back if any part of this fails
  handleUpdateEventC serializer s awu _ ec act = withTMVar (aWBStateFSEventsHandle s) $ \(eHdl, cHdl) -> do
    eBind (runUpdateC awu ec) $ \(es, r, onSuccess, onFail) -> do
        stEs <- mkStorableEvents es

        case extractLastEventId stEs of
          Nothing -> onFail >> (pure . Left $ AWExceptionEventSerialisationError "Could not extract event id, this can only happen if the passed eventC contains no events")
          Just lastEventId -> do
            ioR <- act r

            BL.hPut eHdl $ serializer stEs
            hFlush eHdl
            -- write over the check
            liftIO $ hSeek cHdl AbsoluteSeek 0
            BS.hPut cHdl (encodeUtf8 $ eventIdToText lastEventId)
            hFlush cHdl
            -- at this point the event has been definitively written
            onSuccess
            pure . Right $ (r, ioR)


startOrResumeCurrentEventsLog :: (MonadIO m, AcidSerialiseEvent t) => AWBConfig AcidWorldBackendFS -> AcidSerialiseEventOptions t -> m (Handle, Handle)
startOrResumeCurrentEventsLog c t = do
  readmeP <- getStateFolderReadme
  Dir.copyFile readmeP ((aWBConfigFSStateDir c) <> "/README.md")
  let currentS = currentStateFolder c
  Dir.createDirectoryIfMissing True currentS
  Dir.copyFile readmeP (currentS <> "/README.md")
  let eventPath = makeEventPath c t
      eventCPath = makeEventsCheckPath c t
  eHandle <- liftIO $ openBinaryFile eventPath ReadWriteMode
  cHandle <- liftIO $ openBinaryFile eventCPath ReadWriteMode
  pure (eHandle, cHandle)


writeCheckpoint :: forall t sFields m. (AcidSerialiseEvent t, AcidSerialiseSegmentT t ~ BS.ByteString, MonadUnliftIO m, MonadThrow m, PrimMonad m, All (AcidSerialiseSegmentFieldConstraint t) sFields)  => AWBState AcidWorldBackendFS -> AcidSerialiseEventOptions t -> NP V.ElField sFields -> m ()
writeCheckpoint s t np = do
  let cpFolderFinal = currentCheckpointFolder (aWBStateFSConfig $ s)
      cpFolderTemp = cpFolderFinal <> ".temp"
  Dir.createDirectoryIfMissing True cpFolderTemp

  let middleware = if (aWBConfigGzip  . aWBStateFSConfig $ s) then gzip else awaitForever $ yield
  let acts =  cfoldMap_NP (Proxy :: Proxy (AcidSerialiseSegmentFieldConstraint t)) ((:[]) . writeSegment middleware s t cpFolderTemp) np
  mapConcurrently_ id acts

  Dir.renameDirectory cpFolderTemp cpFolderFinal

writeSegment :: forall t m fs. (AcidSerialiseEvent t, AcidSerialiseSegmentT t ~ BS.ByteString, MonadUnliftIO m, MonadThrow m, AcidSerialiseSegmentFieldConstraint t fs) => ConduitT ByteString ByteString (ResourceT m) () -> AWBState AcidWorldBackendFS -> AcidSerialiseEventOptions t -> FilePath -> V.ElField fs -> m ()
writeSegment middleware s t dir (V.Field seg) = do

  let pp = (Proxy :: Proxy (V.Fst fs))
      segPath = makeSegmentPath dir (aWBStateFSConfig $ s) pp t
      segCheckPath = makeSegmentCheckPath dir (aWBStateFSConfig $ s) pp t

  -- we simultaneously write out the segment file and the calculated segment hash file, and check them afterwards to verify consistency of the write
  _ <-
    runConduitRes $
      serialiseSegment t seg .|
      middleware .|
      sequenceSinks [(sinkFileCautious segPath), (sha256Transformer .| sinkFileCautious segCheckPath)]

  res <- getSegmentHashes segPath segCheckPath
  case res of
    Left err -> throwM $ AWExceptionEventSerialisationError $ prettySegment pp <> "Error when checking segment write: " <> err
    Right (hash, checkHash) -> when (hash /= checkHash) $
        throwM $ AWExceptionEventSerialisationError  $ prettySegment pp <> "Segment check hash did not match written segment file - corruption occurred while writing"




readLastCheckpointState :: forall ss m t. (AcidSerialiseEvent t, ValidSegmentsSerialise t ss,  MonadUnliftIO m, AcidDeserialiseSegmentT t ~ BS.ByteString) =>  ConduitT ByteString ByteString (ResourceT m) () -> SegmentsState ss -> AWBState AcidWorldBackendFS -> AcidSerialiseEventOptions t -> FilePath ->  m (Either Text (SegmentsState ss))
readLastCheckpointState middleware defState s t dir = (fmap . fmap) (npToSegmentsState) segsNpE

  where
    segsNpE :: m (Either Text (NP V.ElField (ToSegmentFields ss)))
    segsNpE = runConcurrently $ unComp $ sequence'_NP segsNp
    segsNp :: NP ((Concurrently m  :.: Either Text) :.: V.ElField) (ToSegmentFields ss)
    segsNp = cmap_NP (Proxy :: Proxy (SegmentFieldSerialise ss t)) readSegmentFromProxy proxyNp
    readSegmentFromProxy :: forall a b. (AcidSerialiseSegmentFieldConstraint t '(a, b), b ~ SegmentS a, HasSegment ss a) => Proxy '(a, b) -> ((Concurrently m :.: Either Text) :.: V.ElField) '(a, b)
    readSegmentFromProxy _ =  Comp $  fmap V.Field $  Comp $ readSegment (Proxy :: Proxy a)
    readSegment :: forall sName. (AcidSerialiseSegmentNameConstraint t sName, HasSegment ss sName) => Proxy sName -> Concurrently m (Either Text (SegmentS sName))
    readSegment ps = Concurrently $ do
      let segPath = makeSegmentPath dir (aWBStateFSConfig $ s) ps t
          segCheckPath = makeSegmentCheckPath dir (aWBStateFSConfig $ s) ps t

      (segPathExists, segCheckPathExists) <- do
        a <- Dir.doesFileExist segPath
        b <- Dir.doesFileExist segCheckPath
        pure (a, b)

      case (segPathExists, segCheckPathExists) of
        (False, False) -> pure . Right $ getSegmentP ps defState
        (True, False) -> pure . Left $ prettySegment ps <> "Segment check file could not be found at " <> showT segCheckPath <> ". If you are confident that your segment file contains the correct data then you can fix this error by manually creating a segment check file at that path with the output of `sha256sum "<> showT segPath <> "`"
        (False, True) -> pure . Left $  prettySegment ps <> "Segment file missing at " <> showT segPath <> ". This is almost certainly due to some kind of data corruption. To clear this error you can delete the check file at " <> showT segCheckPath <> " but that will revert the system to using the default state defined for this segment"
        (True, True) -> do
          eBind (getSegmentHashes segPath segCheckPath) $ \(hash, checkHash) -> do
            if hash /= checkHash
              then pure . Left $ "Invalid checkpoint - check file ("  <> showT segCheckPath <> ": " <> showT checkHash <> ") did not match hash of segment file" <> "(" <> showT segPath <> ": " <>  showT hash <> ")" <> " when reading segment *" <> toUniqueText ps <> "*"
              else runResourceT $ runSTT $ runConduit $
              transPipe lift (sourceFile segPath) .|
              transPipe lift middleware .|
              deserialiseSegment t

    proxyNp :: NP Proxy (ToSegmentFields ss)
    proxyNp = pure_NP Proxy


getSegmentHashes :: (MonadUnliftIO m) =>  FilePath -> FilePath -> m (Either Text (Hash.Digest Hash.SHA256, Hash.Digest Hash.SHA256))
getSegmentHashes sp scp = do
  eBind (readSha256FromFile scp) $ \hb -> do
    ha <- sha256ForFile sp
    pure . pure $ (ha, hb)



sha256Transformer :: forall m. (Monad m) => ConduitT BS.ByteString BS.ByteString m ()
sha256Transformer = await >>= loop (Hash.hashInit)
  where
    loop :: Hash.Context Hash.SHA256 -> Maybe (BS.ByteString) -> ConduitT BS.ByteString BS.ByteString m ()
    loop ctx (Just bs) = await >>= loop (Hash.hashUpdate ctx bs)
    loop ctx (Nothing) = yield (BA.convertToBase BA.Base16 (Hash.hashFinalize ctx))


sha256Sink ::(Monad m) => ConduitT BS.ByteString o m (Hash.Digest Hash.SHA256)
sha256Sink =  fmap Hash.hashFinalize $ foldlC Hash.hashUpdate Hash.hashInit

sha256ForFile :: (MonadUnliftIO m) => FilePath -> m (Hash.Digest Hash.SHA256)
sha256ForFile fp = runConduitRes $
  sourceFile fp .| sha256Sink

readSha256FromFile :: (MonadUnliftIO m) => FilePath -> m (Either Text (Hash.Digest Hash.SHA256))
readSha256FromFile fp = do
  bs <- fmap (BA.convertFromBase BA.Base16) $ BS.readFile fp
  case bs of
    Left e -> pure . Left $ "Could not convert file contents to Base16 Bytes from " <> showT fp <> ": " <> T.pack e
    Right (b :: BA.Bytes) ->
      case Hash.digestFromByteString b of
        Nothing -> pure . Left $ "Could not read sha256 digest from " <> showT fp
        Just a -> pure . pure $ a

currentStateFolder :: AWBConfig AcidWorldBackendFS -> FilePath
currentStateFolder c = (aWBConfigFSStateDir $ c) <> "/current"

previousStateFolder :: AWBConfig AcidWorldBackendFS -> FilePath
previousStateFolder c = (aWBConfigFSStateDir $ c) <> "/previous"


archiveTimeFormat :: String
archiveTimeFormat = "%0Y-%m-%d_%H-%M-%S-%6q_UTC"

archiveStateFolder :: AWBConfig AcidWorldBackendFS -> FilePath
archiveStateFolder c = (aWBConfigFSStateDir c) <> "/archive"


currentCheckpointFolder :: AWBConfig AcidWorldBackendFS -> FilePath
currentCheckpointFolder c = currentStateFolder c <> "/checkpoint"

makeSegmentPath :: (Segment sName, AcidSerialiseEvent t) => FilePath -> AWBConfig AcidWorldBackendFS ->  Proxy sName -> AcidSerialiseEventOptions t -> FilePath
makeSegmentPath cFolder c ps t = cFolder <> "/" <> T.unpack (toUniqueText ps) <> serialiserFileExtension t <> if aWBConfigGzip c then ".gz" else ""

makeSegmentCheckPath :: (Segment sName, AcidSerialiseEvent t) => FilePath -> AWBConfig AcidWorldBackendFS ->  Proxy sName -> AcidSerialiseEventOptions t -> FilePath
makeSegmentCheckPath dir c ps t = makeSegmentPath dir c ps t <> ".check"



makeEventPath :: AcidSerialiseEvent t => AWBConfig AcidWorldBackendFS -> AcidSerialiseEventOptions t -> FilePath
makeEventPath c t = currentStateFolder c <> "/" <> "events" <> serialiserFileExtension t

makeEventsCheckPath :: AcidSerialiseEvent t => AWBConfig AcidWorldBackendFS -> AcidSerialiseEventOptions t -> FilePath
makeEventsCheckPath c t = makeEventPath c t <> ".check"

getStateFolderReadme :: (MonadIO m) => m FilePath
getStateFolderReadme = liftIO $ getDataFileName "src/dataFiles/stateFolderReadMe.md"