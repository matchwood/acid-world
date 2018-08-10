
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
import Control.Arrow (left)
import qualified Crypto.Hash as Hash
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteArray as BA
import Paths_acid_world (getDataFileName)



data AcidWorldBackendFS

sha256 :: BL.ByteString -> Hash.Digest Hash.SHA256
sha256 = Hash.hashlazy


-- @todo all writing needs to be move to a separate thread


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

    -- @todo handle previous folder when restoring after failed checkpoint

    recoverFromPartialCheckpoint finalConf t

    -- create an archive directory for later
    hdls <- startOrResumeCurrentEventsLog finalConf t

    hdlV <- liftIO $ STM.atomically $ newTMVar hdls
    pure . pure $ AWBStateFS finalConf hdlV
  createCheckpointBackend s awu t = do


    let currentS = currentStateFolder (aWBStateFSConfig s)
        previousS = previousStateFolder (aWBStateFSConfig s)

    -- at present we can only recover from this when initialising
    whenM (Dir.doesDirectoryExist previousS) $ throwM (AWExceptionCheckpointError "Last checkpoint did not complete, no more checkpoints can be created until that one has been restore, which can be done by closing and reopening acid world")

    -- @todo add some exception handling here and possible recoveries - particularly for when there is already an uncompleted checkpoint!
    -- close the current events log and return current state - as soon as this completes updates can start runnning again
    let restoreCurrent = do
          whenM (Dir.doesDirectoryExist previousS) $ do
            Dir.removeDirectoryRecursive currentS
            Dir.renameDirectory previousS currentS
          startOrResumeCurrentEventsLog (aWBStateFSConfig s) t


    sToWrite <- modifyTMVarWithOnException (aWBStateFSEventsHandle s) restoreCurrent $ \(eHdl, cHdl) -> do
      st <- runQuery awu askStateNp

      liftIO $ hClose eHdl
      liftIO $ hClose cHdl

      -- move the current state directory
      Dir.renameDirectory currentS previousS

      -- start a new log in a new current folder
      hdl' <- startOrResumeCurrentEventsLog (aWBStateFSConfig s) t

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
    expectedLastEventId <- liftIO $ withTMVarSafe (aWBStateFSEventsHandle s) $ \(_, cHdl) -> do
      liftIO $ hSeek cHdl AbsoluteSeek 0
      empty <- hIsEOF cHdl
      if empty
        then pure . Right $ Nothing
        else fmap ((fmap Just  . eventIdFromText) . decodeUtf8Lenient) $ BS.hGetLine cHdl

    case expectedLastEventId of
      Left err -> pure . Left $ AWExceptionEventSerialisationError err
      Right lastEventId -> pure . Right  $ LoadEventsConduit $ \restConduit -> liftIO $
        withTMVarSafe (aWBStateFSEventsHandle s) $ \(eHdl, _) -> runConduitRes $
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
          loop (Just eId) Nothing _ = yield (Left $ "Expected event with id " <> showT eId <> " to be the last entry in this log but it was never encountered - it looks like the event log has been truncated")
          loop Nothing (Just _) _ =
             -- @essential @todo we encountered an event/parse failures (or multiple events/errors?) for events that weren't fully persisted - anything after this point should be thrown away with a logWarn (@log) and the events file itself should be truncated to this point - this is a recoverable error but we need to implement the recovery, so for now we just fail.

             yield (Left $ "Encountered event that was not fully persisted - recovery is possible but has not yet been implemented")

          loop e (Just we1) (Just we2) = yield we1 >> await >>= loop e (Just we2)
          loop (Just _) (Just (Left err)) Nothing = yield (Left err) -- the last entry is already an error
          -- this is the last entry
          loop (Just eId) (Just (Right we)) Nothing =
            if eId == wrappedEventId we
              then yield $ Right we
              else yield (Left $ "Events log check failed - expected final event with id " <> showT eId <> " but got " <> showT (wrappedEventId we))
  -- @todo restrict the IO actions? also think a bit more about bracketing here and possible error scenarios


  handleUpdateEventC serializer s awu _ ec prePersistHook postPersistHook = withTMVarSafe (aWBStateFSEventsHandle s) $ \(eHdl, cHdl) -> do
    eBind (runUpdateC awu ec) $ \(es, r, onSuccess, onFail) -> do
        stEs <- mkStorableEvents es

        case extractLastEventId stEs of
          Nothing -> onFail >> (pure . Left $ AWExceptionEventSerialisationError "Could not extract event id, this can only happen if the passed eventC contains no events")
          Just lastEventId -> do
            ioRPre <- onException (prePersistHook r) onFail

            (flip onException) onFail $ do
              BL.hPut eHdl $ serializer stEs
              hFlush eHdl
              -- write over the check
              liftIO $ hSeek cHdl AbsoluteSeek 0
              BS.hPut cHdl (encodeUtf8 $ eventIdToText lastEventId)
              hFlush cHdl
              onSuccess

            -- at this point the event has been definitively written
            ioRPost <- (postPersistHook r)

            pure . Right $ (r, (ioRPre, ioRPost))


startOrResumeCurrentEventsLog :: (MonadIO m, AcidSerialiseEvent t) => AWBConfig AcidWorldBackendFS -> AcidSerialiseEventOptions t -> m (Handle, Handle)
startOrResumeCurrentEventsLog c t = do
  readmeP <- getStateFolderReadme
  Dir.copyFile readmeP ((aWBConfigFSStateDir c) <> "/README.md")
  let currentS = currentStateFolder c
  Dir.createDirectoryIfMissing True currentS
  Dir.copyFile readmeP (currentS <> "/README.md")
  let eventPath = makeEventPathWith currentS t
      eventCPath = makeEventsCheckPathWith currentS t
  -- at the moment this is fragile with respect to exceptions
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

recoverFromPartialCheckpoint :: (MonadUnliftIO m, AcidSerialiseEvent t) => AWBConfig AcidWorldBackendFS -> AcidSerialiseEventOptions t -> m ()
recoverFromPartialCheckpoint c t = do
  -- @todo check that this recovers from all possible states (missing current log etc)
  let partialN = "/partial"
      restoreN = "/restore"
      recoveredN = "/recovered"
      previousS = previousStateFolder c
      currentS = currentStateFolder c
      partialS = previousS <> partialN -- where the old 'current' state lives
      restoreS = previousS <> restoreN -- for the eventlogs in 'previous'

  whenM (Dir.doesDirectoryExist previousS) $ do

    whenM (Dir.doesDirectoryExist currentS) $ Dir.renameDirectory currentS partialS
    -- we move the previous logs and check to a temporary folder
    let previousEventsLogP = makeEventPathWith previousS t
        previousEventsLogCheckP = makeEventsCheckPathWith previousS t
        restoreEventsLogP = makeEventPathWith restoreS t
        restoreEventsLogCheckP = makeEventsCheckPathWith restoreS t
        partialEventsLogP = makeEventPathWith partialS t
        partialEventsLogCheckP = makeEventsCheckPathWith partialS t

    Dir.createDirectoryIfMissing False restoreS

    whenM (Dir.doesFileExist previousEventsLogP) $ Dir.renameFile previousEventsLogP restoreEventsLogP
    whenM (Dir.doesFileExist previousEventsLogCheckP) $ Dir.renameFile previousEventsLogCheckP restoreEventsLogCheckP

    -- create the new events log by appending the 'current' log (in 'partial') to the 'previous' log (in 'restore')
    runConduitRes $
      yieldMany [restoreEventsLogP, partialEventsLogP] .|
      awaitForever sourceFile .|
      sinkFileCautious previousEventsLogP
    -- if we did actually have a new event check then copy it, else use the old
    checkFileToCopy <- do
      hasContent <- do
        d <- Dir.doesFileExist partialEventsLogCheckP
        if d
          then fmap (not . BL.null) $ BL.readFile partialEventsLogCheckP
          else pure False
      if hasContent then pure $ partialEventsLogCheckP else pure $ restoreEventsLogCheckP
    Dir.copyFile checkFileToCopy previousEventsLogCheckP
    -- rename the 'previous' folder as the 'current' folder - this signals that restoration is complete
    Dir.renameDirectory previousS currentS
  -- we can delete these but for safety we just move them to another folder
  let recoveredS = currentS <> recoveredN
  whenM (Dir.doesDirectoryExist $ currentS <> partialN) $ do
    Dir.createDirectoryIfMissing False recoveredS
    Dir.renameDirectory (currentS <> partialN) (recoveredS <> partialN)
  whenM (Dir.doesDirectoryExist $ currentS <> restoreN) $ do
    Dir.createDirectoryIfMissing False recoveredS
    Dir.renameDirectory (currentS <> restoreN) (recoveredS <> restoreN)





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
              else runResourceT $ runConduit $
               (sourceFile segPath) .|
               middleware .|
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



makeEventPathWith :: AcidSerialiseEvent t => FilePath -> AcidSerialiseEventOptions t -> FilePath
makeEventPathWith stateFolder t = stateFolder <> "/" <> "events" <> serialiserFileExtension t

makeEventsCheckPathWith :: AcidSerialiseEvent t => FilePath -> AcidSerialiseEventOptions t -> FilePath
makeEventsCheckPathWith stateFolder t = makeEventPathWith stateFolder t <> ".check"

getStateFolderReadme :: (MonadIO m) => m FilePath
getStateFolderReadme = liftIO $ getDataFileName "src/dataFiles/stateFolderReadMe.md"