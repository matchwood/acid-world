{-# OPTIONS_GHC -fno-warn-orphans #-}

module Shared.App (
  module Shared.App,
  module Shared.Schema
  ) where

import RIO
import qualified  RIO.ByteString as BS

import Prelude(userError, putStrLn)
import qualified RIO.Text as T
import qualified RIO.Directory as Dir

import Data.Proxy(Proxy(..))
import Test.QuickCheck.Instances()
import Test.QuickCheck as QC
import qualified System.IO.Temp as Temp
import qualified  System.FilePath as FilePath
import Acid.World

import Shared.Schema

(^*) :: Int -> Int -> Int
(^*) = (^)

type AppValidSerialiserConstraint s = (
  AcidSerialiseEvent s,
  AcidSerialiseConstraintAll s AppSegments AppEvents,
  AcidSerialiseT s ~ Builder,
  AcidSerialiseConduitT s ~ BS.ByteString,
  AcidSerialiseConstraint s AppSegments "insertUser",
  AcidSerialiseConstraint s AppSegments "insertAddress",
  AcidSerialiseConstraint s AppSegments "insertPhonenumber",
  ValidSegmentsSerialise s AppSegments
  )

type AppValidBackendConstraint b = (
  AcidWorldBackend b,
  AWBSerialiseT b ~ Builder,
  AWBSerialiseConduitT b ~ BS.ByteString

  )


defaultAppSerialiser :: AppValidSerialiser
defaultAppSerialiser = AppValidSerialiser AcidSerialiserJSONOptions

allSerialisers :: [AppValidSerialiser]
allSerialisers = [
    AppValidSerialiser AcidSerialiserJSONOptions
  , AppValidSerialiser AcidSerialiserCBOROptions
  , AppValidSerialiser AcidSerialiserSafeCopyOptions
  ]


persistentBackends :: [AppValidBackend]
persistentBackends = [
     fsBackend
    ]

fsBackend :: AppValidBackend
fsBackend = AppValidBackend $ \t -> AWBConfigFS t True

persistentBackendsWithGzip :: [AppValidBackend]
persistentBackendsWithGzip = [
    AppValidBackend $ \t -> AWBConfigFS t False
  , AppValidBackend $ \t -> AWBConfigFS t True
  ]

ephemeralBackends :: [AppValidBackend]
ephemeralBackends = [AppValidBackend $ const AWBConfigMemory]

allBackends ::  [AppValidBackend]
allBackends = persistentBackends ++ ephemeralBackends

data AppValidSerialiser where
  AppValidSerialiser :: (AppValidSerialiserConstraint s) => AcidSerialiseEventOptions s -> AppValidSerialiser
data AppValidBackend where
  AppValidBackend :: (AppValidBackendConstraint b) => (FilePath -> (AWBConfig b)) -> AppValidBackend


topLevelTestDir :: FilePath
topLevelTestDir = "./tmp"

topLevelStoredStateDir :: FilePath
topLevelStoredStateDir = "./var"

instance NFData (AcidWorld a n t) where
  rnf _ = ()



type AppSegments = '["Users", "Addresses", "Phonenumbers"]
type AppEvents = '["insertUser", "insertAddress", "insertPhonenumber"]

type AppAW s = AcidWorld AppSegments AppEvents s

type Middleware s = IO (AppAW s) -> IO (AppAW s)


mkTempDir :: IO (FilePath)
mkTempDir = do
  tmpP <- Dir.makeAbsolute topLevelTestDir
  Dir.createDirectoryIfMissing True tmpP
  liftIO $ Temp.createTempDirectory tmpP "test"




openAppAcidWorldRestoreState :: (AcidSerialiseT s ~ Builder, AcidSerialiseConduitT s ~ BS.ByteString, AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents, ValidSegmentsSerialise s AppSegments) => AcidSerialiseEventOptions s -> String -> IO (AppAW s)
openAppAcidWorldRestoreState opts s = do
  t <- mkTempDir
  let e = topLevelStoredStateDir <> "/" <> "testState" <> "/" <> s
  copyDirectory e t
  aw <- throwEither $ openAcidWorld defaultSegmentsState emptyInvariants (AWBConfigFS t True) AWConfigPureState opts
  -- this is to force the internal state
  i <- query aw fetchUsersStats
  putStrLn $ T.unpack . utf8BuilderToText $ "Opened aw with " <> displayShow i
  pure aw

openAppAcidWorldFresh :: (AcidSerialiseT s ~ Builder, AWBSerialiseT b ~ Builder, AcidSerialiseConduitT s ~ BS.ByteString, AWBSerialiseConduitT b ~ BS.ByteString, AcidWorldBackend b,  AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents, ValidSegmentsSerialise s AppSegments) => (FilePath -> AWBConfig b) -> (AcidSerialiseEventOptions s) -> IO (AppAW s)
openAppAcidWorldFresh bConf opts = openAppAcidWorldFreshWithInvariants bConf opts emptyInvariants

openAppAcidWorldFreshWithInvariants :: (AcidSerialiseT s ~ Builder, AWBSerialiseT b ~ Builder, AcidSerialiseConduitT s ~ BS.ByteString, AWBSerialiseConduitT b ~ BS.ByteString, AcidWorldBackend b,  AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents, ValidSegmentsSerialise s AppSegments) => (FilePath -> AWBConfig b) -> (AcidSerialiseEventOptions s) -> Invariants AppSegments -> IO (AppAW s)
openAppAcidWorldFreshWithInvariants bConf opts invars = do
  td <- mkTempDir
  throwEither $ openAcidWorld defaultSegmentsState invars (bConf td) AWConfigPureState opts


type UseGzip = Bool
openAppAcidWorldFreshFS :: (AcidSerialiseT s ~ Builder, AcidSerialiseConduitT s ~ BS.ByteString, AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents, ValidSegmentsSerialise s AppSegments) => (AcidSerialiseEventOptions s) -> UseGzip -> IO (AppAW s)
openAppAcidWorldFreshFS opts useGzip = do
  openAppAcidWorldFresh (\td -> AWBConfigFS td useGzip) opts

closeAndReopen :: Middleware s
closeAndReopen = reopenAcidWorldMiddleware . closeAcidWorldMiddleware

closeAcidWorldMiddleware :: Middleware s
closeAcidWorldMiddleware iAw = do
  aw <- iAw
  closeAcidWorld aw
  pure aw

reopenAcidWorldMiddleware :: Middleware s
reopenAcidWorldMiddleware iAw = iAw >>= throwEither . reopenAcidWorld


insertUsers :: AcidSerialiseConstraint s AppSegments "insertUser" => Int -> Middleware s
insertUsers i iAw = do
  aw <- iAw
  us <- QC.generate $ generateUsers i
  mapM_ (runInsertUser aw) us
  pure aw


runInsertUser :: AcidSerialiseConstraint s AppSegments "insertUser" => AppAW s -> User -> IO User
runInsertUser aw u = throwEither $ update aw (mkEvent (Proxy :: Proxy ("insertUser")) u)

runInsertUserC :: AcidSerialiseConstraintAll s AppSegments '["insertPhonenumber", "insertUser"] => AppAW s -> (User -> Event "insertPhonenumber") -> User -> IO Phonenumber
runInsertUserC aw mkNumber u = throwEither $ updateC aw $ (mkNumber :<< (EventC $ mkEvent (Proxy :: Proxy ("insertUser")) u))



runInsertAddress :: AcidSerialiseConstraint s AppSegments "insertAddress" => AppAW s -> Address -> IO Address
runInsertAddress aw u = throwEither $ update aw (mkEvent (Proxy :: Proxy ("insertAddress")) u)

runInsertPhonenumber :: AcidSerialiseConstraint s AppSegments "insertPhonenumber" => AppAW s -> Phonenumber -> IO Phonenumber
runInsertPhonenumber aw u = throwEither $ update aw (mkEvent (Proxy :: Proxy ("insertPhonenumber")) u)


throwEither :: Exception e => IO (Either e a) -> IO a
throwEither act = do
  res <- act
  case res of
    Right a -> pure a
    Left e -> throwM e

throwUserError :: String -> IO a
throwUserError = throwIO . userError










copyDirectory :: FilePath -> FilePath -> IO ()
copyDirectory oldO newO = do
  old <- Dir.makeAbsolute oldO
  new <- Dir.makeAbsolute newO
  testExist <- Dir.doesDirectoryExist old
  when (not testExist) (throwIO $ userError $ "Source directory " <> old <> " does not exist")

  allFiles <- getAbsDirectoryContentsRecursive old
  let ts = map (\f -> (f, toNewPath old new f)) allFiles
  void $ mapM (uncurry copyOldToNew) ts
  return ()

  where
    toNewPath :: FilePath -> FilePath -> FilePath -> FilePath
    toNewPath old new file = new <> "/" <> FilePath.makeRelative old file
    copyOldToNew :: FilePath -> FilePath -> IO ()
    copyOldToNew oldF newF = do
      Dir.createDirectoryIfMissing True (FilePath.takeDirectory newF)
      Dir.copyFile oldF newF

getAbsDirectoryContentsRecursive :: FilePath -> IO [FilePath]
getAbsDirectoryContentsRecursive dirPath = do
  names <- Dir.getDirectoryContents dirPath
  let properNames = filter (`notElem` [".", ".."]) names
  absoluteNames <- mapM (Dir.canonicalizePath . (dirPath FilePath.</>)) properNames
  paths <- forM absoluteNames $ \fPath -> do
    isDirectory <- Dir.doesDirectoryExist fPath
    if isDirectory
      then getAbsDirectoryContentsRecursive fPath
      else return [fPath]
  return $ concat paths


