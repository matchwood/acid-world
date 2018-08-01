{-# OPTIONS_GHC -fno-warn-orphans #-}

module Shared.App (
  module Shared.App,
  module Shared.Schema
  ) where

import RIO
import qualified  RIO.ByteString as BS
import qualified  RIO.ByteString.Lazy as BL

import Prelude(userError, putStrLn)
import qualified RIO.Text as T
import qualified RIO.Directory as Dir

import Data.Proxy(Proxy(..))
import Test.QuickCheck.Instances()
import Test.QuickCheck as QC
import qualified System.IO.Temp as Temp
import qualified  System.FilePath as FilePath
import Acid.World
import Acid.Core.State.CacheState

import qualified Database.PostgreSQL.Simple as PSQL
import Shared.Schema
import Data.Char(toLower)

(^*) :: Int -> Int -> Int
(^*) = (^)

type AppValidSerialiserConstraint s = (
  AcidSerialiseEvent s,
  AcidSerialiseConstraintAll s AppSegments AppEvents,
  AcidSerialiseT s ~ BL.ByteString,
  AcidSerialiseConduitT s ~ BS.ByteString,
  AcidSerialiseSegmentT s ~ BS.ByteString,
  AcidDeserialiseSegmentT s ~ BS.ByteString,
  AcidSerialiseConstraint s AppSegments "insertUser",
  AcidSerialiseConstraint s AppSegments "insertAddress",
  AcidSerialiseConstraint s AppSegments "insertPhonenumber",
  ValidSegmentsSerialise s AppSegments
  )

type AppValidBackendConstraint b = (
  AcidWorldBackend b,
  AWBSerialiseT b ~ BL.ByteString,
  AWBSerialiseConduitT b ~ BS.ByteString,
  AWBSerialiseSegmentT b ~ BS.ByteString,
  AWBDeserialiseSegmentT b ~ BS.ByteString

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

type CAppSegments = '["UsersHM", "AddressesHM", "PhonenumbersHM"]

type AppEvents = '["insertUser", "insertAddress", "insertPhonenumber"]

type AppAW s = AcidWorld AppSegments AppEvents s

type Middleware s = IO (AppAW s) -> IO (AppAW s)


mkTempDir :: IO (FilePath)
mkTempDir = do
  tmpP <- Dir.makeAbsolute topLevelTestDir
  Dir.createDirectoryIfMissing True tmpP
  liftIO $ Temp.createTempDirectory tmpP "test"



type ValidAppAcidSerialise s b = (
      AcidSerialiseT s ~ AWBSerialiseT b
    , AcidSerialiseConduitT s ~ AWBSerialiseConduitT b
    , AcidSerialiseSegmentT s ~ AWBSerialiseSegmentT b
    , AcidDeserialiseSegmentT s ~ AWBDeserialiseSegmentT b
    )

type ValidAppAcidSerialiseBS s b = (
    ValidAppAcidSerialise s b
  , AWBSerialiseT b ~ BL.ByteString


  )
openAppAcidWorldRestoreState :: (ValidAppAcidSerialise s AcidWorldBackendFS, AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents, ValidSegmentsSerialise s AppSegments) => AcidSerialiseEventOptions s -> String -> IO (AppAW s)
openAppAcidWorldRestoreState opts s = do
  t <- mkTempDir
  let e = topLevelStoredStateDir <> "/" <> "testState" <> "/" <> s
  copyDirectory e t
  aw <- throwEither $ openAcidWorld defaultSegmentsState emptyInvariants (AWBConfigFS t True) AWConfigPureState opts
  -- this is to force the internal state
  i <- query aw fetchUsersStats
  putStrLn $ T.unpack . utf8BuilderToText $ "Opened aw with " <> displayShow i
  pure aw

openAppAcidWorldFresh :: (ValidAppAcidSerialise s b, AcidWorldBackend b,  AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents, ValidSegmentsSerialise s AppSegments) => (FilePath -> AWBConfig b) -> (AcidSerialiseEventOptions s) -> IO (AppAW s)
openAppAcidWorldFresh bConf opts = openAppAcidWorldFreshWithInvariants bConf opts emptyInvariants

openAppAcidWorldFreshWithInvariants :: (ValidAppAcidSerialise s b, AcidWorldBackend b,  AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents, ValidSegmentsSerialise s AppSegments) => (FilePath -> AWBConfig b) -> (AcidSerialiseEventOptions s) -> Invariants AppSegments -> IO (AppAW s)
openAppAcidWorldFreshWithInvariants bConf opts invars = do
  td <- mkTempDir
  throwEither $ openAcidWorld defaultSegmentsState invars (bConf td) AWConfigPureState opts


type UseGzip = Bool
openAppAcidWorldFreshFS :: (ValidAppAcidSerialise s AcidWorldBackendFS, AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents, ValidSegmentsSerialise s AppSegments) => (AcidSerialiseEventOptions s) -> UseGzip -> IO (AppAW s)
openAppAcidWorldFreshFS opts useGzip = do
  openAppAcidWorldFresh (\td -> AWBConfigFS td useGzip) opts


openAcidWorldPostgresWithInvariants :: String -> Invariants AppSegments -> IO (AppAW AcidSerialiserPostgresql)
openAcidWorldPostgresWithInvariants testNameOrig invars = do
  let testName = map toLower testNameOrig
  let setupConf = PSQL.defaultConnectInfo {
              PSQL.connectUser = "acid_world_test",
              PSQL.connectPassword = "acid_world_test",
              PSQL.connectDatabase = "postgres"
              }
  conn <- PSQL.connect setupConf
  let testDbName = "acid_world_test_" ++ testName
      createQ = mconcat ["CREATE DATABASE ", fromString testDbName,  " WITH OWNER = acid_world_test  ENCODING = 'UTF8'  TABLESPACE = pg_default  LC_COLLATE = 'en_GB.UTF-8'  LC_CTYPE = 'en_GB.UTF-8'  CONNECTION LIMIT = -1;" ]
  _ <- PSQL.execute_ conn $ mconcat ["DROP DATABASE IF EXISTS ", fromString testDbName , ";"]

  _ <- PSQL.execute_ conn createQ
  _ <- PSQL.execute_ conn $ mconcat ["GRANT CONNECT, TEMPORARY ON DATABASE ", fromString testDbName, " TO public;"]
  _ <- PSQL.execute_ conn $ mconcat ["GRANT ALL ON DATABASE ", fromString testDbName, "  TO acid_world_test;"]
  PSQL.close conn
  let conf = setupConf {PSQL.connectDatabase = testDbName}
  conn2 <- PSQL.connect conf
  _ <- PSQL.execute_ conn2 storableEventCreateTable
  _ <- PSQL.execute_ conn2 $ createTable (Proxy :: Proxy UserIxSet)
  _ <- PSQL.execute_ conn2 $ createTable (Proxy :: Proxy AddressIxSet)
  _ <- PSQL.execute_ conn2 $ createTable (Proxy :: Proxy PhonenumberIxSet)

  PSQL.close conn2


  throwEither $ openAcidWorld defaultSegmentsState invars (AWBConfigPostgresql conf) AWConfigPureState AcidSerialiserPostgresqlOptions



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




{-

  Cache state
-}


openCacheStateFresh :: IO (Either AWException (CacheState CAppSegments))
openCacheStateFresh = do
  t <- mkTempDir
  openCacheState t

runInsertUserCS :: CacheState CAppSegments -> User -> IO ()
runInsertUserCS cs u = runUpdateCS cs (insertC (Proxy :: Proxy "UsersHM") (userId u) u)