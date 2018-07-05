{-# LANGUAGE TemplateHaskell #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Shared.App where

import RIO
import qualified  RIO.ByteString as BS

import Prelude(userError, putStrLn)
import qualified RIO.Text as T
import qualified RIO.Time as Time
import qualified RIO.Directory as Dir

import Data.Proxy(Proxy(..))
import Test.QuickCheck.Arbitrary
import Test.QuickCheck.Instances()
import Test.QuickCheck as QC
import qualified System.IO.Temp as Temp
import qualified Generics.SOP as SOP
import qualified Generics.SOP.Arbitrary as SOP
import qualified Data.Aeson as Aeson
import qualified Data.IxSet.Typed as IxSet
import qualified  System.FilePath as FilePath
import Acid.World
import Codec.Serialise
import Data.SafeCopy

(^*) :: Int -> Int -> Int
(^*) = (^)

type AppValidSerialiserConstraint s = (
  AcidSerialiseEvent s,
  AcidSerialiseConstraintAll s AppSegments AppEvents,
  AcidSerialiseT s ~ BS.ByteString,
  AcidSerialiseConstraint s AppSegments "insertUser"
  )

type AppValidBackendConstraint b = (
  AcidWorldBackend b,
  AWBSerialiseT b ~ BS.ByteString
  )

allSerialisers :: [AppValidSerialiser]
allSerialisers = [
    AppValidSerialiser AcidSerialiserJSONOptions
  , AppValidSerialiser AcidSerialiserCBOROptions
  , AppValidSerialiser AcidSerialiserSafeCopyOptions
  ]


persistentBackends :: [AppValidBackend]
persistentBackends = [AppValidBackend $ fmap AWBConfigFS mkTempDir]

ephemeralBackends :: [AppValidBackend]
ephemeralBackends = [AppValidBackend $ pure AWBConfigMemory]

allBackends :: [AppValidBackend]
allBackends = persistentBackends ++ ephemeralBackends



data AppValidSerialiser where
  AppValidSerialiser :: (AppValidSerialiserConstraint s) => AcidSerialiseEventOptions s -> AppValidSerialiser
data AppValidBackend where
  AppValidBackend :: (AppValidBackendConstraint b) => IO (AWBConfig b) -> AppValidBackend


topLevelTestDir :: FilePath
topLevelTestDir = "./tmp"

topLevelStoredStateDir :: FilePath
topLevelStoredStateDir = "./var"

instance NFData (AcidWorld a n t) where
  rnf _ = ()

sampleUser :: User
sampleUser = User {
  userId = 23,
  userFirstName = "AA a sd8f90 sfsdaf9 sda8f0sad9f 8sad0f8sad fsadf sadfsadfsda0f98 sadf8 sa9df8sadfsdfs dfsadf",
  userLastName = "poiasdfo sdfihapioh3 0u09 ahsifo jsafr09wea ureajfl asjdfljsd f03w8o j[fapsi-gs0i",
  userCreated = Nothing,
  userDisabled = False
}

data User = User  {
  userId :: !Int,
  userFirstName :: !Text,
  userLastName :: !Text,
  userCreated :: !(Maybe Time.UTCTime),
  userDisabled :: !Bool
} deriving (Eq, Show, Generic, Ord)

instance Serialise User

type UserIxs = '[Int, Maybe Time.UTCTime, Bool]
type UserIxSet = IxSet.IxSet UserIxs User
instance IxSet.Indexable UserIxs User where
  indices = IxSet.ixList
              (IxSet.ixFun $ (:[]) . userId )
              (IxSet.ixFun $ (:[]) . userCreated )
              (IxSet.ixFun $ (:[]) . userDisabled )

instance Aeson.ToJSON User
instance Aeson.FromJSON User
instance SOP.Generic User
instance Arbitrary User where arbitrary = SOP.garbitrary
instance NFData User

instance Segment "Users" where
  type SegmentS "Users" = UserIxSet
  defaultState _ = IxSet.empty


insertUser :: (ValidAcidWorldState i ss, HasSegment ss  "Users") => User -> AWUpdate i ss User
insertUser a = do
  ls <- getSegment (Proxy :: Proxy "Users")
  let newLs = IxSet.insert a ls
  putSegment (Proxy :: Proxy "Users") newLs
  return a



instance Eventable "insertUser" where
  type EventArgs "insertUser" = '[User]
  type EventResult "insertUser" = User
  type EventSegments "insertUser" = '["Users"]
  runEvent _ = toRunEvent insertUser



fetchUsers :: (ValidAcidWorldState i ss, HasSegment ss  "Users") => AWQuery i ss [User]
fetchUsers = fmap IxSet.toList $ askSegment (Proxy :: Proxy "Users")

fetchUsersStats :: (ValidAcidWorldState i ss, HasSegment ss  "Users") => AWQuery i ss Int
fetchUsersStats = fmap IxSet.size $ askSegment (Proxy :: Proxy "Users")


generateUserIO :: IO User
generateUserIO = QC.generate arbitrary

generateUsers :: Int -> Gen [User]
generateUsers i = do
  us <- sequence $ replicate i (arbitrary)
  pure $ map (\(u, uid) -> u{userId = uid}) $ zip us [1..]


type AppSegments = '["Users"]
type AppEvents = '["insertUser"]

type AppAW s = AcidWorld AppSegments AppEvents s

type Middleware s = IO (AppAW s) -> IO (AppAW s)


mkTempDir :: IO (FilePath)
mkTempDir = do
  tmpP <- Dir.makeAbsolute topLevelTestDir
  Dir.createDirectoryIfMissing True tmpP
  liftIO $ Temp.createTempDirectory tmpP "test"




openAppAcidWorldRestoreState :: (AcidSerialiseT s ~ BS.ByteString, AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents) => AcidSerialiseEventOptions s -> String -> IO (AppAW s)
openAppAcidWorldRestoreState opts s = do
  t <- mkTempDir
  let e = topLevelStoredStateDir <> "/" <> "testState" <> "/" <> s
  copyDirectory e t
  aw <- throwEither $ openAcidWorld Nothing (AWBConfigFS t) AWConfigPureState opts
  -- this is to force the internal state
  i <- query aw fetchUsersStats
  putStrLn $ T.unpack . utf8BuilderToText $ "Opened aw with " <> displayShow i
  pure aw

openAppAcidWorldFresh :: (AcidSerialiseT s ~ BS.ByteString, AcidWorldBackend b, AWBSerialiseT b ~ BS.ByteString, AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents) => IO (AWBConfig b) -> (AcidSerialiseEventOptions s) -> IO (AppAW s)
openAppAcidWorldFresh bConfIO opts = do
  bConf <- bConfIO

  throwEither $ openAcidWorld Nothing bConf AWConfigPureState opts


openAppAcidWorldFreshFS :: (AcidSerialiseT s ~ BS.ByteString, AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents) => (AcidSerialiseEventOptions s) -> IO (AppAW s)
openAppAcidWorldFreshFS opts = openAppAcidWorldFresh (fmap AWBConfigFS mkTempDir) opts

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
runInsertUser aw u = update aw (mkEvent (Proxy :: Proxy ("insertUser")) u)



throwEither :: IO (Either Text a) -> IO a
throwEither act = do
  res <- act
  case res of
    Right a -> pure a
    Left err -> throwUserError $ T.unpack err

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


deriveSafeCopy 0 'base ''User