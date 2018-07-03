{-# OPTIONS_GHC -fno-warn-orphans #-}

module Shared.App where

import RIO
import qualified  RIO.ByteString.Lazy as BL

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

(^*) :: Int -> Int -> Int
(^*) = (^)


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


insertUser :: (AcidWorldUpdateInner m ss, HasSegment ss  "Users") => User -> m ss User
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

instance Eventable "fetchUsers" where
  type EventArgs "fetchUsers" = '[]
  type EventResult "fetchUsers" = [User]
  type EventSegments "fetchUsers" = '["Users"]
  runEvent _ _ = fmap IxSet.toList $ getSegment (Proxy :: Proxy "Users")

instance Eventable "fetchUsersStats" where
  type EventArgs "fetchUsersStats" = '[]
  type EventResult "fetchUsersStats" = Int
  type EventSegments "fetchUsersStats" = '["Users"]
  runEvent _ _ = fmap IxSet.size $ getSegment (Proxy :: Proxy "Users")



generateUserIO :: IO User
generateUserIO = QC.generate arbitrary

generateUsers :: Int -> Gen [User]
generateUsers i = do
  us <- sequence $ replicate i (arbitrary)
  pure $ map (\(u, uid) -> u{userId = uid}) $ zip us [1..]


type AppSegments = '["Users"]
type AppEvents = '["insertUser", "fetchUsers", "fetchUsersStats"]

type AppAW s = AcidWorld AppSegments AppEvents s

type Middleware s = IO (AppAW s) -> IO (AppAW s)


mkTempDir :: IO (FilePath)
mkTempDir = do
  tmpP <- Dir.makeAbsolute topLevelTestDir
  Dir.createDirectoryIfMissing True tmpP
  liftIO $ Temp.createTempDirectory tmpP "test"



{-
-- this isn't really that useful I think
openAppAcidWorldWithDefaultState :: IO AppAW
openAppAcidWorldWithDefaultState = do
  t <- mkTempDir
  us <- generateUsers (10^*5)
  let iset = IxSet.fromList us
  let def = defaultSegmentsState (Proxy :: Proxy AppSegments)
      def' = putSegmentP (Proxy :: Proxy "Users") (Proxy :: Proxy AppSegments) iset def
  aw <- openAcidWorld (Just def') (AWBConfigBackendFS t) AWUConfigStatePure
  pure aw
-}


openAppAcidWorldRestoreState :: (AcidSerialiseT s ~ BL.ByteString, AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents, AcidSerialiseConstraint s AppSegments "fetchUsersStats" ) => AcidSerialiseEventOptions s -> String -> IO (AppAW s)
openAppAcidWorldRestoreState opts s = do
  t <- mkTempDir
  let e = topLevelStoredStateDir <> "/" <> "testState" <> "/" <> s
  copyDirectory e t
  aw <- throwEither $ openAcidWorld Nothing (AWBConfigBackendFS t) AWUConfigStatePure opts
  -- this is to force the internal state
  i <- runFetchUsersStats aw
  putStrLn $ T.unpack . utf8BuilderToText $ "Opened aw with " <> displayShow i
  pure aw


openAppAcidWorldFresh :: (AcidSerialiseT s ~ BL.ByteString, AcidSerialiseEvent s, AcidSerialiseConstraintAll s AppSegments AppEvents) => (AcidSerialiseEventOptions s) -> IO (AppAW s)
openAppAcidWorldFresh opts = do
  t <- mkTempDir
  throwEither $ openAcidWorld Nothing (AWBConfigBackendFS t) AWUConfigStatePure opts

closeAndReopen :: Middleware s
closeAndReopen = reopenAcidWorld . closeAcidWorldMiddleware

closeAcidWorldMiddleware :: Middleware s
closeAcidWorldMiddleware iAw = do
  aw <- iAw
  closeAcidWorld aw
  pure aw

reopenAcidWorld :: Middleware s
reopenAcidWorld iAw = do
  (AcidWorld{..}) <- iAw
  throwEither $ openAcidWorld Nothing (acidWorldBackendConfig) (acidWorldUpdateMonadConfig) acidWorldSerialiserOptions


insertUsers :: AcidSerialiseConstraint s AppSegments "insertUser" => Int -> Middleware s
insertUsers i iAw = do
  aw <- iAw
  us <- QC.generate $ generateUsers i
  mapM_ (runInsertUser aw) us
  pure aw


runInsertUser :: AcidSerialiseConstraint s AppSegments "insertUser" => AppAW s -> User -> IO User
runInsertUser aw u = update aw (mkEvent (Proxy :: Proxy ("insertUser")) u)


runFetchUsersStats :: AcidSerialiseConstraint s  AppSegments "fetchUsersStats" => AppAW s -> IO Int
runFetchUsersStats aw = update aw (mkEvent (Proxy :: Proxy ("fetchUsersStats")) )


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


