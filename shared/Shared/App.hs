{-# OPTIONS_GHC -fno-warn-orphans #-}

module Shared.App where

import RIO
import qualified RIO.Time as T
import qualified RIO.Directory as Dir

import Data.Proxy(Proxy(..))
import Test.QuickCheck.Arbitrary
import Test.QuickCheck as QC
import qualified System.IO.Temp as Temp
import qualified Generics.SOP as SOP
import qualified Generics.SOP.Arbitrary as SOP
import qualified Data.Aeson as Aeson
import Acid.World

topLevelTestDir :: FilePath
topLevelTestDir = "./tmp"



instance NFData (AcidWorld a n) where
  rnf _ = ()



data User = User  {
  userFirstName :: Text,
  userLastName :: Text,
  userCreated :: T.UTCTime,
  userDisabled :: Bool
} deriving (Eq, Show, Generic)

instance Aeson.ToJSON User
instance Aeson.FromJSON User
instance SOP.Generic User
instance Arbitrary User where arbitrary = SOP.garbitrary
instance NFData User



instance Segment "Users" where
  type SegmentS "Users" = [User]
  defaultState _ = []


prependToList :: (AcidWorldUpdate m ss, HasSegment ss  "Users") => User -> m ss User
prependToList a = do
  ls <- getSegment (Proxy :: Proxy "Users")
  let newLs = a : ls
  putSegment (Proxy :: Proxy "Users") newLs
  return a


instance Eventable "insertUser" where
  type EventArgs "insertUser" = '[User]
  type EventResult "insertUser" = User
  type EventSegments "insertUser" = '["Users"]
  runEvent _ = toRunEvent prependToList

instance Eventable "fetchUsers" where
  type EventArgs "fetchUsers" = '[]
  type EventResult "fetchUsers" = [User]
  type EventSegments "fetchUsers" = '["Users"]
  runEvent _ _ = getSegment (Proxy :: Proxy "Users")



generateUser :: IO User
generateUser = QC.generate arbitrary


generateUsers :: Int -> IO [User]
generateUsers i = sequence $ replicate i (QC.generate arbitrary)



type AppSegments = '["Users"]
type AppEvents = '["insertUser", "fetchUsers"]

type AppAW = AcidWorld AppSegments AppEvents

openAppAcidWorldInTempDir :: IO AppAW
openAppAcidWorldInTempDir = do
  tmpP <- Dir.makeAbsolute topLevelTestDir
  Dir.createDirectoryIfMissing True tmpP
  tmpDir <- Temp.createTempDirectory tmpP "test"
  openAcidWorld Nothing (AWBConfigBackendFS tmpDir) AWUConfigStatePure


insertUser :: AppAW -> User -> RIO env User
insertUser aw u = update aw (mkEvent (Proxy :: Proxy ("insertUser")) u)



