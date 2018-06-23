{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# OPTIONS_GHC -freduction-depth=0 #-}
{-# LANGUAGE TemplateHaskell #-}

module Main (main) where

import RIO
import qualified RIO.Directory as Dir

import Data.Proxy(Proxy(..))
import Acid.World
import Test.QuickCheck.Arbitrary
import Test.QuickCheck as QC
import Criterion.Main
import Criterion.Types
import qualified System.IO.Temp as Temp

-- generateEventables (eventableNames 100)
topLevelTestDir :: FilePath
topLevelTestDir = "./tmp"

instance NFData (AcidWorld a n) where
  rnf _ = ()

main :: IO ()
main = do

  let conf = defaultConfig {
        timeLimit = 20
       }
  logOptions <- logOptionsHandle stderr True
  let logOptions' = setLogUseTime True logOptions

  withLogFunc logOptions' $ \lf -> do
    defaultMainWith conf [
       env generateStrings $ \ls -> bench "insert100k" $ perRunEnv (openMyAcidWorldInTempDir) $ \aw -> runRIO lf (insert100k aw ls)
{-      bench  "fetchList" (perRunEnv (Dir.copyFile (eventPath <> ".orig") eventPath) (const $ runRIO lf fetchList)),
      bench  "fetchListWithManyEventNames" (perRunEnv (Dir.copyFile (eventPath <> ".orig") eventPath) (const $ runRIO lf fetchListWithManyEventNames)),
      bench  "fetchListWithManyEventNamesInverted" (perRunEnv (Dir.copyFile (eventPath <> ".orig") eventPath) (const $ runRIO lf fetchListWithManyEventNamesInverted))-}

      ]





instance Segment "List" where
  type SegmentS "List" = [String]
  defaultState _ = []

prependToList :: (AcidWorldUpdate m ss, HasSegment ss  "List") => String -> m ss [String]
prependToList a = do
  ls <- getSegment (Proxy :: Proxy "List")
  let newLs = a : ls
  putSegment (Proxy :: Proxy "List") newLs
  fmap (take 5) $ getSegment (Proxy :: Proxy "List")


instance Eventable "prependToList" where
  type EventArgs "prependToList" = '[String]
  type EventResult "prependToList" = [String]
  type EventSegments "prependToList" = '["List"]
  runEvent _ = toRunEvent prependToList

instance Eventable "getListState" where
  type EventArgs "getListState" = '[]
  type EventResult "getListState" = [String]
  type EventSegments "getListState" = '["List"]
  runEvent _ _ = fmap (take 10) $ getSegment (Proxy :: Proxy "List")



generateStrings :: IO [String]
generateStrings = sequence $ replicate 100000 (QC.generate arbitrary)


openMyAcidWorldInTempDir :: IO (AcidWorld '["List"] '["prependToList", "getListState"])
openMyAcidWorldInTempDir = do
  tmpP <- Dir.makeAbsolute topLevelTestDir
  Dir.createDirectoryIfMissing True tmpP
  tmpDir <- Temp.createTempDirectory tmpP "test"
  openAcidWorld Nothing (AWBConfigBackendFS tmpDir) AWUConfigStatePure


insert100k :: AcidWorld '["List"] '["prependToList", "getListState"] -> [String] -> RIO env [String]
insert100k aw ls = do
  mapM_ (\l -> update aw (mkEvent (Proxy :: Proxy ("prependToList")) l)) ls
  update aw (mkEvent (Proxy :: Proxy ("prependToList")) "Last in")


{-fetchList :: (HasLogFunc env) => RIO env [String]
fetchList = do
  aw <- openMyAcidWorld
  res <- update aw (mkEvent (Proxy :: Proxy ("getListState")))
  return res-}


