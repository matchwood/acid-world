
module Main (main) where

import RIO
import qualified RIO.Text as T
import Data.Proxy (Proxy(..))
import Criterion.Main
import qualified RIO.Directory as Dir

import Shared.App

import Acid.World
import qualified Test.QuickCheck as QC



main :: IO ()
main = do

  let conf = defaultConfig
  -- it seems like we should perRunEnv for the generateUser part of this, but that causes major issues with criterion - something like 3 or 4 order of magnitude slow down in the benchmarked code
  defaultMainWith conf $
    map serialiserBenchmarks allSerialisers


serialiserBenchmarks :: AppValidSerialiser -> Benchmark
serialiserBenchmarks (AppValidSerialiser (o :: AcidSerialiseEventOptions s)) =
  let sName = T.unpack . serialiserName $ (Proxy :: Proxy s)
  in bgroup sName [
       env (openAppAcidWorldFreshFS o True) $ \aw ->
            bgroup "Empty state" [
               bench "insertUser" $ whnfIO (generateUserIO >>= runInsertUser aw)
             ],
       env (openAppAcidWorldRestoreState o (sName <> hundredK)) $ \aw ->
            bgroup "100K restored state" [
               bench "insertUser" $ whnfIO (generateUserIO >>= runInsertUser aw)
             ],
       env (openAppAcidWorldRestoreState o (sName <> oneM)) $ \aw ->
            bgroup "1m restored state" [
               bench "insertUser" $ whnfIO (generateUserIO >>= runInsertUser aw)
             ]
    ]

makeTestState :: AppValidSerialiser -> IO ()
makeTestState (AppValidSerialiser (o :: AcidSerialiseEventOptions s)) = do
  let sName = T.unpack . serialiserName $ (Proxy :: Proxy s)
  tmpP <- Dir.makeAbsolute topLevelTestDir
  let newTestDir = tmpP <> "/newTestState"
  Dir.createDirectoryIfMissing True newTestDir

  mapM_ (createForNums $ newTestDir <> "/" <> sName) [
      (100000, hundredK)
    , (1000000, oneM)
    ]
  where
    createForNums :: FilePath -> (Int, String) -> IO ()
    createForNums newTDir (n, name) = do
      aw <- insertUsers n $ openAppAcidWorldFresh (const $ AWBConfigFS (newTDir <> name) True) o
      as <- QC.generate $ generateAddresses (n `divInt` 10)
      ps <- QC.generate $ generatePhonenumbers (n `divInt` 10)
      mapM_ (runInsertAddress aw) as
      mapM_ (runInsertPhonenumber aw) ps
      createCheckpoint aw
      closeAcidWorld aw

hundredK, oneM :: String
hundredK = "/100kUsers"
oneM = "/1mUsers"

divInt :: Int -> Int -> Int
divInt a b = ceiling $  ((/) `on` (fromIntegral :: Int -> Double)) a b