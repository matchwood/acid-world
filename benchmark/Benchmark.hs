
module Main (main) where

import RIO
import qualified RIO.Text as T
import Data.Proxy (Proxy(..))
import Criterion.Main

import Shared.App

import Acid.World



main :: IO ()
main = do

  --openAppAcidWorldRestoreState "1mUsers"
  --_ <- insertUsers 1000000 $ openAppAcidWorldFreshFS AcidSerialiserSafeCopyOptions

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
       env (openAppAcidWorldRestoreState o (sName <> "/100kUsers")) $ \aw ->
            bgroup "100K restored state" [
               bench "insertUser" $ whnfIO (generateUserIO >>= runInsertUser aw)
             ],
       env (openAppAcidWorldRestoreState o (sName <> "/1mUsers")) $ \aw ->
            bgroup "1m restored state" [
               bench "insertUser" $ whnfIO (generateUserIO >>= runInsertUser aw)
             ]
    ]