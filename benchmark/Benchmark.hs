
module Main (main) where

import RIO

import Criterion.Main
import Criterion.Types

import Shared.App

import Acid.World


main :: IO ()
main = do

  --openAppAcidWorldRestoreState "1mUsers"
  --_ <- insertUsers 1000000 $ openAppAcidWorldFresh AcidSerialiserCBOROptions
  let conf = defaultConfig {
        timeLimit = 20
       }
  -- it seems like we should perRunEnv for the generateUser part of this, but that causes major issues with criterion - something like 3 or 4 order of magnitude slow down in the benchmarked code
  defaultMainWith conf [
    bgroup "JSON" [

       env (openAppAcidWorldFresh AcidSerialiserJSONOptions) $ \aw ->
            bgroup "Empty state" [
               bench "insertUser" $ whnfIO (generateUser >>= runInsertUser aw)
             ],
       env (openAppAcidWorldRestoreState AcidSerialiserJSONOptions "JSON/100kUsers") $ \aw ->
            bgroup "100K restored state" [
               bench "insertUser" $ whnfIO (generateUser >>= runInsertUser aw)
             ],
       env (openAppAcidWorldRestoreState AcidSerialiserJSONOptions "JSON/1mUsers") $ \aw ->
            bgroup "1m restored state" [
               bench "insertUser" $ whnfIO (generateUser >>= runInsertUser aw)
             ]
           ],
    bgroup "CBOR" [

       env (openAppAcidWorldFresh AcidSerialiserCBOROptions) $ \aw ->
            bgroup "Empty state" [
               bench "insertUser" $ whnfIO (generateUser >>= runInsertUser aw)
             ],
       env (openAppAcidWorldRestoreState AcidSerialiserCBOROptions "CBOR/100kUsers") $ \aw ->
            bgroup "100K restored state" [
               bench "insertUser" $ whnfIO (generateUser >>= runInsertUser aw)
             ],
       env (openAppAcidWorldRestoreState AcidSerialiserCBOROptions "CBOR/1mUsers") $ \aw ->
            bgroup "1m restored state" [
               bench "insertUser" $ whnfIO (generateUser >>= runInsertUser aw)
             ]
           ]
      ]