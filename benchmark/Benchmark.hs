
module Main (main) where

import RIO

import Criterion.Main
import Criterion.Types

import Shared.App




main :: IO ()
main = do

 -- openAppAcidWorldRestoreState "100kUsers"

  let conf = defaultConfig {
        timeLimit = 20
       }
  -- it seems like we should perRunEnv for the generateUser part of this, but that causes major issues with criterion - something like 3 or 4 order of magnitude slow down in the benchmarked code
  defaultMainWith conf [
    env (openAppAcidWorldFresh) $ \aw ->
          bgroup "Empty state" [
             bench "insertUser" $ whnfIO (generateUser >>= runInsertUser aw)
           ],
     env (openAppAcidWorldRestoreState "100kUsers") $ \aw ->
          bgroup "100K restored state" [
             bench "insertUser" $ whnfIO (generateUser >>= runInsertUser aw)
           ],
     env (openAppAcidWorldRestoreState "1mUsers") $ \aw ->
          bgroup "1m restored state" [
             bench "insertUser" $ whnfIO (generateUser >>= runInsertUser aw)
           ]
      ]