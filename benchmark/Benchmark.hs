
module Main (main) where

import RIO

import Criterion.Main
import Criterion.Types

import Shared.App

main :: IO ()
main = do

  let conf = defaultConfig {
        timeLimit = 20
       }
  logOptions <- logOptionsHandle stderr True
  let logOptions' = setLogUseTime True logOptions

  withLogFunc logOptions' $ \lf -> do
    defaultMainWith conf [
       env generateUser $ \u ->(
         env openAppAcidWorldInTempDir $
           (\aw -> bench "insertUser" $  nfIO $ runRIO lf (insertUser aw u)))


      ]




