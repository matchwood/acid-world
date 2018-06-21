module Main (main) where

import RIO
import Acid.World


main :: IO ()
main = do
  logOptions <- logOptionsHandle stderr True
  let logOptions' = setLogUseTime True logOptions
  withLogFunc logOptions' $ \lf -> do
    runRIO lf $ do
      s <- littleTest
      logInfo $  displayShow s
