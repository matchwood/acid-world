module Main (main) where

import Shared.App

import RIO
import Data.Proxy(Proxy(..))
import Acid.World
import Test.Tasty

main :: IO ()
main = do
  defaultMain tests

tests :: TestTree
tests = testGroup "Tests" [
      serialiserTests AcidSerialiserJSONOptions,
      serialiserTests AcidSerialiserCBOROptions
   ]
{-  aw@(AcidWorld{..}) <- openAppAcidWorldCBORFresh
  us <- generateUsers 100000
  mapM_ (\u -> update aw (mkEvent (Proxy :: Proxy ("insertUser")) u)) us
  n <- update aw (mkEvent (Proxy :: Proxy ("fetchUsersStats")))
  traceM $ "Users inserted :: " <> (utf8BuilderToText $ displayShow n)
  closeAcidWorld aw

  aw2 <- openAcidWorld Nothing (acidWorldBackendConfig) (acidWorldUpdateMonadConfig) acidWorldSerialiserOptions

  n2 <- update aw (mkEvent (Proxy :: Proxy ("fetchUsersStats")))
  traceM $ "Users inserted after restore :: " <> (utf8BuilderToText $ displayShow n2)
-}

serialiserTests :: AcidSerialiseEventOptions s -> TestTree
serialiserTests = undefined