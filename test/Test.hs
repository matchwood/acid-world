module Main (main) where

import Shared.App

import RIO
import Data.Proxy(Proxy(..))
import Acid.World


main :: IO ()
main = do
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
  pure ()
