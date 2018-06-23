{-# LANGUAGE TemplateHaskell #-}
module TH where
import RIO
import Language.Haskell.TH
import Acid.World


eventableNames :: Int -> [String]
eventableNames i = map (\s -> "events" ++ show s) [0..i]

generateEventables :: [String] -> Q [Dec]
generateEventables ss = do
  res <- fmap concat $ sequence $ map (eventableDef ) ss

  let ts = foldl' (\t s -> appT (appT promotedConsT (litT (strTyLit s))) t) promotedNilT ss

  res2 <- [d|
      type GeneratedEventNames = $(ts)

    |]

  return $ res ++ res2




eventableDef :: String -> Q [Dec]
eventableDef s = do
  let n = litT (strTyLit s)
  res <-
    [d|
    instance Eventable $(n) where
      type EventArgs $(n) = '[]
      type EventResult $(n) = ()
      type EventSegments $(n) = '[]
      runEvent _ _ = pure ()
    |]
  pure res

