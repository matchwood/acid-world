module Main (main) where

import Shared.App

import RIO
import Data.Functor.Identity
import qualified RIO.Text as T
import Data.Proxy(Proxy(..))
import Acid.World
import Test.Tasty
import Test.Tasty.QuickCheck
import qualified Test.QuickCheck as QC
import qualified Test.QuickCheck.Property as QCP
import Conduit

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

serialiserTests :: forall s. (AcidSerialiseEvent s, AcidSerialiseConstraint s AppSegments AppEvents "insertUser", AcidDeserialiseConstraint s AppSegments AppEvents) => AcidSerialiseEventOptions s -> TestTree
serialiserTests o = testGroup "Serialiser" [
    testGroup (T.unpack $ serialiserName (Proxy :: Proxy s)) [
      testProperty "serialiseEqualDeserialise" $ prop_serialiseEqualDeserialise o
    ]

  ]

prop_serialiseEqualDeserialise ::forall s. (AcidSerialiseEvent s, AcidSerialiseConstraint s AppSegments AppEvents "insertUser", AcidDeserialiseConstraint s AppSegments AppEvents) => AcidSerialiseEventOptions s -> QC.Property
prop_serialiseEqualDeserialise o = forAll genStorableEvent $ \e ->
  let serialised = acidSerialiseEvent o e
      deserialisedCon = acidDeserialiseEvents o sParsers
      deserialisedList = runIdentity . runConduit $ yieldMany [serialised] .| deserialisedCon .| sinkList
      deserialised = extractFromList deserialisedList

  in case deserialised of
      Left r -> r
      Right w -> storableEventEqualsWrappedEvent e w

  where
    sParsers :: AcidSerialiseParsers s AppSegments AppEvents
    sParsers = acidSerialiseMakeParsers o (Proxy :: Proxy AppSegments) (Proxy :: Proxy AppEvents)
    extractFromList :: [Either Text (WrappedEvent AppSegments AppEvents)] -> Either (QCP.Result) (WrappedEvent AppSegments AppEvents)
    extractFromList [] = Left $ QCP.failed {QCP.reason = "Expected to deserialise one event, got none"}
    extractFromList [Left err] = Left $ QCP.failed {QCP.reason = T.unpack err}
    extractFromList [Right a] = Right a
    extractFromList xs = Left $ QCP.failed {QCP.reason = "Expected to deserialise one event, got " ++ show (length xs)}

genStorableEvent :: QC.Gen (StorableEvent AppSegments AppEvents "insertUser")
genStorableEvent = do
  t <- QC.arbitrary
  eId <- QC.arbitrary
  u <- QC.arbitrary
  let e = mkEvent (Proxy :: Proxy ("insertUser")) u
  return $ StorableEvent t (EventId eId) e

storableEventEqualsWrappedEvent :: StorableEvent AppSegments AppEvents n -> WrappedEvent AppSegments AppEvents -> QCP.Result
storableEventEqualsWrappedEvent (StorableEvent t1 eId1 e1) (WrappedEvent t2) = undefined