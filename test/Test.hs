module Main (main) where

import Shared.App

import RIO
import qualified RIO.Text as T
import Data.Proxy(Proxy(..))
import Acid.World
import Test.Tasty
import Test.Tasty.QuickCheck
import Test.Tasty.HUnit

import qualified Test.QuickCheck as QC
import qualified Test.QuickCheck.Property as QCP

main :: IO ()
main = do
  defaultMain tests

tests :: TestTree
tests = testGroup "Tests" [
      serialiserTests AcidSerialiserJSONOptions,
      serialiserTests AcidSerialiserCBOROptions
   ]

serialiserTests :: forall s. (AcidSerialiseEvent s, AcidSerialiseConstraint s AppSegments "insertUser", AcidSerialiseConstraintAll s AppSegments AppEvents) => AcidSerialiseEventOptions s -> TestTree
serialiserTests o = testGroup "Serialiser" [
    testGroup (T.unpack $ serialiserName (Proxy :: Proxy s)) [
      testProperty "serialiseEventEqualDeserialise" $ prop_serialiseEventEqualDeserialise o,
      testProperty "serialiseWrappedEventEqualDeserialise" $ prop_serialiseWrappedEventEqualDeserialise o,
      testCaseSteps "insertAndFetchState" $ unit_insertAndFetchState o
    ]

  ]

genStorableEvent :: QC.Gen (StorableEvent AppSegments AppEvents "insertUser")
genStorableEvent = do
  t <- QC.arbitrary
  eId <- QC.arbitrary
  u <- QC.arbitrary
  let e = mkEvent (Proxy :: Proxy ("insertUser")) u
  return $ StorableEvent t (EventId eId) e

prop_serialiseEventEqualDeserialise :: forall s. (AcidSerialiseEvent s, AcidSerialiseConstraint s AppSegments "insertUser") => AcidSerialiseEventOptions s -> QC.Property
prop_serialiseEventEqualDeserialise o = forAll genStorableEvent $ \e ->
  let serialised = serialiseEvent o e
      deserialisedE = deserialiseEvent o serialised
  in case deserialisedE of
      Left r -> property $ QCP.failed {QCP.reason = T.unpack $ "Error encountered when deserialising: " <> r}
      Right e' -> e === e'


prop_serialiseWrappedEventEqualDeserialise :: forall s. (AcidSerialiseEvent s, AcidSerialiseConstraint s AppSegments "insertUser", AcidSerialiseConstraintAll s AppSegments AppEvents) => AcidSerialiseEventOptions s -> QC.Property
prop_serialiseWrappedEventEqualDeserialise o = forAll genStorableEvent $ \e ->
  let serialised = serialiseEvent o e
      deserialisedE = deserialiseWrappedEvent o serialised
  in case deserialisedE of
      Left r -> property $ QCP.failed {QCP.reason = T.unpack $ "Error encountered when deserialising: " <> r}
      (Right (e'  :: WrappedEvent AppSegments AppEvents)) -> show (WrappedEvent e) === show e'

genUsers :: QC.Gen [User]
genUsers = do
  replicate i (QC.generate arbitrary)

unit_insertAndFetchState :: forall s. (AcidSerialiseEvent s, AcidSerialiseConstraint s AppSegments "insertUser") => AcidSerialiseEventOptions s -> (String -> IO ()) -> QC.Property
unit_insertAndFetchState step = forAll (generateUsers 100) \us ->
