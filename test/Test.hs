module Main (main) where

import Shared.App

import RIO
import qualified RIO.Text as T
import qualified  RIO.ByteString.Lazy as BL

import Data.Proxy(Proxy(..))
import Acid.World
import Test.Tasty
import Test.Tasty.QuickCheck
import Test.Tasty.HUnit

import qualified Test.QuickCheck as QC
import qualified Test.QuickCheck.Property as QCP



type AppValidSerialiserConstraint s = (
  AcidSerialiseEvent s,
  AcidSerialiseConstraintAll s AppSegments AppEvents,
  AcidSerialiseT s ~ BL.ByteString,
  AcidSerialiseConstraint s AppSegments "insertUser"
  )

type AppValidBackendConstraint b = (
  AcidWorldBackend b,
  AWBSerialiseT b ~ BL.ByteString
  )


data AppValidSerialiser where
  AppValidSerialiser :: (AppValidSerialiserConstraint s) => AcidSerialiseEventOptions s -> AppValidSerialiser

allSerialisers :: [AppValidSerialiser]
allSerialisers = [
    AppValidSerialiser AcidSerialiserJSONOptions
  , AppValidSerialiser AcidSerialiserCBOROptions
  , AppValidSerialiser AcidSerialiserSafeCopyOptions
  ]

data AppValidBackend where
  AppValidBackend :: (AppValidBackendConstraint b) => IO (AWBConfig b) -> AppValidBackend

persistentBackends :: [AppValidBackend]
persistentBackends = [AppValidBackend $ fmap AWBConfigFS mkTempDir]

ephemeralBackends :: [AppValidBackend]
ephemeralBackends = [AppValidBackend $ pure AWBConfigMemory]

allBackends :: [AppValidBackend]
allBackends = persistentBackends ++ ephemeralBackends

withBackends :: (AppValidBackend -> AppValidSerialiser -> [TestTree]) -> [(AppValidBackend, [AppValidSerialiser])] -> [TestTree]
withBackends f os =
  (flip map) os $ \(b@(AppValidBackend (_ :: IO (AWBConfig b))), ss) ->
     testGroup ("Backend: " <> (T.unpack . backendName $ (Proxy :: Proxy b))) $
       (flip map) ss $ \(o@(AppValidSerialiser (_ :: AcidSerialiseEventOptions s))) ->
         testGroup ("Serialiser: " <> (T.unpack . serialiserName $ (Proxy :: Proxy s))) $
           f b o

backendsWithSerialisers :: [AppValidBackend] -> [(AppValidBackend, [AppValidSerialiser])]
backendsWithSerialisers = map (\b -> (b, allSerialisers))


main :: IO ()
main = do
  defaultMain tests

tests :: TestTree
tests = testGroup "Tests" $
  map serialiserTests allSerialisers ++
  withBackends ephemeralBackendTests (backendsWithSerialisers allBackends) ++
  withBackends persistentBackendTests (backendsWithSerialisers persistentBackends)

serialiserTests ::  AppValidSerialiser-> TestTree
serialiserTests (AppValidSerialiser (o :: AcidSerialiseEventOptions s)) =
  testGroup ("Serialiser: " <> (T.unpack . serialiserName $ (Proxy :: Proxy s))) [
      testProperty "serialiseEventEqualDeserialise" $ prop_serialiseEventEqualDeserialise o
    , testProperty "serialiseWrappedEventEqualDeserialise" $ prop_serialiseWrappedEventEqualDeserialise o
    ]

ephemeralBackendTests :: AppValidBackend -> AppValidSerialiser -> [TestTree]
ephemeralBackendTests (AppValidBackend (bConf :: IO (AWBConfig b))) (AppValidSerialiser (o :: AcidSerialiseEventOptions s)) = [
    testCaseSteps "insertAndFetchState" $ unit_insertAndFetchState bConf o
  ]


persistentBackendTests :: AppValidBackend -> AppValidSerialiser -> [TestTree]
persistentBackendTests (AppValidBackend (bConf :: IO (AWBConfig b))) (AppValidSerialiser (o :: AcidSerialiseEventOptions s)) = [
    testCaseSteps "insertAndRestoreState" $ unit_insertAndRestoreState bConf o
  ]


genStorableEvent :: QC.Gen (StorableEvent AppSegments AppEvents "insertUser")
genStorableEvent = do
  t <- QC.arbitrary
  eId <- QC.arbitrary
  u <- QC.arbitrary
  let e = mkEvent (Proxy :: Proxy ("insertUser")) u
  return $ StorableEvent t (EventId eId) e


prop_serialiseEventEqualDeserialise :: forall s. AppValidSerialiserConstraint s => AcidSerialiseEventOptions s -> QC.Property
prop_serialiseEventEqualDeserialise o = forAll genStorableEvent $ \e ->
  let serialised = serialiseEvent o e
      deserialisedE = deserialiseEvent o serialised
  in case deserialisedE of
      Left r -> property $ QCP.failed {QCP.reason = T.unpack $ "Error encountered when deserialising: " <> r}
      Right e' -> e === e'


prop_serialiseWrappedEventEqualDeserialise :: forall s. AppValidSerialiserConstraint s  => AcidSerialiseEventOptions s -> QC.Property
prop_serialiseWrappedEventEqualDeserialise o = forAll genStorableEvent $ \e ->
  let serialised = serialiseEvent o e
      deserialisedE = deserialiseWrappedEvent o serialised
  in case deserialisedE of
      Left r -> property $ QCP.failed{QCP.reason = T.unpack $ "Error encountered when deserialising: " <> r}
      (Right (e'  :: WrappedEvent AppSegments AppEvents)) -> show (WrappedEvent e) === show e'



unit_insertAndFetchState :: forall b s. (AppValidBackendConstraint b, AppValidSerialiserConstraint s)  => IO (AWBConfig b) -> AcidSerialiseEventOptions s -> (String -> IO ()) -> Assertion
unit_insertAndFetchState b o step = do
  us <- QC.generate $ generateUsers 100
  step "Opening acid world"
  aw <- openAppAcidWorldFresh b o
  step "Inserting users"
  mapM_ (runInsertUser aw) us
  step "Fetching users"
  us2 <- query aw fetchUsers
  assertBool "Fetched user list did not match inserted users" (us == us2)


unit_insertAndRestoreState :: forall b s. (AppValidBackendConstraint b, AppValidSerialiserConstraint s)  => IO (AWBConfig b) -> AcidSerialiseEventOptions s -> (String -> IO ()) -> Assertion
unit_insertAndRestoreState b o step = do
  us <- QC.generate $ generateUsers 1000
  step "Opening acid world"
  aw <- openAppAcidWorldFresh b o
  step "Inserting users"
  mapM_ (runInsertUser aw) us
  step "Closing acid world"
  closeAcidWorld aw
  step "Reopening acid world"
  aw2 <- reopenAcidWorldMiddleware (pure aw)
  step "Fetching users"
  us2 <- query aw2 fetchUsers
  assertBool "Fetched user list did not match inserted users" (us == us2)