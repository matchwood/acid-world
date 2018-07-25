module Main (main) where

import Shared.App

import RIO
import qualified RIO.Text as T
import qualified RIO.List as L
import qualified RIO.Directory as Dir

import Data.Proxy(Proxy(..))
import Acid.World
import Test.Tasty
import Test.Tasty.QuickCheck
import Test.Tasty.HUnit

import qualified Data.IxSet.Typed as IxSet

import qualified Test.QuickCheck as QC
import qualified Test.QuickCheck.Property as QCP




withBackends :: (AppValidBackend -> AppValidSerialiser -> [TestTree]) -> [(AppValidBackend, [AppValidSerialiser])] -> [TestTree]
withBackends f os =
  (flip map) os $ \(b@(AppValidBackend (bc :: (FilePath -> AWBConfig b))), ss) ->
     testGroup ("Backend: " <> (T.unpack . backendName $ (Proxy :: Proxy b)) <> ", " <> T.unpack (backendConfigInfo (bc "*TMPDIR*"))) $
       (flip map) ss $ \(o@(AppValidSerialiser (_ :: AcidSerialiseEventOptions s))) ->
         testGroup ("Serialiser: " <> (T.unpack . serialiserName $ (Proxy :: Proxy s))) $
           f b o

backendsWithSerialisers :: [AppValidBackend] -> [AppValidSerialiser] -> [(AppValidBackend, [AppValidSerialiser])]
backendsWithSerialisers bs ser = map (\b -> (b, ser)) bs


backendsWithAllSerialisers :: [AppValidBackend] -> [(AppValidBackend, [AppValidSerialiser])]
backendsWithAllSerialisers bs = backendsWithSerialisers bs allSerialisers


main :: IO ()
main = do
  --testCheckSum
  defaultMain tests

tests :: TestTree
tests = testGroup "Tests" $
  map serialiserTests allSerialisers ++
  withBackends ephemeralBackendSerialiserTests (backendsWithAllSerialisers allBackends) ++
  withBackends persistentBackendSerialiserTests (backendsWithAllSerialisers persistentBackendsWithGzip) ++
  withBackends persistentBackendConstraintTests (backendsWithSerialisers persistentBackends [defaultAppSerialiser]) ++

  fsSpecificTests (\t -> AWBConfigFS t True) defaultAppSerialiser



serialiserTests ::  AppValidSerialiser-> TestTree
serialiserTests (AppValidSerialiser (o :: AcidSerialiseEventOptions s)) =
  testGroup ("Serialiser: " <> (T.unpack . serialiserName $ (Proxy :: Proxy s))) [
      testProperty "serialiseEventEqualDeserialise" $ prop_serialiseEventEqualDeserialise o
    , testProperty "serialiseWrappedEventEqualDeserialise" $ prop_serialiseWrappedEventEqualDeserialise o
    ]

ephemeralBackendSerialiserTests :: AppValidBackend -> AppValidSerialiser -> [TestTree]
ephemeralBackendSerialiserTests (AppValidBackend (bConf :: FilePath -> (AWBConfig b))) (AppValidSerialiser (o :: AcidSerialiseEventOptions s)) = [
    testCaseSteps "insertAndFetchState" $ unit_insertAndFetchState bConf o
  ]


persistentBackendSerialiserTests :: AppValidBackend -> AppValidSerialiser -> [TestTree]
persistentBackendSerialiserTests (AppValidBackend (bConf :: FilePath -> (AWBConfig b))) (AppValidSerialiser (o :: AcidSerialiseEventOptions s)) = [
    testCaseSteps "insertAndRestoreState" $ unit_insertAndRestoreState bConf o,
    testCaseSteps "checkpointAndRestoreState" $ unit_checkpointAndRestoreState bConf o,
    testCaseSteps "compositionOfEventsState" $ unit_compositionOfEventsState bConf o

  ]


persistentBackendConstraintTests :: AppValidBackend -> AppValidSerialiser -> [TestTree]
persistentBackendConstraintTests (AppValidBackend (bConf :: FilePath -> (AWBConfig b))) (AppValidSerialiser (o :: AcidSerialiseEventOptions s)) = [
    testCaseSteps "validAppConstraintsOnRunEvent" $ unit_validAppConstraintsOnRunEvent bConf o
  , testCaseSteps "validAppConstraintsOnRestore" $ unit_validAppConstraintsOnRestore bConf o

  ]
fsSpecificTests :: (FilePath -> (AWBConfig AcidWorldBackendFS)) -> AppValidSerialiser -> [TestTree]
fsSpecificTests bConf (AppValidSerialiser (o :: AcidSerialiseEventOptions s)) = [
    testCaseSteps "defaultSegmentUsedOnRestore" $ unit_defaultSegmentUsedOnRestore bConf o

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
  let serialised = serialiseStorableEvent o e
      deserialisedE = deserialiseStorableEvent o serialised
  in case deserialisedE of
      Left r -> property $ QCP.failed {QCP.reason = T.unpack $ "Error encountered when deserialising: " <> r <> showT ((serialised))}
      Right e' -> e === e'


prop_serialiseWrappedEventEqualDeserialise :: forall s. AppValidSerialiserConstraint s  => AcidSerialiseEventOptions s -> QC.Property
prop_serialiseWrappedEventEqualDeserialise o = forAll genStorableEvent $ \e ->
  let serialised = serialiseStorableEvent o e
      deserialisedE = deserialiseWrappedEvent o serialised
  in case deserialisedE of
      Left r -> property $ QCP.failed{QCP.reason = T.unpack $ "Error encountered when deserialising: " <> r}
      (Right (e'  :: WrappedEvent AppSegments AppEvents)) -> show (WrappedEvent e) === show e'



unit_insertAndFetchState :: forall b s. (AppValidBackendConstraint b, AppValidSerialiserConstraint s)  => (FilePath -> AWBConfig b) -> AcidSerialiseEventOptions s -> (String -> IO ()) -> Assertion
unit_insertAndFetchState b o step = do
  us <- QC.generate $ generateUsers 100
  step "Opening acid world"
  aw <- openAppAcidWorldFresh b o
  step "Inserting users"
  mapM_ (runInsertUser aw) us
  step "Fetching users"
  us2 <- query aw fetchUsers
  assertBool "Fetched user list did not match inserted users" (us == us2)


unit_insertAndRestoreState :: forall b s. (AppValidBackendConstraint b, AppValidSerialiserConstraint s)  => (FilePath -> AWBConfig b) -> AcidSerialiseEventOptions s -> (String -> IO ()) -> Assertion
unit_insertAndRestoreState b o step = do
  allUs <- QC.generate $ generateUsers 2000
  let (us, us2) = L.splitAt 1000 allUs
  step "Opening acid world"
  aw <- openAppAcidWorldFresh b o
  step "Inserting users"
  mapM_ (runInsertUser aw) us
  step "Closing acid world"
  closeAcidWorld aw
  step "Reopening acid world"
  aw2 <- reopenAcidWorldMiddleware (pure aw)
  step "Inserting second set of users"
  mapM_ (runInsertUser aw2) us2
  step "Closing acid world"
  closeAcidWorld aw2
  step "Reopening acid world"
  aw3 <- reopenAcidWorldMiddleware (pure aw2)
  step "Fetching users"
  us3 <- query aw3 fetchUsers
  step $ "Fetched users: " ++ (show . length $ us3)

  assertBool "Fetched user list did not match inserted users" (L.sort (us ++ us2) == L.sort us3)

unit_checkpointAndRestoreState :: forall b s. (AppValidBackendConstraint b, AppValidSerialiserConstraint s)  => (FilePath -> AWBConfig b) -> AcidSerialiseEventOptions s -> (String -> IO ()) -> Assertion
unit_checkpointAndRestoreState b o step = do
  us <- QC.generate $ generateUsers 1000
  as <- QC.generate $ generateAddresses 1000
  ps <- QC.generate $ generatePhonenumbers 1000
  step "Opening acid world"
  aw <- openAppAcidWorldFresh b o
  step "Inserting users"
  mapM_ (runInsertUser aw) us
  step "Inserting addresses"
  mapM_ (runInsertAddress aw) as
  step "Inserting phonenumbers"
  mapM_ (runInsertPhonenumber aw) ps
  step "Creating checkpoint"
  createCheckpoint aw
  step "Closing acid world"
  closeAcidWorld aw
  step "Reopening acid world"
  aw2 <- reopenAcidWorldMiddleware (pure aw)
  step "Fetching users"
  us2 <- query aw2 fetchUsers
  assertBool "Fetched user list did not match inserted users" (L.sort us == L.sort us2)


unit_compositionOfEventsState :: forall b s. (AppValidBackendConstraint b, AppValidSerialiserConstraint s)  => (FilePath -> AWBConfig b) -> AcidSerialiseEventOptions s -> (String -> IO ()) -> Assertion
unit_compositionOfEventsState b o step = do
  us <- QC.generate $ generateUsers 1000
  step "Opening acid world"
  aw <- openAppAcidWorldFresh b o
  step "Inserting users with phonenumbers"
  mapM_ (runInsertUserC aw userToPhoneNumber) us
  usF <- query aw fetchUsers
  ps <- query aw fetchPhonenumbers
  assertBool "Expected equal number of users and pns" (length usF == length ps)
  assertBool "Expected equal ids for every pair of user/numbers" (and $ map (\(u, p) -> userId u == phonenumberId p ) (zip usF ps))
  where
    userToPhoneNumber :: User -> Event "insertPhonenumber"
    userToPhoneNumber u = (mkEvent (Proxy :: Proxy ("insertPhonenumber")) $ Phonenumber (userId u) "asdf" 24 False)



unit_validAppConstraintsOnRunEvent :: forall b s. (AppValidBackendConstraint b, AppValidSerialiserConstraint s)  => (FilePath -> AWBConfig b) -> AcidSerialiseEventOptions s -> (String -> IO ()) -> Assertion
unit_validAppConstraintsOnRunEvent b o step = do
  step "Opening acid world"
  aw <- openAppAcidWorldFreshWithInvariants b o unitInvariants
  step "Inserting users"
  us <- QC.generate $ generateUsers 1000
  mapM_ (runInsertUser aw) us

  step "Inserting invalid user"
  u <- QC.generate arbitrary

  res <- update aw (mkEvent (Proxy :: Proxy ("insertUser")) u{userId = 1500, userDisabled = False})
  us2 <- query aw fetchUsers
  assertEqual "Expected update to fail" (Left (AWExceptionInvariantsViolated [("Users", userInvariantFailureMessage)])) res
  assertBool "Fetched user list did not match inserted users" (L.sort us == L.sort us2)

  where
    unitInvariants :: Invariants AppSegments
    unitInvariants = putInvariantP (Just userInvariant) emptyInvariants
    userInvariant :: Invariant AppSegments "Users"
    userInvariant = Invariant $ \ixset ->
      if (IxSet.size . IxSet.getEQ False . IxSet.getGT (1400 :: Int) $ ixset) > 0
        then pure userInvariantFailureMessage
        else Nothing
    userInvariantFailureMessage :: Text
    userInvariantFailureMessage = "All users with an id > 1400 must be enabled"

unit_validAppConstraintsOnRestore :: forall b s. (AppValidBackendConstraint b, AppValidSerialiserConstraint s)  => (FilePath -> AWBConfig b) -> AcidSerialiseEventOptions s -> (String -> IO ()) -> Assertion
unit_validAppConstraintsOnRestore b o step = do
  step "Opening acid world"
  aw <- openAppAcidWorldFresh b o
  step "Inserting users"
  us <- QC.generate $ generateUsers 1000
  mapM_ (runInsertUser aw) us
  step "Creating checkpoint"
  createCheckpoint aw
  step "Closing acid world"
  closeAcidWorld aw

  step "Reopening acid world with new invariant"
  res <- reopenAcidWorld aw{acidWorldInvariants = unitInvariants}
  case res of
    Left err -> assertEqual "Expected open from checkpoint to fail" ((AWExceptionInvariantsViolated [("Users", userInvariantFailureMessage)])) err
    Right _ -> assertFailure "Expected to get an error when opening acid world, but opened successfully"

  where
    unitInvariants :: Invariants AppSegments
    unitInvariants = putInvariantP (Just userInvariant) emptyInvariants
    userInvariant :: Invariant AppSegments "Users"
    userInvariant = Invariant $ \ixset ->
      if (IxSet.size ixset) > 800
        then pure userInvariantFailureMessage
        else Nothing
    userInvariantFailureMessage :: Text
    userInvariantFailureMessage = "Only 800 users are allowed in this segment"


unit_defaultSegmentUsedOnRestore :: forall s. (AppValidBackendConstraint AcidWorldBackendFS, AppValidSerialiserConstraint s)  => (FilePath -> AWBConfig AcidWorldBackendFS) -> AcidSerialiseEventOptions s -> (String -> IO ()) -> Assertion
unit_defaultSegmentUsedOnRestore b o step = do

  td <- mkTempDir
  let conf = b td

  us <- QC.generate $ generateUsers 1000
  allPs <- QC.generate $ generatePhonenumbers 1000
  let (ps, ps2) = (take 500 allPs, drop 500 allPs)

  step "Opening acid world"
  aw <- throwEither $ openAcidWorld (unitDefaultState ps) emptyInvariants conf AWConfigPureState o

  step "Inserting records"
  mapM_ (runInsertUser aw) us
  mapM_ (runInsertPhonenumber aw) ps2
  ps3 <- query aw fetchPhonenumbers
  assertBool "Fetched phonenumber list did not match default ++ inserted phonenumbers" (L.sort allPs == L.sort ps3)

  step "Creating checkpoint"
  createCheckpoint aw
  step "Closing acid world"
  closeAcidWorld aw
  --delete pn checkpoint
  let pp  = (Proxy :: Proxy "Phonenumbers")
      cpFolder = currentCheckpointFolder conf
      segPath = makeSegmentPath cpFolder conf pp o
      segCheckPatch =  makeSegmentCheckPath cpFolder conf pp o
      segPathTemp = segPath <> ".temp"

  step "Moving segment and reopening"

  Dir.renameFile segPath segPathTemp
  res <- reopenAcidWorld aw
  assertErrorPrefix (AWExceptionSegmentDeserialisationError $ (prettySegment pp) <> "Segment file missing at") res

  step "Replacing segment, removing segment check and reopening"

  Dir.renameFile segPathTemp segPath
  Dir.renameFile segCheckPatch (segCheckPatch <> ".deleted")
  res2 <- reopenAcidWorld aw
  assertErrorPrefix (AWExceptionSegmentDeserialisationError $ (prettySegment pp) <> "Segment check file could not be found at") res2

  Dir.renameFile segPath (segPath <> ".deleted")

  step "Removing segment and check and reopening"


  aw2 <- throwEither $ reopenAcidWorld aw
  ps4 <- query aw2 fetchPhonenumbers
  us2 <- query aw2 fetchUsers
  assertBool "Fetched phonenumber list did not match default phonenumber state" (L.sort ps == L.sort ps4)
  assertBool "Fetched user list did not match inserted users" (L.sort us == L.sort us2)

  where
    unitDefaultState :: [Phonenumber] -> SegmentsState AppSegments
    unitDefaultState ps =
      putSegmentP (Proxy :: Proxy "Phonenumbers") (IxSet.fromList ps) defaultSegmentsState


assertErrorPrefix :: AWException -> Either AWException a -> Assertion
assertErrorPrefix e (Right _) = assertFailure $ "Expected an exception like " <> show e <> " but got a success"
assertErrorPrefix (AWExceptionInvariantsViolated ts) (Left (AWExceptionInvariantsViolated ts2)) = assertEqualPrefix (showT ts) (showT ts2)
assertErrorPrefix (AWExceptionEventSerialisationError ts) (Left (AWExceptionEventSerialisationError ts2)) = assertEqualPrefix ts ts2
assertErrorPrefix (AWExceptionEventDeserialisationError ts) (Left (AWExceptionEventDeserialisationError ts2)) = assertEqualPrefix ts ts2
assertErrorPrefix (AWExceptionSegmentDeserialisationError ts) (Left (AWExceptionSegmentDeserialisationError ts2)) = assertEqualPrefix ts ts2
assertErrorPrefix e (Left e2) = assertEqual "Expected matching exception constructors" e e2

assertEqualPrefix :: Text -> Text -> Assertion
assertEqualPrefix t t' = assertEqual "Expected matching error message prefixes " t (T.take (T.length t) t')