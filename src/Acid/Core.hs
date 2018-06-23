{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans#-}

module Acid.Core where
import RIO
import qualified RIO.Directory as Dir
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T
import qualified  RIO.ByteString.Lazy as BL
--import qualified  RIO.Vector as V
import qualified  RIO.Time as Time

import Control.Arrow (left)
import Generics.SOP
import Generics.SOP.NP
import GHC.TypeLits
import qualified Control.Monad.State.Strict as St

import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.TypeLevel as V
import qualified  Data.Vinyl.Curry as V
import qualified  Data.Vinyl.Functor as V


import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import Data.Aeson(ToJSON(..), Value(..), Object)

import qualified Control.Concurrent.STM.TVar as  TVar
import qualified Control.Concurrent.STM  as STM
import qualified Data.UUID  as UUID
import qualified Data.UUID.V4  as UUID

import Acid.Core.Segment
import Acid.Core.Utils


data AcidWorldException =
  AcidWorldInitialisationE Text
  deriving (Eq, Show, Typeable)
instance Exception AcidWorldException

data AcidWorld  ss nn where
  AcidWorld :: (
                 AcidWorldBackend bMonad ss nn
               , AcidWorldUpdate uMonad ss
               ) => {
    acidWorldBackendMonad :: Proxy bMonad,
    acidWorldUpdateMonad :: Proxy uMonad,
    acidWorldBackendInitState :: AWBState bMonad ss nn,
    acidWorldUpdateMonadInitState :: AWUState uMonad ss
    } -> AcidWorld ss nn


openAcidWorld :: forall m ss nn bMonad uMonad.
               ( MonadIO m

               , AcidWorldBackend bMonad ss nn
               , AcidWorldUpdate uMonad ss
               ) => Maybe (SegmentsState ss) -> Proxy bMonad -> Proxy uMonad ->  m (AcidWorld ss nn)
openAcidWorld mDefSt acidWorldBackendMonad acidWorldUpdateMonad = do
  let defState = fromMaybe (defaultSegmentsState (Proxy :: Proxy ss)) mDefSt
  (acidWorldBackendInitState, acidWorldUpdateMonadInitState) <- initialise acidWorldUpdateMonad defState
  return $ AcidWorld{..}


update :: (IsValidEvent ss nn n, MonadIO m) => AcidWorld ss nn -> Event n -> m (EventResult n)
update (AcidWorld {..}) = handleUpdateEvent acidWorldBackendInitState acidWorldUpdateMonadInitState

{-
AcidWorldBackend
-}
class ( Monad (m ss nn)
      , MonadIO (m ss nn)
      , MonadThrow (m ss nn)
      , ValidSegmentNames ss
      , ValidEventNames ss nn
      ) =>
  AcidWorldBackend (m :: [Symbol] -> [Symbol] -> * -> *) (ss :: [Symbol]) (nn :: [Symbol]) where
  data AWBState m ss nn
  initialise :: (MonadIO z, AcidWorldUpdate uMonad ss) => (Proxy uMonad) -> (SegmentsState ss) -> z (AWBState m ss nn, AWUState uMonad ss)
  -- should return the most recent checkpoint state, if any
  getLastCheckpointState :: m ss nn (Maybe (SegmentsState ss))
  getLastCheckpointState = pure Nothing
  -- return events since the last checkpoint, if any
  loadEvents :: m ss nn (Either Text [WrappedEvent ss nn])
  loadEvents = pure . pure $ []
  persistEvent :: (MonadIO z, IsValidEvent ss nn n) => (AWBState m ss nn) -> Event n -> z ()
  handleUpdateEvent :: (IsValidEvent ss nn n, MonadIO z, AcidWorldUpdate u ss) => (AWBState m ss nn) -> (AWUState u ss) -> Event n -> z (EventResult n)


{-  handleUpdateEvent e = do
    s <- getState
    saveEvent e
    (a, _) <- runUpdateEvent (Proxy :: Proxy (AcidWorldUpdateStatePure ss)) e s
    return a-}

newtype AcidWorldBackendFS ss nn a = AcidWorldBackendFS (IO a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadThrow)


instance ( ValidSegmentNames ss
         , ValidEventNames ss nn ) =>
  AcidWorldBackend AcidWorldBackendFS ss nn where
  data AWBState AcidWorldBackendFS ss nn = AWBStateBackendFS
  initialise (Proxy :: Proxy uMonad) defState = do
    let (AcidWorldBackendFS m :: AcidWorldBackendFS ss nn (AWUState uMonad ss)) = initialiseUpdate defState
    uState <- liftIO m
    pure (AWBStateBackendFS, uState)
  loadEvents = do
    let ps = makeParsers
    bl <- do
      b <- Dir.doesFileExist eventPath
      if b
        then BL.readFile eventPath
        else pure ""

    case decodeToTaggedObjects bl of
      Left err -> pure $ Left err
      Right vs -> pure $ sequence $ map (extractWrappedEvent ps) vs




  persistEvent _ e = do
    (wr :: WrappedEvent ss nn) <- mkWrappedEvent e
    b <- Dir.doesFileExist eventPath
    when (not b) (BL.writeFile eventPath "")
    BL.appendFile eventPath (Aeson.encode wr <> "\n")

  -- this should be bracketed and so forth @todo
  handleUpdateEvent awb awu (e :: Event n) = do
    persistEvent awb e
    let (AcidWorldBackendFS m :: AcidWorldBackendFS ss nn (EventResult n)) = runUpdateEvent awu e
    liftIO $ m





{-
AcidWorld Inner monad
-}
class (Monad (m ss)) => AcidWorldUpdate m ss where
  data AWUState m ss
  initialiseUpdate :: AcidWorldBackend b ss nn => (SegmentsState ss) -> b ss nn (AWUState m ss)
  getSegment :: (HasSegment ss s) =>  Proxy s -> m ss (SegmentS s)
  putSegment :: (HasSegment ss s) =>  Proxy s -> (SegmentS s) -> m ss ()
  runWrappedEvent :: WrappedEvent ss e -> m ss ()
  runWrappedEvent (WrappedEvent _ _ (Event xs :: Event n)) = void $ runEvent (Proxy :: Proxy n) xs
  runUpdateEvent :: ( AcidWorldBackend b ss nn
                    , HasSegments ss (EventSegments n)) =>
    AWUState m ss -> Event n -> b ss nn (EventResult n)


newtype AcidWorldUpdateStatePure ss a = AcidWorldUpdateStatePure (St.State (SegmentsState ss) a)
  deriving (Functor, Applicative, Monad)






instance AcidWorldUpdate AcidWorldUpdateStatePure ss where
  data AWUState AcidWorldUpdateStatePure ss = AWUStateStatePure {
      aWUStateStatePure :: TVar (SegmentsState ss)
    }
  initialiseUpdate defState = do
    mCpState <- getLastCheckpointState
    let startState = fromMaybe defState mCpState
    events <- do
      errEs <- loadEvents
      case errEs of
        Left err -> throwM $ AcidWorldInitialisationE err
        Right es -> pure es
    let (AcidWorldUpdateStatePure stm) = mapM runWrappedEvent events
    let (_ , s) = St.runState stm startState
    tvar <- liftIO $ STM.atomically $ TVar.newTVar s
    pure $ AWUStateStatePure tvar
  getSegment (Proxy :: Proxy s) = do
    r <- AcidWorldUpdateStatePure St.get
    pure $ V.getField $ V.rgetf (V.Label :: V.Label s) r
  putSegment (Proxy :: Proxy s) seg = do
    r <- AcidWorldUpdateStatePure St.get
    AcidWorldUpdateStatePure (St.put $ V.rputf (V.Label :: V.Label s) seg r)
  runUpdateEvent awuState (Event xs :: Event n) = do
    let (AcidWorldUpdateStatePure stm :: AcidWorldUpdateStatePure ss (EventResult n)) = runEvent (Proxy :: Proxy n) xs
    liftIO $ STM.atomically $ do
      s <- STM.readTVar (aWUStateStatePure awuState)
      let (a, s') = St.runState stm s
      STM.writeTVar (aWUStateStatePure awuState) s'
      return a

{-
  runUpdateEvent (Proxy :: Proxy (AcidWorldUpdateStatePure ss)) (Event xs :: Event n) s = do
    let (AcidWorldUpdateStatePure stm :: AcidWorldUpdateStatePure ss (EventResult n)) = runEvent (Proxy :: Proxy n) xs
    return $ St.runState stm s

-}

{- EVENTS -}



class ToUniqueText (a :: k) where
  toUniqueText :: Proxy a -> Text

instance (KnownSymbol a) => ToUniqueText (a :: Symbol) where
  toUniqueText = T.pack . symbolVal



eventPath :: FilePath
eventPath = "./event.json"





class (ElemOrErr n nn, Eventable n, HasSegments ss (EventSegments n)) => IsValidEvent ss nn n
instance (ElemOrErr n nn, Eventable n, HasSegments ss (EventSegments n)) => IsValidEvent ss nn n



class (Eventable n, HasSegments ss (EventSegments n)) => ValidEventName ss n
instance (Eventable n, HasSegments ss (EventSegments n)) => ValidEventName ss n

type ValidEventNames ss nn = All (ValidEventName ss) nn





-- representing the relationship between n xs and r
type EventableR n xs r =
  (Eventable n, EventArgs n ~ xs, EventResult n ~ r)


class (ToUniqueText n, V.RecAll V.Identity (EventArgs n) ToJSON, SListI (EventArgs n), All Aeson.FromJSON (EventArgs n)) => Eventable (n :: k) where
  type EventArgs n :: [*]
  type EventResult n :: *
  type EventSegments n :: [Symbol]
  runEvent :: (AcidWorldUpdate m ss, HasSegments ss (EventSegments n)) => Proxy n -> V.HList (EventArgs n) -> m ss (EventResult n)

toRunEvent :: V.Curried ts a -> V.Rec V.Identity ts -> a
toRunEvent = V.runcurry'

data Event (n :: k) where
  Event :: (Eventable n, EventArgs n ~ xs) => V.HList xs -> Event n

mkEvent :: forall n xs r. (V.RecordCurry xs, EventableR n xs r) => Proxy n -> V.Curried (xs ) (Event n)
mkEvent _  = V.rcurry' (Event :: V.HList xs -> Event n)

data WrappedEvent ss nn where
  WrappedEvent :: (HasSegments ss (EventSegments n)) => {
    wrappedEventTime :: Time.UTCTime,
    wrappedEventId :: UUID.UUID,
    wrappedEventEvent :: Event n} -> WrappedEvent ss nn


mkWrappedEvent :: (MonadIO m, HasSegments ss (EventSegments n)) => Event n -> m (WrappedEvent ss nn)
mkWrappedEvent e = do
  t <- Time.getCurrentTime
  uuid <- liftIO $ UUID.nextRandom
  return $ WrappedEvent t uuid e



-- @todo remove this orphan (probably by changing this code to use NP rather than Vinyl)
instance (ToJSON a) => ToJSON (V.Identity a) where
  toJSON = toJSON . V.getIdentity

instance ToJSON (WrappedEvent ss nn) where
  toJSON (WrappedEvent t ui (Event xs :: Event n)) = Object $ HM.fromList [
    ("eventName", toJSON (toUniqueText (Proxy :: Proxy n))),
    ("eventTime", toJSON t),
    ("eventId", toJSON ui),
    ("eventArgs", recToJSON xs)]

recToJSON :: forall xs. V.RecAll V.Identity xs ToJSON => V.HList xs -> Value
recToJSON xs =
  let (vs :: [Value]) = V.rfoldMap eachToJSON reifiedXs
  in toJSON vs
  where
    reifiedXs :: V.Rec (V.Dict ToJSON V.:. V.Identity) xs
    reifiedXs = V.reifyConstraint (Proxy :: Proxy ToJSON) xs
    eachToJSON :: (V.Dict ToJSON V.:. V.Identity) x -> [Value]
    eachToJSON (V.getCompose -> V.Dict a ) = [toJSON a]




newtype WrappedEventT ss nn n = WrappedEventT (WrappedEvent ss nn )
newtype ValueT a = ValueT Value



type WrappedEventParsers ss nn = HM.HashMap Text (Object -> Either Text (WrappedEvent ss nn))

makeParsers :: forall ss nn. (ValidEventNames ss nn) => WrappedEventParsers ss nn
makeParsers =
  let (wres) = cfoldMap_NP (Proxy :: Proxy (ValidEventName ss)) (\p -> [toTaggedTuple p]) proxyRec
  in HM.fromList wres
  where
    proxyRec :: NP Proxy nn
    proxyRec = pure_NP Proxy
    toTaggedTuple :: (ValidEventName ss n) => Proxy n -> (Text, Object -> Either Text (WrappedEvent ss nn))
    toTaggedTuple p = (toUniqueText p, decodeWrappedEvent p)

fromJSONEither :: Aeson.FromJSON a => Value -> Either Text a
fromJSONEither v =
  case Aeson.fromJSON v of
    (Aeson.Success a) -> pure a
    (Aeson.Error e) -> fail e

decodeToTaggedObjects :: BL.ByteString -> Either Text [(Object, Text)]
decodeToTaggedObjects bl = do
  let bs = filter (not . BL.null) $ BL.split 10 bl
  sequence . sequence $ mapM decodeToTaggedValue bs

decodeToTaggedValue :: BL.ByteString -> Either Text (Object, Text)
decodeToTaggedValue b = do
  hm <- left (T.pack) $ Aeson.eitherDecode' b
  case HM.lookup "eventName" hm of
    Just (String s) -> pure (hm, s)
    _ -> fail $ "Expected to find a text eventName key in value " <> show hm

extractWrappedEvent ::  WrappedEventParsers ss nn -> (Object, Text) -> Either Text (WrappedEvent ss nn)
extractWrappedEvent ps (o, t) = do
  case HM.lookup t ps of
    Nothing -> fail $ "Could not find parser for event named " <> show t
    Just p -> p o


decodeWrappedEvent :: forall n ss nn. (ValidEventName ss n) => Proxy n -> Object -> Either Text (WrappedEvent ss nn)
decodeWrappedEvent _ hm = do
  ((WrappedEventT wr) :: (WrappedEventT ss nn n))  <- fromJSONEither (Object hm)
  pure $ wr

instance (ValidEventName ss n, EventArgs n ~ xs) => Aeson.FromJSON (WrappedEventT ss nn n) where
  parseJSON = Aeson.withObject "WrappedEventT" $ \o -> do
    t <- o Aeson..: "eventTime"
    uid <- o Aeson..: "eventId"
    argVals <- o Aeson..: "eventArgs"
    {-npV <- npValues argVals
    let zipped = zipWith_NP (\_ v -> ValueT (unK v))  (pure_NP Proxy) npV
    npXs <- sequence_NP $ cmap_NP (Proxy :: Proxy Aeson.FromJSON) argToJSON zipped
-}
    npXs <- constructFromJson_NP argVals
    return $ WrappedEventT (WrappedEvent t uid ((Event $ npIToVinylHList npXs) :: Event n))
{-    where
      argToJSON :: (Aeson.FromJSON a) => ValueT a -> Aeson.Parser a
      argToJSON (ValueT v) = Aeson.parseJSON v
      npValues :: [Value] -> Aeson.Parser (NP (K Value) xs)
      npValues vs =
        case Generics.SOP.NP.fromList vs of
          Nothing -> fail $ "Expected to find a list that matched the number of argument xs, but got: " ++ (show vs)
          Just np -> pure np
-}
constructFromJson_NP :: forall xs. (All Aeson.FromJSON xs) => [Value] -> Aeson.Parser (NP I xs)
constructFromJson_NP [] =
  case sList :: SList xs of
    SNil   -> pure Nil
    SCons  -> fail "No values left but still expecting a type"

constructFromJson_NP (v:vs) =
  case sList :: SList xs of
    SNil   -> fail "More values than expected"
    SCons -> do
      r <- constructFromJson_NP vs
      a <- Aeson.parseJSON v
      pure $ I a :* r

    --f :* pure_NP f







{-
 attempt at composing events
instance (Eventable a, Eventable b) => Eventable (a, b) where
  type EventArgs (a, b) = '[(V.HList (EventArgs a), V.HList (EventArgs b))]
  type EventResult (a, b) = (EventResult a, EventResult b)
  type EventS (a, b) = (EventS a) V.++ (EventS b)
  --type EventC (a, b) = ()
  runEvent _ args = do
    let (argsA, argsB) = rHead args
    res1 <- runEvent (Proxy :: Proxy a) argsA
    res2 <- runEvent (Proxy :: Proxy b) argsB
    return (res1, res2)
  rHead :: V.Rec V.Identity (a ': xs) -> a
  rHead (ir V.:& _) = V.getIdentity ir

-}







