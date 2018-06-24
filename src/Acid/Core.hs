{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Acid.Core where
import RIO
import qualified RIO.Directory as Dir
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T
import qualified  RIO.ByteString.Lazy as BL
import qualified  RIO.Vector as V
import qualified  RIO.Vector.Boxed as VB
import qualified  RIO.Time as Time

import Control.Arrow (left)
import Generics.SOP
import Generics.SOP.NP
import GHC.TypeLits
import qualified Control.Monad.State.Strict as St

import qualified  Data.Vinyl as V


import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import Data.Aeson(ToJSON(..), Value(..), Object)

import qualified Control.Concurrent.STM.TVar as  TVar
import qualified Control.Concurrent.STM  as STM
import qualified Data.UUID  as UUID
import qualified Data.UUID.V4  as UUID

import Prelude(putStrLn)
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
    acidWorldBackendConfig :: AWBConfig bMonad ss nn,
    acidWorldUpdateMonadConfig :: AWUConfig uMonad ss,
    acidWorldBackendState :: AWBState bMonad ss nn,
    acidWorldUpdateState :: AWUState uMonad ss
    } -> AcidWorld ss nn


openAcidWorld :: forall m ss nn bMonad uMonad.
               ( MonadIO m
               , MonadThrow m
               , AcidWorldBackend bMonad ss nn
               , AcidWorldUpdate uMonad ss
               ) => Maybe (SegmentsState ss) -> AWBConfig bMonad ss nn -> AWUConfig uMonad ss ->  m (AcidWorld ss nn)
openAcidWorld mDefSt acidWorldBackendConfig acidWorldUpdateMonadConfig = do
  let defState = fromMaybe (defaultSegmentsState (Proxy :: Proxy ss)) mDefSt
  (acidWorldBackendState) <- initialiseBackend acidWorldBackendConfig defState


  let handles = BackendHandles {
          bhLoadEvents = loadEvents acidWorldBackendState,
          bhGetLastCheckpointState = getLastCheckpointState acidWorldBackendState
        }

  (acidWorldUpdateState) <- initialiseUpdate acidWorldUpdateMonadConfig handles defState

  return $ AcidWorld{..}

closeAcidWorld :: (MonadIO m) => AcidWorld ss nn -> m ()
closeAcidWorld (AcidWorld {..}) = do
  closeBackend acidWorldBackendState
  closeUpdate acidWorldUpdateState


update :: (IsValidEvent ss nn n, MonadIO m) => AcidWorld ss nn -> Event n -> m (EventResult n)
update (AcidWorld {..}) = handleUpdateEvent acidWorldBackendState acidWorldUpdateState

data BackendHandles m ss nn = BackendHandles {
    bhLoadEvents :: MonadIO m => m (Either Text (VB.Vector (WrappedEvent ss nn))),
    bhGetLastCheckpointState :: MonadIO m => m (Maybe (SegmentsState ss))
  }

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
  data AWBConfig m ss nn
  initialiseBackend :: (MonadIO z) => AWBConfig m ss nn -> (SegmentsState ss) -> z (AWBState m ss nn)
  closeBackend :: (MonadIO z) => AWBState m ss nn -> z ()
  closeBackend _ = pure ()

  -- should return the most recent checkpoint state, if any
  getLastCheckpointState :: (MonadIO z) => AWBState m ss nn -> z (Maybe (SegmentsState ss))
  getLastCheckpointState _ = pure Nothing
  -- return events since the last checkpoint, if any
  loadEvents :: (MonadIO z) =>  AWBState m ss nn -> z (Either Text (VB.Vector (WrappedEvent ss nn)))
  loadEvents _ = pure . pure $ VB.empty
  handleUpdateEvent :: (IsValidEvent ss nn n, MonadIO z, AcidWorldUpdate u ss) => (AWBState m ss nn) -> (AWUState u ss) -> Event n -> z (EventResult n)

newtype AcidWorldBackendFS ss nn a = AcidWorldBackendFS (IO a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadThrow)





instance ( ValidSegmentNames ss
         , ValidEventNames ss nn ) =>
  AcidWorldBackend AcidWorldBackendFS ss nn where
  data AWBState AcidWorldBackendFS ss nn = AWBStateBackendFS {
    sWBStateBackendConfig :: AWBConfig AcidWorldBackendFS ss nn
  }
  data AWBConfig AcidWorldBackendFS ss nn = AWBConfigBackendFS {
    aWBConfigBackendFSStateDir :: FilePath
  }
  initialiseBackend c _  = do
    stateP <- Dir.makeAbsolute (aWBConfigBackendFSStateDir c)
    Dir.createDirectoryIfMissing True stateP
    let eventPath = makeEventPath stateP
    b <- Dir.doesFileExist eventPath
    when (not b) (BL.writeFile eventPath "")
    pure $ AWBStateBackendFS c{aWBConfigBackendFSStateDir = stateP}
  loadEvents s = do
    let eventPath = makeEventPath (aWBConfigBackendFSStateDir . sWBStateBackendConfig $ s)
    let ps = makeParsers
    bl <- BL.readFile eventPath
    case decodeToTaggedObjects bl of
      Left err -> pure $ Left err
      Right vs -> pure $ sequence $ VB.map (extractWrappedEvent ps) vs
  -- this should be bracketed and so forth @todo
  handleUpdateEvent awb awu (e :: Event n) = do
    let eventPath = makeEventPath (aWBConfigBackendFSStateDir . sWBStateBackendConfig $ awb)
    (wr :: WrappedEvent ss nn) <- mkWrappedEvent e
    BL.appendFile eventPath (Aeson.encode wr <> "\n")

    let (AcidWorldBackendFS m :: AcidWorldBackendFS ss nn (EventResult n)) = runUpdateEvent awu e
    liftIO $ m

makeEventPath :: FilePath -> FilePath
makeEventPath fp = fp <> "/" <> "events.json"



{-
AcidWorld Inner monad
-}

runWrappedEvent :: AcidWorldUpdate m ss => WrappedEvent ss e -> m ss ()

runWrappedEvent (WrappedEvent _ _ (Event xs :: Event n)) = void $ runEvent (Proxy :: Proxy n) xs

class (Monad (m ss)) => AcidWorldUpdate m ss where
  data AWUState m ss
  data AWUConfig m ss
  initialiseUpdate :: (MonadIO z, MonadThrow z) => AWUConfig m ss -> (BackendHandles z ss nn) -> (SegmentsState ss) -> z (AWUState m ss)
  closeUpdate :: (MonadIO z) => AWUState m ss -> z ()
  closeUpdate _ = pure ()

  getSegment :: (HasSegment ss s) =>  Proxy s -> m ss (SegmentS s)
  putSegment :: (HasSegment ss s) =>  Proxy s -> (SegmentS s) -> m ss ()

  runUpdateEvent :: ( ValidEventName ss n
                    , MonadIO z) =>
    AWUState m ss -> Event n -> z (EventResult n)


newtype AcidWorldUpdateStatePure ss a = AcidWorldUpdateStatePure (St.State (SegmentsState ss) a)
  deriving (Functor, Applicative, Monad)






instance AcidWorldUpdate AcidWorldUpdateStatePure ss where
  data AWUState AcidWorldUpdateStatePure ss = AWUStateStatePure {
      aWUStateStatePure :: !(TVar (SegmentsState ss)),
      aWUStateStateDefState :: !(SegmentsState ss)
    }
  data AWUConfig AcidWorldUpdateStatePure ss = AWUConfigStatePure
  initialiseUpdate _ (BackendHandles{..}) defState = do
    mCpState <- bhGetLastCheckpointState
    let startState = fromMaybe defState mCpState
    events <- do
      errEs <- bhLoadEvents
      case errEs of
        Left err -> throwM $ AcidWorldInitialisationE err
        Right es -> pure es
    let (AcidWorldUpdateStatePure stm) = V.mapM runWrappedEvent events
    let (_ , !s) = St.runState stm startState
    tvar <- liftIO $ STM.atomically $ TVar.newTVar s

    pure $ AWUStateStatePure tvar s
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
      (!a, !s') <- pure $ St.runState stm s
      STM.writeTVar (aWUStateStatePure awuState) s'
      return a


{- EVENTS -}



class ToUniqueText (a :: k) where
  toUniqueText :: Proxy a -> Text

instance (KnownSymbol a) => ToUniqueText (a :: Symbol) where
  toUniqueText = T.pack . symbolVal







class (ElemOrErr n nn, Eventable n, HasSegments ss (EventSegments n)) => IsValidEvent ss nn n
instance (ElemOrErr n nn, Eventable n, HasSegments ss (EventSegments n)) => IsValidEvent ss nn n



class (Eventable n, HasSegments ss (EventSegments n)) => ValidEventName ss (n :: Symbol)
instance (Eventable n, HasSegments ss (EventSegments n)) => ValidEventName ss n

type ValidEventNames ss nn = All (ValidEventName ss) nn





-- representing the relationship between n xs and r
type EventableR n xs r =
  (Eventable n, EventArgs n ~ xs, EventResult n ~ r)


class (ToUniqueText n, All ToJSON (EventArgs n) , SListI (EventArgs n), All Aeson.FromJSON (EventArgs n)) => Eventable (n :: k) where
  type EventArgs n :: [*]
  type EventResult n :: *
  type EventSegments n :: [Symbol]
  runEvent :: (AcidWorldUpdate m ss, HasSegments ss (EventSegments n)) => Proxy n -> EventArgsContainer (EventArgs n) -> m ss (EventResult n)




newtype EventArgsContainer xs = EventArgsContainer {eventArgsContainerNp ::  NP I xs}

data Event (n :: k) where
  Event :: (Eventable n, EventArgs n ~ xs) => EventArgsContainer xs -> Event n

toRunEvent :: NPCurried ts a -> EventArgsContainer ts -> a
toRunEvent f  = npIUncurry f . eventArgsContainerNp

mkEvent :: forall n xs r. (NPCurry xs, EventableR n xs r) => Proxy n -> NPCurried xs (Event n)
mkEvent _  = npICurry (Event . EventArgsContainer :: NP I xs -> Event n)

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



instance ToJSON (WrappedEvent ss nn) where
  toJSON (WrappedEvent t ui (Event xs :: Event n)) = Object $ HM.fromList [
    ("eventName", toJSON (toUniqueText (Proxy :: Proxy n))),
    ("eventTime", toJSON t),
    ("eventId", toJSON ui),
    ("eventArgs", toJSON xs)]

instance (All ToJSON xs) => ToJSON (EventArgsContainer xs) where
  toJSON (EventArgsContainer np) =
    toJSON $ collapse_NP $ cmap_NP (Proxy :: Proxy ToJSON) (K . toJSON . unI) np


instance (ValidEventName ss n, EventArgs n ~ xs) => Aeson.FromJSON (WrappedEventT ss nn n) where
  parseJSON = Aeson.withObject "WrappedEventT" $ \o -> do
    t <- o Aeson..: "eventTime"
    uid <- o Aeson..: "eventId"
    args <- o Aeson..: "eventArgs"
    return $ WrappedEventT (WrappedEvent t uid ((Event args) :: Event n))

instance (All Aeson.FromJSON xs) => Aeson.FromJSON (EventArgsContainer xs) where
  parseJSON = Aeson.withArray "EventArgsContainer" $ \v -> fmap EventArgsContainer $ constructFromJson_NP (V.toList v)

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

decodeToTaggedObjects :: BL.ByteString -> Either Text (VB.Vector (Object, Text))
decodeToTaggedObjects bl = do
  let bs = VB.filter (not . BL.null) $ VB.fromList $ BL.split 10 bl
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







