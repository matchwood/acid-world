{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Acid.Core where
import RIO
import qualified RIO.Directory as Dir
import qualified  RIO.ByteString.Lazy as BL
import qualified  RIO.Vector as V
import qualified  RIO.Vector.Boxed as VB

import Generics.SOP
import GHC.TypeLits
import qualified Control.Monad.State.Strict as St

import qualified  Data.Vinyl as V

import qualified Control.Concurrent.STM.TVar as  TVar
import qualified Control.Concurrent.STM  as STM

import Acid.Core.Segment
import Acid.Core.Event
import Acid.Core.Serialise


data AcidWorldException =
  AcidWorldInitialisationE Text
  deriving (Eq, Show, Typeable)
instance Exception AcidWorldException

data AcidWorld  ss nn t where
  AcidWorld :: (
                 AcidWorldBackend bMonad ss nn
               , AcidWorldUpdate uMonad ss
               , AcidSerialiseEvent t
               , AcidSerialiseT t ~ AWBSerialiseT bMonad ss nn
               ) => {
    acidWorldBackendConfig :: AWBConfig bMonad ss nn,
    acidWorldUpdateMonadConfig :: AWUConfig uMonad ss,
    acidWorldBackendState :: AWBState bMonad ss nn,
    acidWorldUpdateState :: AWUState uMonad ss,
    acidWorldSerialiserOptions :: AcidSerialiseEventOptions t
    } -> AcidWorld ss nn t


openAcidWorld :: forall m ss nn bMonad uMonad t.
               ( MonadIO m
               , MonadThrow m
               , AcidWorldBackend bMonad ss nn
               , AcidWorldUpdate uMonad ss
               , AcidSerialiseEvent t
               , AcidSerialiseT t ~ AWBSerialiseT bMonad ss nn
               , AcidDeserialiseConstraint t ss nn
               ) => Maybe (SegmentsState ss) -> AWBConfig bMonad ss nn -> AWUConfig uMonad ss -> AcidSerialiseEventOptions t ->  m (Either Text (AcidWorld ss nn t))
openAcidWorld mDefSt acidWorldBackendConfig acidWorldUpdateMonadConfig acidWorldSerialiserOptions = do
  let defState = fromMaybe (defaultSegmentsState (Proxy :: Proxy ss)) mDefSt
  (eAcidWorldBackendState) <- initialiseBackend acidWorldBackendConfig defState
  case eAcidWorldBackendState of
    Left err -> pure . Left $ err
    Right acidWorldBackendState -> do
      let parsers = acidSerialiseMakeParsers acidWorldSerialiserOptions (Proxy :: Proxy ss) (Proxy :: Proxy nn)
      let handles = BackendHandles {
              bhLoadEvents = loadEvents (acidDeserialiseEvent acidWorldSerialiserOptions parsers) acidWorldBackendState,
              bhGetLastCheckpointState = getLastCheckpointState acidWorldBackendState
            }

      (eAcidWorldUpdateState) <- initialiseUpdate acidWorldUpdateMonadConfig handles defState
      pure $ do
        acidWorldUpdateState <- eAcidWorldUpdateState
        pure AcidWorld{..}

closeAcidWorld :: (MonadIO m) => AcidWorld ss nn t -> m ()
closeAcidWorld (AcidWorld {..}) = do
  closeBackend acidWorldBackendState
  closeUpdate acidWorldUpdateState


update :: (IsValidEvent ss nn n, MonadIO m, AcidSerialiseConstraint t n) => AcidWorld ss nn t -> Event n -> m (EventResult n)
update (AcidWorld {..}) = handleUpdateEvent (acidSerialiseEvent acidWorldSerialiserOptions)  acidWorldBackendState acidWorldUpdateState

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
  type AWBSerialiseT m ss nn :: *
  initialiseBackend :: (MonadIO z) => AWBConfig m ss nn -> (SegmentsState ss) -> z (Either Text (AWBState m ss nn))
  closeBackend :: (MonadIO z) => AWBState m ss nn -> z ()
  closeBackend _ = pure ()

  -- should return the most recent checkpoint state, if any
  getLastCheckpointState :: (MonadIO z) => AWBState m ss nn -> z (Maybe (SegmentsState ss))
  getLastCheckpointState _ = pure Nothing
  -- return events since the last checkpoint, if any
  loadEvents :: (MonadIO z) => (AWBSerialiseT m ss nn -> Either Text (WrappedEvent ss nn)) ->  AWBState m ss nn -> z (Either Text (VB.Vector (WrappedEvent ss nn)))
  loadEvents _ _ = pure . pure $ VB.empty
  handleUpdateEvent :: (IsValidEvent ss nn n, MonadIO z, AcidWorldUpdate u ss) => (StorableEvent ss nn n -> AWBSerialiseT m ss nn) ->  (AWBState m ss nn) -> (AWUState u ss) -> Event n -> z (EventResult n)

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
  type AWBSerialiseT AcidWorldBackendFS ss nn = BL.ByteString
  initialiseBackend c _  = do
    stateP <- Dir.makeAbsolute (aWBConfigBackendFSStateDir c)
    Dir.createDirectoryIfMissing True stateP
    let eventPath = makeEventPath stateP
    b <- Dir.doesFileExist eventPath
    when (not b) (BL.writeFile eventPath "")
    pure . pure $ AWBStateBackendFS c{aWBConfigBackendFSStateDir = stateP}
  loadEvents deserialiser s = do
    let eventPath = makeEventPath (aWBConfigBackendFSStateDir . sWBStateBackendConfig $ s)



    bl <- BL.readFile eventPath
    let bs = VB.filter (not . BL.null) $ VB.fromList $ BL.split 10 bl

    pure $ sequence $ VB.map deserialiser bs

  -- this should be bracketed and so forth @todo
  handleUpdateEvent serializer awb awu (e :: Event n) = do
    let eventPath = makeEventPath (aWBConfigBackendFSStateDir . sWBStateBackendConfig $ awb)
    stE <- mkStorableEvent e
    BL.appendFile eventPath (serializer stE <> "\n")

    let (AcidWorldBackendFS m :: AcidWorldBackendFS ss nn (EventResult n)) = runUpdateEvent awu e
    liftIO $ m

makeEventPath :: FilePath -> FilePath
makeEventPath fp = fp <> "/" <> "events.json"



{-
AcidWorld Inner monad
-}

runWrappedEvent :: AcidWorldUpdate m ss => WrappedEvent ss e -> m ss ()

runWrappedEvent (WrappedEvent _ _ (Event xs :: Event n)) = void $ runEvent (Proxy :: Proxy n) xs

class (AcidWorldUpdateInner m ss) => AcidWorldUpdate m ss where
  data AWUState m ss
  data AWUConfig m ss
  initialiseUpdate :: (MonadIO z, MonadThrow z) => AWUConfig m ss -> (BackendHandles z ss nn) -> (SegmentsState ss) -> z (Either Text (AWUState m ss))
  closeUpdate :: (MonadIO z) => AWUState m ss -> z ()
  closeUpdate _ = pure ()



  runUpdateEvent :: ( ValidEventName ss n
                    , MonadIO z) =>
    AWUState m ss -> Event n -> z (EventResult n)


newtype AcidWorldUpdateStatePure ss a = AcidWorldUpdateStatePure (St.State (SegmentsState ss) a)
  deriving (Functor, Applicative, Monad)



instance AcidWorldUpdateInner AcidWorldUpdateStatePure ss where
  getSegment (Proxy :: Proxy s) = do
    r <- AcidWorldUpdateStatePure St.get
    pure $ V.getField $ V.rgetf (V.Label :: V.Label s) r
  putSegment (Proxy :: Proxy s) seg = do
    r <- AcidWorldUpdateStatePure St.get
    AcidWorldUpdateStatePure (St.put $ V.rputf (V.Label :: V.Label s) seg r)


instance AcidWorldUpdate AcidWorldUpdateStatePure ss where
  data AWUState AcidWorldUpdateStatePure ss = AWUStateStatePure {
      aWUStateStatePure :: !(TVar (SegmentsState ss)),
      aWUStateStateDefState :: !(SegmentsState ss)
    }
  data AWUConfig AcidWorldUpdateStatePure ss = AWUConfigStatePure
  initialiseUpdate _ (BackendHandles{..}) defState = do
    mCpState <- bhGetLastCheckpointState
    let startState = fromMaybe defState mCpState
    errEs <- bhLoadEvents
    case errEs of
      Left err -> pure . Left $ err
      Right events -> do
        let (AcidWorldUpdateStatePure stm) = V.mapM runWrappedEvent events
        let (_ , !s) = St.runState stm startState
        tvar <- liftIO $ STM.atomically $ TVar.newTVar s
        pure . pure $ AWUStateStatePure tvar s

  runUpdateEvent awuState (Event xs :: Event n) = do
    let (AcidWorldUpdateStatePure stm :: AcidWorldUpdateStatePure ss (EventResult n)) = runEvent (Proxy :: Proxy n) xs
    liftIO $ STM.atomically $ do
      s <- STM.readTVar (aWUStateStatePure awuState)
      (!a, !s') <- pure $ St.runState stm s
      STM.writeTVar (aWUStateStatePure awuState) s'
      return a















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







