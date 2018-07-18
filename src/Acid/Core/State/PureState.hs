module Acid.Core.State.PureState where

import RIO

import Generics.SOP
import qualified Control.Monad.State.Strict as St
import qualified Control.Monad.Reader as Re

import qualified Control.Concurrent.STM.TVar as  TVar
import qualified Control.Concurrent.STM  as STM

import Acid.Core.Segment
import Acid.Core.State.Abstract
import Conduit


data AcidStatePureState



instance AcidWorldState AcidStatePureState where
  data AWState AcidStatePureState ss = AWStatePureState {
      aWStatePureStateState :: !(TVar (SegmentsState ss))
    }
  data AWConfig AcidStatePureState ss = AWConfigPureState

  newtype AWUpdate AcidStatePureState ss a = AWUpdatePureState (St.State (SegmentsState ss) a)
    deriving (Functor, Applicative, Monad)
  newtype AWQuery AcidStatePureState ss a = AWQueryPureState (Re.Reader (SegmentsState ss) a)
    deriving (Functor, Applicative, Monad)
  getSegment ps = AWUpdatePureState $ getSegmentP ps `fmap` St.get

  putSegment ps seg = AWUpdatePureState $ St.modify' (putSegmentP ps seg)
  askSegment ps = AWQueryPureState $ getSegmentP ps `fmap` Re.ask
  initialiseState :: forall z ss nn. (MonadIO z,  ValidSegments ss) => AWConfig AcidStatePureState ss -> (BackendHandles z ss nn) -> (SegmentsState ss) -> z (Either Text (AWState AcidStatePureState ss))
  initialiseState _ (BackendHandles{..}) defState = do
    mCpState <- bhGetLastCheckpointState

    case mCpState of
      (Left err) -> pure $ Left err
      Right cpState -> do
        let startState = fromMaybe defState cpState
        weStream <- bhLoadEvents
        eS <- liftIO $ runConduitRes $ weStream .| breakOnLeft applyToState startState
        case eS of
          Left err -> pure $ Left err
          Right s -> do
            tvar <- liftIO $ STM.atomically $ TVar.newTVar s
            pure . pure $ AWStatePureState tvar
    where
      breakOnLeft :: (Monad m) => (s -> a -> s) -> s -> ConduitT (Either Text a) o m (Either Text s)
      breakOnLeft f = loop
        where
          loop !s = await >>= maybe (return $ Right s) go
            where
              go (Left err) = pure (Left err)
              go (Right a) = loop (f s a)
      applyToState :: SegmentsState ss -> WrappedEvent ss nn -> SegmentsState ss
      applyToState s e =
        let (AWUpdatePureState stm) = runWrappedEvent e
        in snd $ St.runState stm s
  runUpdate :: forall ss n m. (ValidAcidWorldState AcidStatePureState ss, ValidEventName ss n , MonadIO m) => AWState AcidStatePureState ss -> Event n -> m (EventResult n)
  runUpdate awState (Event xs :: Event n) = liftIO $ STM.atomically $ do
    let (AWUpdatePureState stm :: ((AWUpdate AcidStatePureState)   ss (EventResult n))) = runEvent (Proxy :: Proxy n) xs
    s <- STM.readTVar (aWStatePureStateState awState)
    (!a, !s') <- pure $ St.runState stm s
    STM.writeTVar (aWStatePureStateState awState) s'
    return a

  runQuery awState (AWQueryPureState q) = do
    liftIO $ STM.atomically $ do
      s <- STM.readTVar (aWStatePureStateState awState)
      pure $ Re.runReader q s
  liftQuery (AWQueryPureState q) = do
    s <- AWUpdatePureState St.get
    pure $ Re.runReader q s
