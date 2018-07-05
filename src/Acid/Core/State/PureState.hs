module Acid.Core.State.PureState where

import RIO

import Generics.SOP
import qualified Control.Monad.State.Strict as St
import qualified Control.Monad.Reader as Re

import qualified  Data.Vinyl as V

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
  getSegment (Proxy :: Proxy s) = do
    r <- AWUpdatePureState St.get
    pure $ V.getField $ V.rgetf (V.Label :: V.Label s) r
  putSegment (Proxy :: Proxy s) seg = do
    r <- AWUpdatePureState St.get
    AWUpdatePureState (St.put $ V.rputf (V.Label :: V.Label s) seg r)
  askSegment (Proxy :: Proxy s) = do
    r <- AWQueryPureState Re.ask
    pure $ V.getField $ V.rgetf (V.Label :: V.Label s) r
  initialiseState _ (BackendHandles{..}) defState = do
    mCpState <- bhGetLastCheckpointState
    let startState = fromMaybe defState mCpState
    weStream <- bhLoadEvents
    s <- liftIO $ runConduitRes $ weStream .| foldlC applyToState startState
    tvar <- liftIO $ STM.atomically $ TVar.newTVar s
    pure . pure $ AWStatePureState tvar
    where
      applyToState :: SegmentsState ss -> WrappedEvent ss nn -> SegmentsState ss
      applyToState s e =
        let (AWUpdatePureState stm) = runWrappedEvent e
        in snd $ St.runState stm s
  runUpdate :: forall ss n m. ( ValidEventName ss n , MonadIO m) => AWState AcidStatePureState ss -> Event n -> m (EventResult n)
  runUpdate awState (Event xs :: Event n) = do
    let (AWUpdatePureState stm :: ((AWUpdate AcidStatePureState)   ss (EventResult n))) = runEvent (Proxy :: Proxy n) xs
    liftIO $ STM.atomically $ do
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
