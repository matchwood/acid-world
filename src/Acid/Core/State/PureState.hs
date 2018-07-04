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



instance AcidWorldState AcidStatePureState ss where
  data AWState AcidStatePureState ss = AWStateStatePure {
      aWStateStatePure :: !(TVar (SegmentsState ss)),
      aWStateStateDefState :: !(SegmentsState ss)
    }
  data AWConfig AcidStatePureState ss = AWConfigStatePure

  newtype AWUpdate AcidStatePureState ss a = AWUpdateStatePure (St.State (SegmentsState ss) a)
    deriving (Functor, Applicative, Monad)
  newtype AWQuery AcidStatePureState ss a = AWQueryStatePure (Re.Reader (SegmentsState ss) a)
    deriving (Functor, Applicative, Monad)
  getSegment (Proxy :: Proxy s) = do
    r <- AWUpdateStatePure St.get
    pure $ V.getField $ V.rgetf (V.Label :: V.Label s) r
  putSegment (Proxy :: Proxy s) seg = do
    r <- AWUpdateStatePure St.get
    AWUpdateStatePure (St.put $ V.rputf (V.Label :: V.Label s) seg r)
  askSegment (Proxy :: Proxy s) = do
    r <- AWQueryStatePure Re.ask
    pure $ V.getField $ V.rgetf (V.Label :: V.Label s) r
  initialiseState _ (BackendHandles{..}) defState = do
    mCpState <- bhGetLastCheckpointState
    let startState = fromMaybe defState mCpState
    weStream <- bhLoadEvents
    s <- liftIO $ runConduitRes $ weStream .| foldlC applyToState startState
    tvar <- liftIO $ STM.atomically $ TVar.newTVar s
    pure . pure $ AWStateStatePure tvar defState
    where
      applyToState :: SegmentsState ss -> WrappedEvent ss nn -> SegmentsState ss
      applyToState s e =
        let (AWUpdateStatePure stm) = runWrappedEvent e
        in snd $ St.runState stm s

  runUpdate awState (Event xs :: Event n) = do
    let (AWUpdateStatePure stm :: ((AWUpdate AcidStatePureState)   ss (EventResult n))) = runEvent (Proxy :: Proxy n) xs
    liftIO $ STM.atomically $ do
      s <- STM.readTVar (aWStateStatePure awState)
      (!a, !s') <- pure $ St.runState stm s
      STM.writeTVar (aWStateStatePure awState) s'
      return a

  runQuery awState (AWQueryStatePure q) = do
    liftIO $ STM.atomically $ do
      s <- STM.readTVar (aWStateStatePure awState)
      pure $ Re.runReader q s
  liftQuery (AWQueryStatePure q) = do
    s <- AWUpdateStatePure St.get
    pure $ Re.runReader q s
