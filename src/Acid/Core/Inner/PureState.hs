module Acid.Core.Inner.PureState where

import RIO

import Generics.SOP
import qualified Control.Monad.State.Strict as St

import qualified  Data.Vinyl as V

import qualified Control.Concurrent.STM.TVar as  TVar
import qualified Control.Concurrent.STM  as STM

import Acid.Core.Segment
import Acid.Core.Event
import Acid.Core.Inner.Abstract
import Conduit


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
    weStream <- bhLoadEvents

    s <- liftIO $ runConduitRes $ weStream .| foldlC applyToState startState


    tvar <- liftIO $ STM.atomically $ TVar.newTVar s
    pure . pure $ AWUStateStatePure tvar defState

    where
      applyToState :: SegmentsState ss -> WrappedEvent ss nn -> SegmentsState ss
      applyToState s e =
        let (AcidWorldUpdateStatePure stm) = runWrappedEvent e
        in snd $ St.runState stm s

    {-
    errEs <- bhLoadEvents
    case errEs of
      Left err -> pure . Left $ err
      Right events -> do
        let (AcidWorldUpdateStatePure stm) = V.mapM runWrappedEvent events
        let (_ , !s) = St.runState stm startState
        tvar <- liftIO $ STM.atomically $ TVar.newTVar s
        pure . pure $ AWUStateStatePure tvar s
-}
  runUpdateEvent awuState (Event xs :: Event n) = do
    let (AcidWorldUpdateStatePure stm :: AcidWorldUpdateStatePure ss (EventResult n)) = runEvent (Proxy :: Proxy n) xs
    liftIO $ STM.atomically $ do
      s <- STM.readTVar (aWUStateStatePure awuState)
      (!a, !s') <- pure $ St.runState stm s
      STM.writeTVar (aWUStateStatePure awuState) s'
      return a
