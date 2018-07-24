module Acid.Core.State.PureState where

import RIO

import Generics.SOP
import qualified Control.Monad.State.Strict as St
import qualified Control.Monad.Reader as Re
import qualified  RIO.HashMap as HM

import qualified Control.Concurrent.STM.TMVar as  TMVar
import qualified Control.Concurrent.STM  as STM

import Acid.Core.Utils
import Acid.Core.Segment
import Acid.Core.State.Abstract
import Conduit


data AcidStatePureState



instance AcidWorldState AcidStatePureState where
  data AWState AcidStatePureState ss = AWStatePureState {
      aWStatePureStateState :: !(TMVar (SegmentsState ss)),
      awStatePureStateInvariants :: Invariants ss
    }
  data AWConfig AcidStatePureState ss = AWConfigPureState

  newtype AWUpdate AcidStatePureState ss a = AWUpdatePureState {extractAWUpdatePureState :: Re.ReaderT (Invariants ss) (St.StateT (ChangedSegmentsInvariantsMap AcidStatePureState  ss) (St.State (SegmentsState ss))) a}
    deriving (Functor, Applicative, Monad)
  newtype AWQuery AcidStatePureState ss a = AWQueryPureState (Re.Reader (SegmentsState ss) a)
    deriving (Functor, Applicative, Monad)
  getSegment ps = AWUpdatePureState . lift . lift $ getSegmentP ps `fmap` St.get

  putSegment (ps :: Proxy s) seg = AWUpdatePureState $ do
    invariants <- ask
    case getInvariantP invariants of
      Nothing -> pure ()
      Just (invar :: Invariant ss s) -> lift (St.modify' (registerChangedSegment invar))

    lift . lift $ St.modify' (putSegmentP ps seg)
  askSegment ps = AWQueryPureState $ getSegmentP ps `fmap` Re.ask
  initialiseState :: forall z ss nn. (MonadIO z,  ValidSegmentsAndInvar ss) => AWConfig AcidStatePureState ss -> (BackendHandles z ss nn) -> (Invariants ss) -> z (Either AWException (AWState AcidStatePureState ss))
  initialiseState _ (BackendHandles{..}) invars = do


    eBind bhGetInitialState $ \initState -> do
      weStream <- bhLoadEvents
      eS <- liftIO $ runConduitRes $ weStream .| breakOnLeft applyToState initState
      case eS of
        Left err -> pure . Left . AWExceptionEventDeserialisationError $ err
        Right s -> do

          -- run invariants
          case runAWQueryPureState (runChangedSegmentsInvariantsMap (allInvariants invars)) s of
            Nothing -> do
              tvar <- liftIO $ STM.atomically $ TMVar.newTMVar s
              pure . pure $ AWStatePureState tvar invars
            Just err -> pure . Left $ err
    where
      breakOnLeft :: (Monad m) => (s -> a -> s) -> s -> ConduitT (Either Text a) o m (Either Text s)
      breakOnLeft f = loop
        where
          loop !s = await >>= maybe (return $ Right s) go
            where
              go (Left err) = pure (Left err)
              go (Right a) = loop (f s a)
      applyToState :: SegmentsState ss -> WrappedEvent ss nn -> SegmentsState ss
      applyToState s e = snd $ runAWUpdatePureState (runWrappedEvent e) s invars

  runUpdateC :: forall ss firstN ns m. (ValidAcidWorldState AcidStatePureState ss, All (ValidEventName ss) (firstN ': ns), MonadIO m) => AWState AcidStatePureState ss -> EventC (firstN ': ns) ->  m (Either AWException (NP Event (firstN ': ns), EventResult firstN, m (), m ()))
  runUpdateC awState ec = liftIO $ STM.atomically $ do
    s <- TMVar.takeTMVar (aWStatePureStateState awState)
    let (((!events, !eventResult), !invarsToRun), !s') = runAWUpdatePureState (runEventC ec) s (awStatePureStateInvariants awState)

    case runAWQueryPureState (runChangedSegmentsInvariantsMap invarsToRun) s' of
      Nothing -> do
        pure . Right $ (events, eventResult, (liftIO . STM.atomically $ TMVar.putTMVar (aWStatePureStateState awState) s'), (liftIO . STM.atomically $ TMVar.putTMVar (aWStatePureStateState awState) s))
      Just errs -> do
        TMVar.putTMVar (aWStatePureStateState awState) s
        pure . Left $ errs


  runQuery awState q = do
    liftIO $ STM.atomically $ do
      s <- TMVar.readTMVar (aWStatePureStateState awState)
      pure $ runAWQueryPureState q s
  liftQuery q = do
    s <- AWUpdatePureState . lift . lift $ St.get
    pure $ runAWQueryPureState q s


runAWQueryPureState :: AWQuery AcidStatePureState ss a -> SegmentsState ss -> a
runAWQueryPureState (AWQueryPureState q) s = Re.runReader q s

runAWUpdatePureState :: AWUpdate AcidStatePureState ss a -> SegmentsState ss -> Invariants ss -> ((a, ChangedSegmentsInvariantsMap AcidStatePureState ss), SegmentsState ss)
runAWUpdatePureState act s i =
  let stm = extractAWUpdatePureState act
  in St.runState (St.runStateT (Re.runReaderT stm i)  HM.empty) s