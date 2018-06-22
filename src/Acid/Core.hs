{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# LANGUAGE AllowAmbiguousTypes #-}

module Acid.Core where
import RIO

import Generics.SOP
--import Generics.SOP.NP
import GHC.TypeLits
--import GHC.Exts
import qualified Control.Monad.State.Strict as St

import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.TypeLevel as V
import qualified  Data.Vinyl.Curry as V
import qualified  Data.Vinyl.Functor as V



type family FunctionArgs a = (res :: [*]) where
  FunctionArgs (a -> b) = (a ': FunctionArgs b)
  FunctionArgs (a) = '[]

type family DropResult a where
  DropResult (a -> b -> c) = a -> DropResult (b -> c)
  DropResult (a -> b) = a

type family FunctionResult a where
  FunctionResult (a -> b -> c -> d) = d
  FunctionResult (a -> b -> c) = c
  FunctionResult (a -> b) = b
  FunctionResult a = a


type family Elem (a :: k) (b :: [k]) :: Bool where
    Elem a '[] = 'False
    Elem a (a ': xs) = 'True
    Elem a (b ': xs) = Elem a xs

type family MapSegmentS (ss :: [Symbol]) :: [(Symbol, *)] where
  MapSegmentS '[] = '[]
  MapSegmentS (s ': ss) = '(s, SegmentS s) ': MapSegmentS ss


type family ToFields (ss :: [Symbol]) = (res :: [(Symbol, *)]) where
  ToFields '[] = '[]
  ToFields (s ': ss) = '(s, SegmentS s) ': ToFields ss


class (Elem a b ~ 'True) => IsElem (a :: k) (b :: [k])
instance  (Elem a b ~ 'True) => IsElem a b




class Segment (s :: Symbol) where
  type SegmentS s :: *
  defaultState :: Proxy s -> SegmentS s

class (V.KnownField a, Segment (V.Fst a), SegmentS (V.Fst a) ~ (V.Snd a)) => KnownSegmentField a
instance (V.KnownField a, Segment (V.Fst a), SegmentS (V.Fst a) ~ (V.Snd a)) => KnownSegmentField a


class (Monad (m ss), V.AllFields ss, V.AllConstrained KnownSegmentField ss) => AcidWorldBackend m (ss :: [(Symbol, *)]) where
  getState :: m ss (V.FieldRec ss)
  runAcidWorldBackend :: (MonadIO n) => m ss a -> n a

newtype AcidWorldBackendFS ss a = AcidWorldBackendFS (IO a)
  deriving (Functor, Applicative, Monad, MonadIO)


runAcidWorldBackendFS :: (MonadIO m, AcidWorldBackend AcidWorldBackendFS ss) => AcidWorldBackendFS ss a -> m a
runAcidWorldBackendFS = runAcidWorldBackend


makeField :: forall a. KnownSegmentField a => V.ElField '(V.Fst a, (V.Snd a))
makeField = (V.Label :: V.Label (V.Fst a)) V.=: (defaultState (Proxy :: Proxy (V.Fst a)))

instance (V.AllFields ss,  V.AllConstrained KnownSegmentField ss) => AcidWorldBackend AcidWorldBackendFS ss where
  getState = do
    let (a :: V.FieldRec ss) = V.rpureConstrained (Proxy :: Proxy KnownSegmentField) makeField
    return a

  runAcidWorldBackend (AcidWorldBackendFS m) = liftIO m

class (V.HasField V.Rec s ss (SegmentS s), KnownSymbol s) => HasSegment s ss
instance (V.HasField V.Rec s ss (SegmentS s), KnownSymbol s) => HasSegment s ss

class (V.HasField V.Rec s ss (SegmentS s), KnownSymbol s) => HasSegmentF ss s
instance (V.HasField V.Rec s ss (SegmentS s), KnownSymbol s) => HasSegmentF ss s

class (V.AllConstrained (HasSegmentF state) ss) => HasSegments state ss
instance (V.AllConstrained (HasSegmentF state) ss) => HasSegments state ss


class (Monad (m ss)) => AcidWorldUpdate m ss where
  getSegment :: (HasSegment s ss) =>  Proxy s -> m ss (SegmentS s)
  putSegment :: (HasSegment s ss) =>  Proxy s -> (SegmentS s) -> m ss ()
  runUpdate :: (AcidWorldBackend n ss) => m ss a -> n ss a

runAcidWorldUpdateStatePure :: (AcidWorldBackend m ss) => AcidWorldUpdateStatePure ss a -> m ss a
runAcidWorldUpdateStatePure = runUpdate

newtype AcidWorldUpdateStatePure ss a = AcidWorldUpdateStatePure (St.State (V.FieldRec ss) a)
  deriving (Functor, Applicative, Monad)


instance AcidWorldUpdate AcidWorldUpdateStatePure ss where
  getSegment (Proxy :: Proxy s) = do
    r <- AcidWorldUpdateStatePure St.get
    pure $ V.getField $ V.rgetf (V.Label :: V.Label s) r
  putSegment (Proxy :: Proxy s) seg = do
    r <- AcidWorldUpdateStatePure St.get
    AcidWorldUpdateStatePure (St.put $ V.rputf (V.Label :: V.Label s) seg r)
  runUpdate (AcidWorldUpdateStatePure stm) = do
    s <- getState
    let (a, _) = St.runState stm s
    return a



type EventableR n xs r =
  (Eventable n, EventArgs n ~ xs, EventResult n ~ r)

class Eventable (n :: Symbol) where
  type EventArgs n :: [*]
  type EventResult n :: *
  type EventS n :: [Symbol]
  runEvent :: (AcidWorldUpdate m ss, HasSegments ss (EventS n)) => Proxy n -> V.HList (EventArgs n) -> m ss (EventResult n)



data Event n where
  Event :: (Eventable n, EventArgs n ~ xs) => V.HList xs -> Event n

toEvent :: forall n xs r. (V.RecordCurry xs, EventableR n xs r) => Proxy n -> V.Curried (xs ) (Event n)
toEvent _  = V.rcurry' (Event :: V.Rec V.Identity xs -> Event n)

saveEvent :: Event n -> m ()
saveEvent _ = undefined


issueEvent :: (AcidWorldUpdate m ss, EventableR n xs r, HasSegments ss (EventS n) ) => Event n -> m ss (r)
issueEvent e =
  --saveEvent e
  executeEvent e

executeEvent :: forall m ss n . (AcidWorldUpdate m ss,  HasSegments ss (EventS n) ) => Event n -> m ss (EventResult n)
executeEvent (Event xs) = runEvent (Proxy :: Proxy n) xs





{- TEST CODE (will move later)-}




instance Segment "Tups" where
  type SegmentS "Tups" = [(Bool, Int)]
  defaultState _ = [(True, 1), (False, 2)]

instance Segment "List" where
  type SegmentS "List" = [String]
  defaultState _ = ["Hello", "I", "Work!"]


someMFunc :: (AcidWorldUpdate m ss, HasSegment "Tups" ss) => Int -> Bool -> Text -> m ss String
someMFunc i b t = do
  tups <- getSegment (Proxy :: Proxy "Tups")
  let newTups = (tups ++ [(b, i)])
  putSegment (Proxy :: Proxy "Tups") newTups
  pure $ show t ++ show newTups

someFFunc :: (AcidWorldUpdate m ss, HasSegment "List" ss) => String -> String -> String -> m ss ()
someFFunc a b c = do
  ls <- getSegment (Proxy :: Proxy "List")
  let newLs = ls ++ [a, b, c]
  putSegment (Proxy :: Proxy "List") newLs


instance Eventable "someMFunc" where
  type EventArgs "someMFunc" = '[Int, Bool, Text]
  type EventResult "someMFunc" = String
  type EventS "someMFunc" = '["Tups"]
  runEvent _ = V.runcurry' someMFunc

instance Eventable "someFFunc" where
  type EventArgs "someFFunc" = '[String, String, String]
  type EventResult "someFFunc" = ()
  type EventS "someFFunc" = '["List"]
  runEvent _ = V.runcurry' someFFunc

instance Eventable "returnListState" where
  type EventArgs "returnListState" = '[]
  type EventResult "returnListState" = [String]
  type EventS "returnListState" = '["List", "Tups"]
  runEvent _ _ =  do
    t <- getSegment (Proxy :: Proxy "List")
    l <- getSegment (Proxy :: Proxy "List")
    return $ show t : l


app :: (AcidWorldUpdate m ss, HasSegments ss (EventS "someMFunc"), HasSegments ss (EventS "someFFunc")) => m ss String
app = do
  s <- issueEvent $ toEvent (Proxy :: Proxy ("someMFunc")) 3 False "asdfsdf"
  void $ issueEvent $ toEvent (Proxy :: Proxy ("someFFunc")) "I" "Really" "Do"
  s2 <- issueEvent $ toEvent (Proxy :: Proxy ("returnListState"))
  return $ concat (s : s2)



littleTest :: (MonadIO m) => m String
littleTest = do
  let (u :: (AcidWorldBackendFS (ToFields '["Tups", "List"]) String)) = runAcidWorldUpdateStatePure app

  runAcidWorldBackendFS u