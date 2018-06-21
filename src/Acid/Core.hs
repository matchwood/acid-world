{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Acid.Core where
import RIO

import Generics.SOP
--import Generics.SOP.NP
import GHC.TypeLits
--import GHC.Exts
import Data.HList.HCurry
import Data.HList.HList
import qualified Data.HList.FakePrelude as FP
import qualified Control.Monad.State.Strict as St

import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.TypeLevel as V
import qualified  Data.Vinyl.Curry as V
{-
something :: NP I '[Text, String, Int] -> Bool
something np = runEvent (Event (Proxy :: Proxy "anotherFunc") np)


instance Eventable "someFunc" where
  type EventT "someFunc" = Bool -> Int -> Text
  event _ = undefined

instance Eventable "anotherFunc" where
  type EventT "anotherFunc" = Text -> String -> Int -> Bool
  event _ = undefined-}

{-

serialiseEvent :: Event n -> Text
serialiseEvent = undefined
deserialiseEvent :: Text -> Either Text (Event n)
deserialiseEvent = undefined

runEvent :: Event n -> m (EventResult n)-}


{-type family LookupT s c where
  LookupT s (NP I ((Proxy s, t) ': moress)) = t
  LookupT s (NP I ((Proxy a, t) ': moress)) = LookupT s (NP I moress)

class HasElem (s :: Symbol) c where
  lookupT :: Proxy s -> c -> LookupT s c


instance (SegmentS s ~ t) => HasElem s (NP I ((Proxy s, t) ': moress)) where
  lookupT _ =  snd . unI . hd

instance (HasElem s (NP I moress)) => HasElem s (NP I ((Proxy a, t) ': moress)) where
  lookupT s np = lookupT s  $ tl np



tryLookup :: NP I ('[(Proxy "Tups", [(Bool, Int)])]) -> SegmentS "Tups"
tryLookup np = lookupT (Proxy :: Proxy "Tups") np-}



{-instance HasElem s (s ': moress) where
  lookup _ _
-}
{-type family TypeEqual (a :: k) (b :: k) :: Bool where
  TypeEqual a a = 'True
  TypeEqual _ _ = 'False
-}

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

type family ArityS a where
  ArityS (a -> b) = 'FP.HSucc (ArityS b)
  ArityS a = 'FP.HZero

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



instance Segment "Tups" where
  type SegmentS "Tups" = [(Bool, Int)]
  defaultState _ = [(True, 1), (False, 2)]

instance Segment "List" where
  type SegmentS "List" = [String]
  defaultState _ = ["Hello", "I", "Work!"]

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


    -- undefined
    -- pure $ TM.index @s tmv


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


type EventableR n xs r =
  (Eventable n, EventArgs n ~ xs, EventResult n ~ r)

class Eventable (n :: Symbol) where
  type EventArgs n :: [*]
  type EventResult n :: *
  type EventS n :: Symbol
  runEvent :: (AcidWorldUpdate m ss, HasSegment (EventS n) ss) => Proxy n -> HList (EventArgs n) -> m ss (EventResult n)

  runEvent2 :: (AcidWorldUpdate m ss, HasSegment (EventS n) ss) => Proxy n -> V.HList (EventArgs n) -> m ss (EventResult n)

instance Eventable "someMFunc" where
  type EventArgs "someMFunc" = '[Int, Bool, Text]
  type EventResult "someMFunc" = String
  type EventS "someMFunc" = "Tups"
  runEvent _ = hUncurry someMFunc
  runEvent2 _ = V.runcurry' someMFunc

instance Eventable "someFFunc" where
  type EventArgs "someFFunc" = '[String, String, String]
  type EventResult "someFFunc" = ()
  type EventS "someFFunc" = "List"
  runEvent _ = hUncurry someFFunc

instance Eventable "returnListState" where
  type EventArgs "returnListState" = '[]
  type EventResult "returnListState" = [String]
  type EventS "returnListState" = "List"
  runEvent _ _ =  getSegment (Proxy :: Proxy "List")


data RegisteredEvent n where
  RegisteredEvent :: (Eventable n) => Proxy n -> RegisteredEvent n

data Event n where
  Event :: (Eventable n, EventArgs n ~ xs) => HList xs -> Event n

data TaggedEvent n where
  TaggedEvent :: (Eventable n) => TaggedEvent n


toEvent :: (EventableR n xs r, HTuple xs args) => Proxy n -> args -> Event n
toEvent _  tup = Event (hFromTuple tup)



saveEvent :: Event n -> m ()
saveEvent _ = undefined


issueEvent :: (AcidWorldUpdate m ss, EventableR n xs r, HTuple xs args, HasSegment (EventS n) ss) => Proxy n -> args -> m ss (r)
issueEvent p args = do
  let e = toEvent p args
  --saveEvent e
  executeEvent e

executeEvent :: forall m ss n . (AcidWorldUpdate m ss, HasSegment (EventS n) ss ) => Event n -> m ss (EventResult n)
executeEvent (Event xs) = runEvent (Proxy :: Proxy n) xs


app :: (AcidWorldUpdate m ss, HasSegment (EventS "someMFunc") ss, HasSegment (EventS "someFFunc") ss) => m ss String
app = do
  s <- issueEvent (Proxy :: Proxy ("someMFunc")) ((3 :: Int), False, ("asdf" :: Text))
  void $ issueEvent (Proxy :: Proxy ("someFFunc")) ("I", "Really", "do")
  s2 <- issueEvent (Proxy :: Proxy ("returnListState")) ()
  return $ concat (s : s2)



littleTest :: (MonadIO m) => m String
littleTest = do
  let (u :: (AcidWorldBackendFS (ToFields '["Tups", "List"]) String)) = runAcidWorldUpdateStatePure app

  runAcidWorldBackendFS u