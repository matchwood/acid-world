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
import qualified  Data.TypeMap.Vector as TM
import qualified  Data.TypeMap.Internal.Unsafe as TM
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


class HasElem (s :: Symbol) c where
  lookupT :: Proxy s -> c -> SegmentS s


instance (SegmentS s ~ t) => HasElem s (NP I ((Proxy s, t) ': moress)) where
  lookupT _ =  snd . unI . hd

instance (HasElem s (NP I moress)) => HasElem s (NP I ((Proxy a, t) ': moress)) where
  lookupT = lookupT



tryLookup :: NP I ('[(Proxy "Tups", [(Bool, Int)])]) -> SegmentS "Tups"
tryLookup np = lookupT  (Proxy :: Proxy "Tups") np



{-instance HasElem s (s ': moress) where
  lookup _ _
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

class (Elem a b ~ 'True) => IsElem (a :: k) (b :: [k])
instance  (Elem a b ~ 'True) => IsElem a b

class (CanLookup (EventS n) ss) => HasState n ss
instance (CanLookup (EventS n) ss) => HasState n ss


class Segment (s :: Symbol) where
  type SegmentS s :: *

instance Segment "Tups" where
  type SegmentS "Tups" = [(Bool, Int)]

{-data AcidWorldBackendFS =
  AcidWorldBackendFS ()
class (Monad m) => AcidWorldBackend m
-}
type CanLookup s ss = (TM.Lookup s (MapSegmentS ss) ~ SegmentS s, KnownNat (TM.Index s (MapSegmentS ss)))

class (Monad (m ss), All Segment ss) => AcidWorldUpdate m ss where
  getSegment :: (CanLookup s ss) => Proxy s -> m ss (SegmentS s)
  put ::  Proxy s -> (SegmentS s) -> m ss ()

{-class () => LookupSegment ss s where
  lookupSegment :: Proxy s -> Proxy ss -> TM.TypeVector (MapSegmentS (ss)) -> SegmentS s

instance (TM.Lookup s (MapSegmentS ss) ~ SegmentS s, KnownNat (TM.Index s (MapSegmentS ss))) => LookupSegment ss s where
  lookupSegment _ _ tmv = TM.index @s tmv-}

newtype AcidWorldUpdateStatePure (ss :: [Symbol]) a = AcidWorldUpdateStatePure (St.State (TM.TypeVector (MapSegmentS (ss))) a)
  deriving (Functor, Applicative, Monad)


instance (All Segment ss) => AcidWorldUpdate AcidWorldUpdateStatePure ss where
  getSegment (Proxy :: Proxy s) = do
    tmv <- AcidWorldUpdateStatePure St.get
    pure $ TM.index @s tmv


someMFunc :: (AcidWorldUpdate m ss, CanLookup "Tups" ss) => Int -> Bool -> Text -> m ss String
someMFunc i b t = do
  tups <- getSegment (Proxy :: Proxy "Tups")
  let newTups = (tups ++ [(b, i)])
  put (Proxy :: Proxy "Tups") newTups
  pure $ show t ++ show newTups



someFFunc :: (AcidWorldUpdate m ss, Show a) => a -> Bool -> Text -> m ss String
someFFunc i b t = do
  pure $ show (i,b,t)


type EventableR n xs r =
  (Eventable n, EventArgs n ~ xs, EventResult n ~ r)

class Eventable (n :: Symbol) where
  type EventArgs n :: [*]
  type EventResult n :: *
  type EventS n :: Symbol
  runEvent :: (AcidWorldUpdate m ss, CanLookup (EventS n) ss) => Proxy n -> HList (EventArgs n) -> m ss (EventResult n)

instance Eventable "someMFunc" where
  type EventArgs "someMFunc" = '[Int, Bool, Text]
  type EventResult "someMFunc" = String
  type EventS "someMFunc" = "Tups"
  runEvent _ = hUncurry someMFunc

instance Eventable "someFFunc" where
  type EventArgs "someFFunc" = '[Int, Bool, Text]
  type EventResult "someFFunc" = String
  type EventS "someFFunc" = "Tups"
  runEvent _ = hUncurry someFFunc

instance Eventable "renamedSomeFFunc" where
  type EventArgs "renamedSomeFFunc" = '[Int, Bool, Text]
  type EventResult "renamedSomeFFunc" = String
  type EventS "renamedSomeFFunc" = "Tups"
  runEvent _ = runEvent (Proxy :: Proxy "renamedSomeFFunc")


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


issueEvent :: (HasState n ss, AcidWorldUpdate m ss, EventableR n xs r, HTuple xs args) => Proxy n -> args -> m ss (r)
issueEvent p args = do
  let e = toEvent p args
  saveEvent e
  executeEvent e

executeEvent :: forall m ss n . (AcidWorldUpdate m ss, HasState n ss) => Event n -> m ss (EventResult n)
executeEvent (Event xs) = runEvent (Proxy :: Proxy n) xs


app :: (AcidWorldUpdate m ss, CanLookup "Tups" ss) => m ss ()
app =
  void $ issueEvent (Proxy :: Proxy ("someMFunc")) ((3 :: Int), False, ("asdf" :: Text))



{-toTaggedEvent :: forall f ar xs r n. (ar ~ ArityS f, FP.ArityRev f ar, FP.ArityFwd f ar, HCurry' ar f xs r) => Proxy n -> f -> TaggedEvent n
toTaggedEvent p f = TaggedEvent p (Proxy :: Proxy xs) (Proxy :: Proxy r) f


test :: TaggedEvent "someMFunc"
test = toTaggedEvent (Proxy :: Proxy "someMFunc") someMFunc
-}
{-
toRegisteredEvent :: n ->
-}

{-data TaggedEvent (n :: Symbol) f where
  TaggedEvent :: (EventFM n f) => Proxy n -> Proxy f -> TaggedEvent n f
-}
{-toTaggedEvent :: Proxy n -> f -> TaggedEvent n f
toTaggedEvent = TaggedEvent

runTaggedEvent :: TaggedEvent n f -> f
runTaggedEvent (TaggedEvent _ f) = f-}


{-data Event (n :: Symbol) xs where
  Event :: (Apply (EventF n) xs) => (Proxy n) -> (NP I xs) -> Event n xs

runEvent :: TaggedEvent n f -> Event n xs -> ApplyResult f xs
runEvent (TaggedEvent _ f)  (Event _ np) = apply f np
-}




{-
class ToBackendMonad a r where
  toBackendMonad :: a -> m r

instance (BackendMonad m) =>  IsBMonad (m a)

class (IsBMonad a) => EventFM n a where
  eventFM :: Proxy n -> a


instance (BackendMonad m) => EventFM "something" (Int -> m Bool) where
  eventFM _ i = pure $ i > 0-}

{-
type family EventF (n :: Symbol)

type instance EventF "something" = Int -> Bool
-}
{-

class (Monad m) => BackendMonad m where



class (BackendMonad m, EventResult m n ~ FunctionResult (EventT m n)) => Eventable m (n :: Symbol)  where
  type EventT m n :: *
  type EventResult m n :: *
  event ::Proxy m -> (Proxy n) -> EventT m n


instance (BackendMonad m) => Eventable m "anotherFunc" where
  type EventT m "anotherFunc" = Text -> String -> Int -> m Bool
  type EventResult m "anotherFunc" = m Bool
  event _ _ =  anotherFunc
-}





anotherFunc :: Text -> String -> Int -> m Bool
anotherFunc = undefined

{-

-}
{-extractEvent :: Event n xs -> DropResult (EventT n) -> m (FunctionResult (EventT n))
extractEvent (Event pn _) = event pn-}

{-
runEvent :: ( BackendMonad m) => Event n xs -> m (FunctionResult (EventT n))
runEvent (Event pn np) = apply (event pn) np
-}
{-class ApplyM m f xs where
  applyM :: f -> (NP I xs) ->  m (ApplyResult f xs)


instance ApplyM m (m a) ('[]) where
  applyM f _ = f

instance ApplyM m (a -> b) (a ': '[]) where
  applyM f np = f (unI . hd $ np)

instance (ApplyM m b (x ': xs)) => ApplyM m (a -> b) (a ': x ': xs) where
  applyM f np = apply (f (unI . hd $ np)) (tl np)-}




class Apply f xs where
  type ApplyResult f xs
  apply :: f -> (NP I xs) ->  ApplyResult f xs


instance Apply (a) ('[]) where
  type ApplyResult a  '[] = a
  apply f _ = f

instance Apply (a -> b) (a ': '[]) where
  type ApplyResult (a -> b) (a ': '[]) = b
  apply f np = f (unI . hd $ np)

instance (Apply b (x ': xs)) => Apply (a -> b) (a ': x ': xs) where
  type ApplyResult (a -> b) (a ': x ': xs) = ApplyResult b (x ': xs)
  apply f np = apply (f (unI . hd $ np)) (tl np)







{-class Apply (f :: * -> k) (xs :: [*]) where
  apply :: ((a ': '[moreXs]) ~ xs)=> f -> (NP I xs) ->  f a-}


{-apply ::((a ': moreXs ) ~ xs) => (a -> b) -> NP I xs -> b
apply = undefined-}

