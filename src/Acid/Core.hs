{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -fno-warn-orphans#-}

module Acid.Core where
import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T
import qualified  RIO.ByteString.Lazy as BL
import qualified  RIO.Vector as V

import Generics.SOP
import Generics.SOP.NP
import GHC.TypeLits
import GHC.Exts
import qualified Control.Monad.State.Strict as St

import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.TypeLevel as V
import qualified  Data.Vinyl.Curry as V
import qualified  Data.Vinyl.Functor as V


import qualified Data.Aeson as Aeson
import Data.Aeson(ToJSON(..), Value(..))

showSymbol :: (KnownSymbol a) => proxy a -> T.Text
showSymbol p = T.pack $ symbolVal p

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
type family ToEvents (nn :: [Symbol]) = (res :: [*]) where
  ToEvents '[] = '[]
  ToEvents (n ': nn) = (Event n) ': ToEvents nn

npToHList :: NP I xs -> V.HList xs
npToHList Nil = V.RNil
npToHList ((:*) ix restNp) = (V.Identity (unI ix)) V.:& (npToHList restNp)

class (Elem a b ~ 'True) => IsElem (a :: k) (b :: [k])
instance  (Elem a b ~ 'True) => IsElem a b

class ToText (a :: k) where
  toText :: Proxy a -> Text

instance (KnownSymbol a) => ToText (a :: Symbol) where
  toText = T.pack . symbolVal

instance (ToJSON a) => ToJSON (V.Identity a) where
  toJSON = toJSON . V.getIdentity

instance ToJSON (Event n) where
  toJSON (Event xs :: Event n) = Object $ HM.fromList [(toText (Proxy :: Proxy n), toJSON (recToJSON xs))]

recToJSON :: forall xs. V.RecAll V.Identity xs ToJSON => V.HList xs -> Value
recToJSON xs =
  let (vs :: [Value]) = V.rfoldMap eachToJSON reifiedXs
  in toJSON vs
  where
    reifiedXs :: V.Rec (V.Dict ToJSON V.:. V.Identity) xs
    reifiedXs = V.reifyConstraint (Proxy :: Proxy ToJSON) xs
    eachToJSON :: (V.Dict ToJSON V.:. V.Identity) x -> [Value]
    eachToJSON (V.getCompose -> V.Dict a ) = [toJSON a]

eventPath :: FilePath
eventPath = "./event.json"

class Segment (s :: Symbol) where
  type SegmentS s :: *
  defaultState :: Proxy s -> SegmentS s


type SegmentsState ss = V.FieldRec (ToFields ss)
type EventsList nn = V.HList (ToEvents nn)

class ToRealEvent n a where
  toRealEvent :: Proxy n -> a -> Event n


class (V.KnownField a, Segment (V.Fst a), SegmentS (V.Fst a) ~ (V.Snd a)) => KnownSegmentField a
instance (V.KnownField a, Segment (V.Fst a), SegmentS (V.Fst a) ~ (V.Snd a)) => KnownSegmentField a

class (V.HasField V.Rec s ss (SegmentS s), KnownSymbol s) => HasSegment s ss
instance (V.HasField V.Rec s ss (SegmentS s), KnownSymbol s) => HasSegment s ss

class (V.HasField V.Rec s ss (SegmentS s), KnownSymbol s) => HasSegmentF ss s
instance (V.HasField V.Rec s ss (SegmentS s), KnownSymbol s) => HasSegmentF ss s

type family HasSegments state ss :: Constraint where
  HasSegments state ss = V.AllConstrained (HasSegmentF (ToFields state)) ss

class (IsElem n nn, Eventable n) => HasEvent nn n
instance (IsElem n nn, Eventable n) => HasEvent nn n


class (Monad (m ss nn), V.AllFields (ToFields ss), V.AllConstrained KnownSegmentField (ToFields ss)) => AcidWorldBackend (m :: [Symbol] -> [Symbol] -> * -> *) (ss :: [Symbol]) (nn :: [Symbol]) where
  getState :: m ss nn (SegmentsState ss)
  runAcidWorldBackend :: (MonadIO n) => m ss nn a -> n a
  loadEvents :: m ss nn [WrappedEvent ss nn]

  runWrappedEvent :: (SegmentsState ss) -> WrappedEvent ss nn -> m ss nn (SegmentsState ss)
  runWrappedEvent s (WrappedEvent e) = do
    (_, s') <- runUpdateEvent (Proxy :: Proxy (AcidWorldUpdateStatePure ss)) e s
    return s'
  runEvents :: SegmentsState ss -> [WrappedEvent ss nn] -> m ss nn (SegmentsState ss)
  runEvents s nn = foldM runWrappedEvent s nn

{-    where-}
{-      appliedEvents :: [SegmentsState ss -> m ss nn (SegmentsState ss)]
      appliedEvents = V.rfoldMap applyEvent nn
      applyEvent :: (HasSegments ss (EventS n)) => V.Identity x -> [SegmentsState ss -> m ss nn (SegmentsState ss)]
      applyEvent e = [ \s -> do
        (_, s') <- runUpdateEvent (Proxy :: Proxy (AcidWorldUpdateStatePure ss)) (toEvent (Proxy :: Proxy n) e) s
        return s']-}



  saveEvent :: Event n -> m ss nn  ()
  issueUpdateEvent :: (HasEvent nn n, HasSegments ss (EventS n)) => Event n -> m ss nn (EventResult n)
  issueUpdateEvent e = do
    s <- getState
    saveEvent e
    (a, _) <- runUpdateEvent (Proxy :: Proxy (AcidWorldUpdateStatePure ss)) e s
    return a

newtype AcidWorldBackendFS ss nn a = AcidWorldBackendFS (IO a)
  deriving (Functor, Applicative, Monad, MonadIO)

runAcidWorldBackendFS :: (MonadIO m, AcidWorldBackend AcidWorldBackendFS ss nn) => AcidWorldBackendFS ss nn a -> m a
runAcidWorldBackendFS = runAcidWorldBackend


makeDefaultSegment :: forall a. KnownSegmentField a => V.ElField '(V.Fst a, (V.Snd a))
makeDefaultSegment = (V.Label :: V.Label (V.Fst a)) V.=: (defaultState (Proxy :: Proxy (V.Fst a)))


class (Eventable n, HasSegments ss (EventS n)) => ConstraintOnN ss n
instance (Eventable n, HasSegments ss (EventS n)) => ConstraintOnN ss n


instance (V.AllFields (ToFields ss),  V.AllConstrained KnownSegmentField (ToFields ss),  All (ConstraintOnN ss) nn) => AcidWorldBackend AcidWorldBackendFS ss nn where

  loadEvents = do
    bl <- BL.readFile eventPath
    case Aeson.eitherDecode' bl of
      Left err -> error err
      Right (v :: Value) -> do
        pure $ cfoldMap_NP (Proxy :: Proxy (ConstraintOnN ss)) (\p -> decodeWrappedEvent v p) proxyRec

    where
      proxyRec :: NP Proxy nn
      proxyRec = pure_NP Proxy

  saveEvent e = BL.writeFile eventPath (Aeson.encode e)

  getState = do
    let (a :: V.FieldRec (ToFields ss)) = V.rpureConstrained (Proxy :: Proxy KnownSegmentField) makeDefaultSegment

    es <- loadEvents
    runEvents a es

  runAcidWorldBackend (AcidWorldBackendFS m) = liftIO m


{-class (V.AllConstrained (HasSegmentF state) ss) => HasSegments state ss
instance (V.AllConstrained (HasSegmentF state) ss) => HasSegments state ss
-}


class (Monad (m ss)) => AcidWorldUpdate m ss where
  getSegment :: (HasSegment s (ToFields ss)) =>  Proxy s -> m ss (SegmentS s)
  putSegment :: (HasSegment s (ToFields ss)) =>  Proxy s -> (SegmentS s) -> m ss ()
  runUpdateEvent :: (AcidWorldBackend b ss nn, HasSegments ss (EventS n)) => Proxy (m ss) -> Event n -> (SegmentsState ss) -> b ss nn (EventResult n, SegmentsState ss)


newtype AcidWorldUpdateStatePure ss a = AcidWorldUpdateStatePure (St.State (SegmentsState ss) a)
  deriving (Functor, Applicative, Monad)


instance AcidWorldUpdate AcidWorldUpdateStatePure ss where
  getSegment (Proxy :: Proxy s) = do
    r <- AcidWorldUpdateStatePure St.get
    pure $ V.getField $ V.rgetf (V.Label :: V.Label s) r
  putSegment (Proxy :: Proxy s) seg = do
    r <- AcidWorldUpdateStatePure St.get
    AcidWorldUpdateStatePure (St.put $ V.rputf (V.Label :: V.Label s) seg r)

  runUpdateEvent (Proxy :: Proxy (AcidWorldUpdateStatePure ss)) (Event xs :: Event n) s = do
    let (AcidWorldUpdateStatePure stm :: AcidWorldUpdateStatePure ss (EventResult n)) = runEvent (Proxy :: Proxy n) xs
    return $ St.runState stm s


type EventableR n xs r =
  (Eventable n, EventArgs n ~ xs, EventResult n ~ r)

class (ToText n, V.RecAll V.Identity (EventArgs n) ToJSON, SListI (EventArgs n), All Aeson.FromJSON (EventArgs n)) => Eventable (n :: k) where
  type EventArgs n :: [*]
  type EventResult n :: *
  type EventS n :: [Symbol]
  runEvent :: (AcidWorldUpdate m ss, HasSegments ss (EventS n)) => Proxy n -> V.HList (EventArgs n) -> m ss (EventResult n)



data Event (n :: k) where
  Event :: (Eventable n, EventArgs n ~ xs) => V.HList xs -> Event n


data WrappedEvent ss nn where
  WrappedEvent :: (HasSegments ss (EventS n)) => Event n -> WrappedEvent ss nn

toEvent :: forall n xs r. (V.RecordCurry xs, EventableR n xs r) => Proxy n -> V.Curried (xs ) (Event n)
toEvent _  = V.rcurry' (Event :: V.Rec V.Identity xs -> Event n)


data ValueHolder a = ValueHolder (Proxy a, Value)

decodeWrappedEvent :: forall n ss nn xs. (ConstraintOnN ss n, EventArgs n ~ xs) =>  Value -> Proxy n -> [WrappedEvent ss nn]
decodeWrappedEvent (Object hm) p =
  case HM.lookup (toText p) hm of
    Nothing -> []
    Just (Array v) -> [WrappedEvent $ ((Event $ toHList (V.toList v)) :: Event n)]
    _ -> error "Expected to get an array"
  where
    toHList :: [Value] -> V.HList xs
    toHList vs = npToHList (xsNp vs)
    xsNp :: [Value] -> NP I xs
    xsNp vs = cmap_NP (Proxy :: Proxy Aeson.FromJSON) argToJSON (proxyNp vs)
    argToJSON :: (Aeson.FromJSON a) => ValueHolder a -> I a
    argToJSON (ValueHolder (_, v)) =
      case Aeson.fromJSON v of
        (Aeson.Error s) -> error s
        (Aeson.Success a) -> I a

    proxyNp :: [Value] -> NP (ValueHolder) xs
    proxyNp  vs = zipWith_NP (\pa v -> ValueHolder (pa, unK v))  (pure_NP Proxy) (kNp vs)
    kNp :: [Value] -> NP (K Value) xs
    kNp vs =
      case Generics.SOP.NP.fromList vs of
        Nothing -> error $ "Expected to find a list that matched the number of argument xs: " ++ (show vs)
        Just np -> np

decodeWrappedEvent _ _ = error "Expected object"



{- TEST CODE (will move later)-}




instance Segment "Tups" where
  type SegmentS "Tups" = [(Bool, Int)]
  defaultState _ = [(True, 1), (False, 2)]

instance Segment "List" where
  type SegmentS "List" = [String]
  defaultState _ = ["Hello", "I", "Work!"]


someMFunc :: (AcidWorldUpdate m ss, HasSegment "Tups" (ToFields ss)) => Int -> Bool -> Text -> m ss String
someMFunc i b t = do
  tups <- getSegment (Proxy :: Proxy "Tups")
  let newTups = (tups ++ [(b, i)])
  putSegment (Proxy :: Proxy "Tups") newTups
  pure $ show t ++ show newTups

someFFunc :: (AcidWorldUpdate m ss, HasSegment "List" (ToFields ss)) => String -> String -> String -> m ss [String]
someFFunc a b c = do
  ls <- getSegment (Proxy :: Proxy "List")
  let newLs = ls ++ [a, b, c]
  putSegment (Proxy :: Proxy "List") newLs
  getSegment (Proxy :: Proxy "List")


instance Eventable "someMFunc" where
  type EventArgs "someMFunc" = '[Int, Bool, Text]
  type EventResult "someMFunc" = String
  type EventS "someMFunc" = '["Tups"]
  runEvent _ = V.runcurry' someMFunc

instance Eventable "someFFunc" where
  type EventArgs "someFFunc" = '[String, String, String]
  type EventResult "someFFunc" = [String]
  type EventS "someFFunc" = '["List"]
  runEvent _ = V.runcurry' someFFunc

instance Eventable "returnListState" where
  type EventArgs "returnListState" = '[]
  type EventResult "returnListState" = [String]
  type EventS "returnListState" = '["List", "Tups"]
  runEvent _ _ =  do
    t <- getSegment (Proxy :: Proxy "Tups")
    l <- getSegment (Proxy :: Proxy "List")
    return $ show t : l

type family Union (a :: [k]) (b :: [k]) = (res :: [k]) where
  Union '[] b = b
  Union (a ': xs) b = a ': Union xs b

{-instance (Eventable a, Eventable b) => Eventable (a, b) where
  type EventArgs (a, b) = '[(V.HList (EventArgs a), V.HList (EventArgs b))]
  type EventResult (a, b) = (EventResult a, EventResult b)
  type EventS (a, b) = (EventS a) V.++ (EventS b)
  --type EventC (a, b) = ()
  runEvent _ args = do
    let (argsA, argsB) = rHead args
    res1 <- runEvent (Proxy :: Proxy a) argsA
    res2 <- runEvent (Proxy :: Proxy b) argsB
    return (res1, res2)
-}
rHead :: V.Rec V.Identity (a ': xs) -> a
rHead (ir V.:& _) = V.getIdentity ir

app :: (AcidWorldBackend m ss nn, HasEvent nn "someFFunc",  HasSegments ss (EventS "someFFunc")) => m ss nn String
app = do
  --s <- issueUpdateEvent $ toEvent (Proxy :: Proxy ("someMFunc")) 3 False "asdfsdf"
  s <- issueUpdateEvent $ toEvent (Proxy :: Proxy ("someFFunc")) "I" "Really" "Do"
  -- s <- issueUpdateEvent $ toEvent (Proxy :: Proxy ("returnListState"))
  return $ show s



littleTest :: (MonadIO m) => m String
littleTest = do
  let (u :: (AcidWorldBackendFS '["Tups", "List"] '["someFFunc"] String)) =  app
  runAcidWorldBackendFS u