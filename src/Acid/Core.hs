{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
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
import qualified Control.Monad.State.Strict as St

import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.TypeLevel as V
import qualified  Data.Vinyl.Curry as V
import qualified  Data.Vinyl.Functor as V


import qualified Data.Aeson as Aeson
import Data.Aeson(ToJSON(..), Value(..))

import Acid.Core.Segment
import Acid.Core.Utils


{-
AcidWorldBackend
-}
class ( Monad (m ss nn)
      , ValidSegmentNames ss
      , ValidEventNames ss nn
      ) =>
  AcidWorldBackend (m :: [Symbol] -> [Symbol] -> * -> *) (ss :: [Symbol]) (nn :: [Symbol]) where
  getState :: m ss nn (SegmentsState ss)
  runAcidWorldBackend :: (MonadIO n) => m ss nn a -> n a
  loadEvents :: m ss nn [WrappedEvent ss nn]
  runWrappedEvent :: (SegmentsState ss) -> WrappedEvent ss nn -> m ss nn (SegmentsState ss)
  runWrappedEvent s (WrappedEvent e) = do
    (_, s') <- runUpdateEvent (Proxy :: Proxy (AcidWorldUpdateStatePure ss)) e s
    return s'
  runWrappedEvents :: SegmentsState ss -> [WrappedEvent ss nn] -> m ss nn (SegmentsState ss)
  runWrappedEvents s nn = foldM runWrappedEvent s nn



  saveEvent :: Event n -> m ss nn  ()
  issueUpdateEvent :: (HasEvent nn n, HasSegments ss (EventSegments n)) => Event n -> m ss nn (EventResult n)
  issueUpdateEvent e = do
    s <- getState
    saveEvent e
    (a, _) <- runUpdateEvent (Proxy :: Proxy (AcidWorldUpdateStatePure ss)) e s
    return a

newtype AcidWorldBackendFS ss nn a = AcidWorldBackendFS (IO a)
  deriving (Functor, Applicative, Monad, MonadIO)


instance ( ValidSegmentNames ss
         , ValidEventNames ss nn ) =>
  AcidWorldBackend AcidWorldBackendFS ss nn where

  loadEvents = do
    bl <- BL.readFile eventPath
    case Aeson.eitherDecode' bl of
      Left err -> error err
      Right (v :: Value) -> do
        pure $ cfoldMap_NP (Proxy :: Proxy (ValidEventName ss)) (\p -> decodeWrappedEvent v p) proxyRec

    where
      proxyRec :: NP Proxy nn
      proxyRec = pure_NP Proxy

  saveEvent e = BL.writeFile eventPath (Aeson.encode e)

  getState = do
    let defState = defaultSegmentsState (Proxy :: Proxy ss)

    es <- loadEvents
    runWrappedEvents defState es

  runAcidWorldBackend (AcidWorldBackendFS m) = liftIO m

runAcidWorldBackendFS :: (MonadIO m, AcidWorldBackend AcidWorldBackendFS ss nn) => AcidWorldBackendFS ss nn a -> m a
runAcidWorldBackendFS = runAcidWorldBackend



{-
AcidWorld Inner monad
-}
class (Monad (m ss)) => AcidWorldUpdate m ss where
  getSegment :: (HasSegment ss s) =>  Proxy s -> m ss (SegmentS s)
  putSegment :: (HasSegment ss s) =>  Proxy s -> (SegmentS s) -> m ss ()
  runUpdateEvent :: ( AcidWorldBackend b ss nn
                    , HasSegments ss (EventSegments n)) =>
    Proxy (m ss) -> Event n -> (SegmentsState ss) -> b ss nn (EventResult n, SegmentsState ss)


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



{- EVENTS -}



class ToUniqueText (a :: k) where
  toUniqueText :: Proxy a -> Text

instance (KnownSymbol a) => ToUniqueText (a :: Symbol) where
  toUniqueText = T.pack . symbolVal



instance (ToJSON a) => ToJSON (V.Identity a) where
  toJSON = toJSON . V.getIdentity

instance ToJSON (Event n) where
  toJSON (Event xs :: Event n) = Object $ HM.fromList [(toUniqueText (Proxy :: Proxy n), toJSON (recToJSON xs))]

recToJSON :: forall xs. V.RecAll V.Identity xs ToJSON => V.HList xs -> Value
recToJSON xs =
  let (vs :: [Value]) = V.rfoldMap eachToJSON reifiedXs
  in toJSON vs
  where
    reifiedXs :: V.Rec (V.Dict ToJSON V.:. V.Identity) xs
    reifiedXs = V.reifyConstraint (Proxy :: Proxy ToJSON) xs
    eachToJSON :: (V.Dict ToJSON V.:. V.Identity) x -> [Value]
    eachToJSON (V.getCompose -> V.Dict a ) = [toJSON a]

data ValueHolder a = ValueHolder (Proxy a, Value)

decodeWrappedEvent :: forall n ss nn xs. (ValidEventName ss n, EventArgs n ~ xs) =>  Value -> Proxy n -> [WrappedEvent ss nn]
decodeWrappedEvent (Object hm) p =
  case HM.lookup (toUniqueText p) hm of
    Nothing -> []
    Just (Array v) -> [WrappedEvent $ ((Event $ toHList (V.toList v)) :: Event n)]
    _ -> error "Expected to get an array"
  where
    toHList :: [Value] -> V.HList xs
    toHList vs = npIToVinylHList (xsNp vs)
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


eventPath :: FilePath
eventPath = "./event.json"






class (IsElem n nn, Eventable n) => HasEvent nn n
instance (IsElem n nn, Eventable n) => HasEvent nn n

class (Eventable n, HasSegments ss (EventSegments n)) => ValidEventName ss n
instance (Eventable n, HasSegments ss (EventSegments n)) => ValidEventName ss n

type ValidEventNames ss nn = All (ValidEventName ss) nn





-- representing the relationship between n xs and r
type EventableR n xs r =
  (Eventable n, EventArgs n ~ xs, EventResult n ~ r)


class (ToUniqueText n, V.RecAll V.Identity (EventArgs n) ToJSON, SListI (EventArgs n), All Aeson.FromJSON (EventArgs n)) => Eventable (n :: k) where
  type EventArgs n :: [*]
  type EventResult n :: *
  type EventSegments n :: [Symbol]
  runEvent :: (AcidWorldUpdate m ss, HasSegments ss (EventSegments n)) => Proxy n -> V.HList (EventArgs n) -> m ss (EventResult n)

data Event (n :: k) where
  Event :: (Eventable n, EventArgs n ~ xs) => V.HList xs -> Event n

mkEvent :: forall n xs r. (V.RecordCurry xs, EventableR n xs r) => Proxy n -> V.Curried (xs ) (Event n)
mkEvent _  = V.rcurry' (Event :: V.HList xs -> Event n)

data WrappedEvent ss nn where
  WrappedEvent :: (HasSegments ss (EventSegments n)) => Event n -> WrappedEvent ss nn








{- TEST CODE (will move later)-}




instance Segment "Tups" where
  type SegmentS "Tups" = [(Bool, Int)]
  defaultState _ = [(True, 1), (False, 2)]

instance Segment "List" where
  type SegmentS "List" = [String]
  defaultState _ = ["Hello", "I", "Work!"]


someMFunc :: (AcidWorldUpdate m ss, HasSegment ss  "Tups" ) => Int -> Bool -> Text -> m ss String
someMFunc i b t = do
  tups <- getSegment (Proxy :: Proxy "Tups")
  let newTups = (tups ++ [(b, i)])
  putSegment (Proxy :: Proxy "Tups") newTups
  pure $ show t ++ show newTups

someFFunc :: (AcidWorldUpdate m ss, HasSegment ss  "List") => String -> String -> String -> m ss [String]
someFFunc a b c = do
  ls <- getSegment (Proxy :: Proxy "List")
  let newLs = ls ++ [a, b, c]
  putSegment (Proxy :: Proxy "List") newLs
  getSegment (Proxy :: Proxy "List")


instance Eventable "someMFunc" where
  type EventArgs "someMFunc" = '[Int, Bool, Text]
  type EventResult "someMFunc" = String
  type EventSegments "someMFunc" = '["Tups"]
  runEvent _ = V.runcurry' someMFunc

instance Eventable "someFFunc" where
  type EventArgs "someFFunc" = '[String, String, String]
  type EventResult "someFFunc" = [String]
  type EventSegments "someFFunc" = '["List"]
  runEvent _ = V.runcurry' someFFunc

instance Eventable "returnListState" where
  type EventArgs "returnListState" = '[]
  type EventResult "returnListState" = [String]
  type EventSegments "returnListState" = '["List", "Tups"]
  runEvent _ _ =  do
    t <- getSegment (Proxy :: Proxy "Tups")
    l <- getSegment (Proxy :: Proxy "List")
    return $ show t : l

type family Union (a :: [k]) (b :: [k]) = (res :: [k]) where
  Union '[] b = b
  Union (a ': xs) b = a ': Union xs b

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

app :: (AcidWorldBackend m ss nn, HasEvent nn "someFFunc",  HasSegments ss (EventSegments "someFFunc")) => m ss nn String
app = do
  --s <- issueUpdateEvent $ toEvent (Proxy :: Proxy ("someMFunc")) 3 False "asdfsdf"
  s <- issueUpdateEvent $ mkEvent (Proxy :: Proxy ("someFFunc")) "I" "Really" "Do"
  -- s <- issueUpdateEvent $ toEvent (Proxy :: Proxy ("returnListState"))
  return $ show s



littleTest :: (MonadIO m) => m String
littleTest = do
  let (u :: (AcidWorldBackendFS '["Tups", "List"] '["someFFunc"] String)) =  app
  runAcidWorldBackendFS u