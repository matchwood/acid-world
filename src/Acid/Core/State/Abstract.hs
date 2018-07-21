
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Acid.Core.State.Abstract where
import RIO
import qualified  RIO.Text as T
import qualified  RIO.List as L
import qualified  RIO.Time as Time

import Generics.SOP
import Generics.SOP.NP
import GHC.TypeLits


import qualified Data.UUID  as UUID
import qualified Data.UUID.V4  as UUID

import Acid.Core.Segment
import Acid.Core.Utils
import Data.Aeson(FromJSON(..), ToJSON(..))
import Conduit

import qualified  Data.Vinyl.Derived as V
import qualified  Data.Vinyl.TypeLevel as V


{-
the main definition of an state managing strategy
-}


class AcidWorldState (i :: *) where
  data AWState i (ss :: [Symbol])
  data AWConfig i (ss :: [Symbol])
  data AWUpdate i (ss :: [Symbol]) a
  data AWQuery i (ss :: [Symbol]) a
  initialiseState :: (MonadIO z, ValidSegments ss) => AWConfig i ss -> (BackendHandles z ss nn) -> (SegmentsState ss) -> z (Either Text (AWState i ss))
  closeState :: (MonadIO z) => AWState i ss -> z ()
  closeState _ = pure ()
  getSegment :: (HasSegment ss s) =>  Proxy s -> AWUpdate i ss (SegmentS s)
  putSegment :: (HasSegment ss s) =>  Proxy s -> (SegmentS s) -> AWUpdate i ss ()
  askSegment :: (HasSegment ss s) =>  Proxy s -> AWQuery i ss (SegmentS s)
  runUpdate :: (ValidSegments ss, ValidEventName ss n, MonadIO m) => AWState i ss -> Event n -> m (EventResult n)
  runUpdateC :: (ValidSegments ss, All (ValidEventName ss) (firstN ': ns), MonadIO m) => AWState i ss -> EventC (firstN ': ns) -> m (NP Event (firstN ': ns), EventResult firstN)
  runQuery :: (MonadIO m) => AWState i ss -> AWQuery i ss a -> m a
  liftQuery :: AWQuery i ss a -> AWUpdate i ss a









class ( AcidWorldState i
      , Monad (AWUpdate i ss)
      , Monad (AWQuery i ss)
      , ValidSegments ss)
      => ValidAcidWorldState i ss

instance ( AcidWorldState i
      , Monad (AWUpdate i ss)
      , Monad (AWQuery i ss)
      , ValidSegments ss)
      => ValidAcidWorldState i ss


askStateNp :: forall i ss. (ValidAcidWorldState i ss) => AWQuery i ss (NP V.ElField (ToSegmentFields ss))
askStateNp = sequence'_NP segsNp
  where
    segsNp :: NP (AWQuery i ss :.: V.ElField) (ToSegmentFields ss)
    segsNp = cmap_NP (Proxy :: Proxy (SegmentFetching ss)) askSegmentFromProxy proxyNp
    askSegmentFromProxy :: forall sField. (SegmentFetching ss sField) =>  Proxy sField -> (AWQuery i ss :.: V.ElField) sField
    askSegmentFromProxy _ =  Comp $  fmap V.Field $ askSegment (Proxy :: Proxy (V.Fst sField))
    proxyNp :: NP Proxy (ToSegmentFields ss)
    proxyNp = pure_NP Proxy

askSegmentsState :: forall i ss. (ValidAcidWorldState i ss) => AWQuery i ss (SegmentsState ss)
askSegmentsState = fmap npToSegmentsState askStateNp



data BackendHandles m ss nn = BackendHandles {
    bhLoadEvents :: forall i. MonadIO m => m (ConduitT i (Either Text (WrappedEvent ss nn)) (ResourceT IO) ()),
    bhGetLastCheckpointState :: MonadIO m => m ((Either Text (Maybe (SegmentsState ss))))
  }



{-
  Events and basic event utilities
-}

data EventC :: [k] -> * where
  EventC :: Event n -> EventC '[n]
  (:<<) :: (EventResult firstN -> Event n) -> EventC (firstN ': ns) -> EventC (n ': (firstN ': ns))

runEventC :: forall ss firstN ns i. (All (ValidEventName ss) (firstN ': ns), ValidAcidWorldState i ss) => EventC (firstN ': ns) -> AWUpdate i ss (NP Event (firstN ': ns), EventResult firstN)
runEventC (EventC (e@(Event xs) :: Event n)) = do
  r <- runEvent (Proxy :: Proxy n) xs
  pure (e :* Nil, r)
runEventC ((:<<) f ec) = do
  (npRest, r) <- runEventC ec
  case f r of
    (e@(Event xs) :: Event n) -> do
      fr <- runEvent (Proxy :: Proxy n) xs
      pure (e :* npRest, fr)



class (Eventable n, HasSegments ss (EventSegments n)) => ValidEventName ss (n :: Symbol)
instance (Eventable n, HasSegments ss (EventSegments n)) => ValidEventName ss n

class (ElemOrErr n nn, Eventable n, HasSegments ss (EventSegments n)) => IsValidEvent ss nn (n :: Symbol)
instance (ElemOrErr n nn, Eventable n, HasSegments ss (EventSegments n)) => IsValidEvent ss nn n



type ValidEventNames ss nn = (All (ValidEventName ss) nn, UniqueElementsWithErr nn ~ 'True)





-- representing the relationship between n xs and r
type EventableR n xs r =
  (Eventable n, EventArgs n ~ xs, EventResult n ~ r)



class (ToUniqueText n, SListI (EventArgs n), All Eq (EventArgs n), All Show (EventArgs n)) => Eventable (n :: k) where
  type EventArgs n :: [*]
  type EventResult n :: *
  type EventSegments n :: [Symbol]
  runEvent :: (ValidAcidWorldState i ss, HasSegments ss (EventSegments n)) => Proxy n -> EventArgsContainer (EventArgs n) -> AWUpdate i ss (EventResult n)



newtype EventArgsContainer xs = EventArgsContainer {eventArgsContainerNp ::  NP I xs}

instance (All Show xs) => Show (EventArgsContainer xs) where
  show (EventArgsContainer np) = L.intercalate ", " $ cfoldMap_NP (Proxy :: Proxy Show) ((:[]) . show . unI) np

instance (All Eq xs) => Eq (EventArgsContainer xs) where
  (==) (EventArgsContainer np1) (EventArgsContainer np2) = and . collapse_NP $ czipWith_NP (Proxy :: Proxy Eq) (\ia ib -> K $ ia == ib) np1 np2




newtype EventId = EventId{uuidFromEventId :: UUID.UUID} deriving(Show, Eq, ToJSON, FromJSON)

data Event (n :: k) where
  Event :: (Eventable n, EventArgs n ~ xs, All Eq xs, All Show xs) => EventArgsContainer xs -> Event n


instance Show (Event n) where
  show (Event c) = "Event :: " ++ (T.unpack $ toUniqueText (Proxy :: Proxy n)) ++ "\n with args::" ++ show c


instance Eq (Event n) where
  (==) (Event c) (Event c1) = c == c1


toRunEvent :: NPCurried ts a -> EventArgsContainer ts -> a
toRunEvent f  = npIUncurry f . eventArgsContainerNp

mkEvent :: forall n xs r. (NPCurry xs, EventableR n xs r) => Proxy n -> NPCurried xs (Event n)
mkEvent _  = npICurry (Event . EventArgsContainer :: NP I xs -> Event n)


data StorableEvent ss nn n = StorableEvent {
    storableEventTime :: Time.UTCTime,
    storableEventId :: EventId,
    storableEventEvent :: Event n
  } deriving (Eq, Show)

mkStorableEvents :: forall m ns ss nn. (MonadIO m, SListI ns) => NP Event ns -> m (NP (StorableEvent ss nn) ns)
mkStorableEvents np = sequence'_NP stCompNp
  where
    stCompNp :: NP (m :.: StorableEvent ss nn) ns
    stCompNp = map_NP (Comp . mkStorableEvent) np

mkStorableEvent :: (MonadIO m) => Event n -> m (StorableEvent ss nn n)
mkStorableEvent e = do
  t <- Time.getCurrentTime
  uuid <- liftIO $ UUID.nextRandom
  return $ StorableEvent t (EventId uuid) e

data WrappedEvent ss nn where
  WrappedEvent :: (HasSegments ss (EventSegments n)) => StorableEvent ss nn n -> WrappedEvent ss nn

instance Show (WrappedEvent ss nn) where
  show (WrappedEvent se) = "WrappedEvent: " <> show se

runWrappedEvent :: ValidAcidWorldState i ss => WrappedEvent ss e -> AWUpdate i ss ()
runWrappedEvent (WrappedEvent (StorableEvent _ _ (Event xs :: Event n))) = void $ runEvent (Proxy :: Proxy n) xs




