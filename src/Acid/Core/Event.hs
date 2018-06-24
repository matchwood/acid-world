
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Acid.Core.Event where
import RIO
import qualified  RIO.Text as T
import qualified  RIO.Time as Time

import Generics.SOP
import GHC.TypeLits


import qualified Data.UUID  as UUID
import qualified Data.UUID.V4  as UUID

import Acid.Core.Segment
import Acid.Core.Utils

-- this is the class that events run in

class (Monad (m ss)) => AcidWorldUpdateInner m ss where
  getSegment :: (HasSegment ss s) =>  Proxy s -> m ss (SegmentS s)
  putSegment :: (HasSegment ss s) =>  Proxy s -> (SegmentS s) -> m ss ()

class ToUniqueText (a :: k) where
  toUniqueText :: Proxy a -> Text

instance (KnownSymbol a) => ToUniqueText (a :: Symbol) where
  toUniqueText = T.pack . symbolVal







class (ElemOrErr n nn, Eventable n, HasSegments ss (EventSegments n)) => IsValidEvent ss nn n
instance (ElemOrErr n nn, Eventable n, HasSegments ss (EventSegments n)) => IsValidEvent ss nn n



class (Eventable n, HasSegments ss (EventSegments n)) => ValidEventName ss (n :: Symbol)
instance (Eventable n, HasSegments ss (EventSegments n)) => ValidEventName ss n

type ValidEventNames ss nn = All (ValidEventName ss) nn





-- representing the relationship between n xs and r
type EventableR n xs r =
  (Eventable n, EventArgs n ~ xs, EventResult n ~ r)



class (ToUniqueText n, SListI (EventArgs n)) => Eventable (n :: k) where
  type EventArgs n :: [*]
  type EventResult n :: *
  type EventSegments n :: [Symbol]
  runEvent :: (AcidWorldUpdateInner m ss, HasSegments ss (EventSegments n)) => Proxy n -> EventArgsContainer (EventArgs n) -> m ss (EventResult n)




newtype EventArgsContainer xs = EventArgsContainer {eventArgsContainerNp ::  NP I xs}

data Event (n :: k) where
  Event :: (Eventable n, EventArgs n ~ xs) => EventArgsContainer xs -> Event n

toRunEvent :: NPCurried ts a -> EventArgsContainer ts -> a
toRunEvent f  = npIUncurry f . eventArgsContainerNp

mkEvent :: forall n xs r. (NPCurry xs, EventableR n xs r) => Proxy n -> NPCurried xs (Event n)
mkEvent _  = npICurry (Event . EventArgsContainer :: NP I xs -> Event n)


data StorableEvent ss nn n = StorableEvent {
    storableEventTime :: Time.UTCTime,
    storableEventId :: UUID.UUID,
    storableEventEvent :: Event n
  }

mkStorableEvent :: (MonadIO m) => Event n -> m (StorableEvent ss nn n)
mkStorableEvent e = do
  t <- Time.getCurrentTime
  uuid <- liftIO $ UUID.nextRandom
  return $ StorableEvent t uuid e

data WrappedEvent ss nn where
  WrappedEvent :: (HasSegments ss (EventSegments n)) => {
    wrappedEventTime :: Time.UTCTime,
    wrappedEventId :: UUID.UUID,
    wrappedEventEvent :: Event n} -> WrappedEvent ss nn

newtype WrappedEventT ss nn n = WrappedEventT (WrappedEvent ss nn )


runWrappedEvent :: AcidWorldUpdateInner m ss => WrappedEvent ss e -> m ss ()
runWrappedEvent (WrappedEvent _ _ (Event xs :: Event n)) = void $ runEvent (Proxy :: Proxy n) xs




