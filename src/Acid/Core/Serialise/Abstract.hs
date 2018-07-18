
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Serialise.Abstract  where


import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T

import Generics.SOP
import GHC.TypeLits
import GHC.Exts (Constraint)

import Data.Proxy(Proxy(..))

import Acid.Core.Utils
import Acid.Core.Segment
import Acid.Core.State
import Data.Typeable
import qualified  Data.Vinyl.TypeLevel as V

import Conduit


{-

  This is a fiddly setup. The issue is that we want to abstract serialisation in a context where we are parsing to constrained types. The general approach is to construct a text indexed map of parsers specialised to a specific type and serialise the text key along with the event data
-}
class AcidSerialiseEvent (t :: k) where
  data AcidSerialiseEventOptions t :: *
  type AcidSerialiseT t :: *
  type AcidSerialiseParser t (ss :: [Symbol]) (nn :: [Symbol]) :: *
  serialiserName :: Proxy t -> Text
  default serialiserName :: (Typeable t) => Proxy t -> Text
  serialiserName p = T.pack $ (showsTypeRep . typeRep $ p) ""
  serialiseEvent :: (AcidSerialiseConstraint t ss n) => AcidSerialiseEventOptions t -> StorableEvent ss nn n -> AcidSerialiseT t
  deserialiseEvent :: (AcidSerialiseConstraint t ss n) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> (Either Text (StorableEvent ss nn n))

  makeDeserialiseParsers :: (ValidEventNames ss nn, AcidSerialiseConstraintAll t ss nn) => AcidSerialiseEventOptions t -> Proxy ss -> Proxy nn -> AcidSerialiseParsers t ss nn
  deserialiseEventStream :: (Monad m) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> (ConduitT (AcidSerialiseT t) (Either Text (WrappedEvent ss nn)) (m) ())


class AcidSerialiseSegment (t :: k) seg where
  serialiseSegment :: AcidSerialiseEventOptions t -> seg -> AcidSerialiseT t
  deserialiseSegment :: AcidSerialiseEventOptions t -> AcidSerialiseT t -> Either Text seg


class AcidSerialiseC t where
  type AcidSerialiseConstraint t (ss :: [Symbol]) (n :: Symbol) :: Constraint
  type AcidSerialiseConstraintAll t (ss :: [Symbol]) (nn :: [Symbol]) :: Constraint


class (AcidSerialiseSegment t (SegmentS fieldName), ToUniqueText fieldName) => AcidSerialiseSegmentNameConstraint t fieldName
instance (AcidSerialiseSegment t (SegmentS fieldName), ToUniqueText fieldName) => AcidSerialiseSegmentNameConstraint t fieldName


class (AcidSerialiseSegment t (V.Snd field), ToUniqueText (V.Fst field), KnownSymbol (V.Fst field)) => AcidSerialiseSegmentFieldConstraint t field
instance (AcidSerialiseSegment t (V.Snd field), ToUniqueText (V.Fst field), KnownSymbol (V.Fst field)) => AcidSerialiseSegmentFieldConstraint t field




class (a ~ b, KnownSegmentField a, SegmentFetching ss a, AcidSerialiseSegmentFieldConstraint t a) => SegmentFieldToSegmentFieldSerialise ss t a b
instance (a ~ b, KnownSegmentField a, SegmentFetching ss a, AcidSerialiseSegmentFieldConstraint t a) => SegmentFieldToSegmentFieldSerialise ss t a b

type ValidSegmentsSerialise t ss = (All (AcidSerialiseSegmentFieldConstraint t) (ToSegmentFields ss), ValidSegments ss, AllZip (SegmentFieldToSegmentFieldSerialise ss t) (ToSegmentFields ss) (ToSegmentFields ss))



type AcidSerialiseParsers t ss nn = HM.HashMap Text (AcidSerialiseParser t ss nn)


deserialiseWrappedEvent :: forall t ss (nn :: [Symbol]). (AcidSerialiseEvent t, ValidEventNames ss nn, AcidSerialiseConstraintAll t ss nn) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> Either Text (WrappedEvent ss nn)
deserialiseWrappedEvent o s = deserialiseWrappedEventWithParsers o (makeDeserialiseParsers o (Proxy :: Proxy ss) (Proxy :: Proxy nn)) s

deserialiseWrappedEventWithParsers :: (AcidSerialiseEvent t) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> AcidSerialiseT t -> Either Text (WrappedEvent ss nn)
deserialiseWrappedEventWithParsers o ps s =
  case runIdentity . runConduit $ yieldMany [s] .| (deserialiseEventStream o ps) .| sinkList of
    [] -> Left "Expected to deserialise one event, got none"
    [Left err] -> Left err
    [Right a] -> Right a
    xs -> Left $ "Expected to deserialise one event, got " <> (T.pack (show (length xs)))