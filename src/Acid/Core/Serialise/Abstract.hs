
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Serialise.Abstract  where


import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T
import qualified  RIO.ByteString as BS
import qualified  RIO.ByteString.Lazy as BL

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
import Control.Monad.ST.Trans


{-

  This is a fiddly setup. The issue is that we want to abstract serialisation in a context where we are parsing to constrained types. The general approach is to construct a text indexed map of parsers specialised to a specific type and serialise the text key along with the event data
-}
class AcidSerialiseEvent (t :: k) where
  data AcidSerialiseEventOptions t :: *
  type AcidSerialiseT t :: *
  type AcidSerialiseConduitT t :: *

  type AcidSerialiseParser t (ss :: [Symbol]) (nn :: [Symbol]) :: *

  tConversions :: AcidSerialiseEventOptions t -> (AcidSerialiseT t -> AcidSerialiseConduitT t, AcidSerialiseConduitT t -> AcidSerialiseT t)
  default tConversions :: (AcidSerialiseT t ~ BL.ByteString, AcidSerialiseConduitT t ~ BS.ByteString) => AcidSerialiseEventOptions t -> (AcidSerialiseT t -> AcidSerialiseConduitT t, AcidSerialiseConduitT t -> AcidSerialiseT t)
  tConversions _ = (BL.toStrict, BL.fromStrict)
  serialiserName :: Proxy t -> Text
  default serialiserName :: (Typeable t) => Proxy t -> Text
  serialiserName p = T.pack $ (showsTypeRep . typeRep $ p) ""
  serialiseEvent :: (AcidSerialiseConstraint t ss n) => AcidSerialiseEventOptions t -> StorableEvent ss nn n -> AcidSerialiseT t
  deserialiseEvent :: (AcidSerialiseConstraint t ss n) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> (Either Text (StorableEvent ss nn n))

  makeDeserialiseParsers :: (ValidEventNames ss nn, AcidSerialiseConstraintAll t ss nn) => AcidSerialiseEventOptions t -> Proxy ss -> Proxy nn -> AcidSerialiseParsers t ss nn
  deserialiseEventStream :: (Monad m) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> (ConduitT (AcidSerialiseConduitT t) (Either Text (WrappedEvent ss nn)) (m) ())

toConduitType :: (AcidSerialiseEvent t) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> AcidSerialiseConduitT t
toConduitType = fst . tConversions

fromConduitType :: (AcidSerialiseEvent t) => AcidSerialiseEventOptions t -> AcidSerialiseConduitT t -> AcidSerialiseT t
fromConduitType = snd . tConversions

class AcidSerialiseSegment (t :: k) seg where
  serialiseSegment :: (Monad m) => AcidSerialiseEventOptions t -> seg -> ConduitT i (AcidSerialiseConduitT t) m ()
  deserialiseSegment :: (Monad m) => AcidSerialiseEventOptions t -> ConduitT (AcidSerialiseConduitT t) o (STT s m) (Either Text seg)


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

newtype PartialParser s a = PartialParser {extractPartialParser :: (s -> Either Text (Either (PartialParser s a) (s, a)))}

runPartialParser :: PartialParser s a -> s ->  Either Text (Either (PartialParser s a) (s, a))
runPartialParser = extractPartialParser

type PartialParserBS a = PartialParser BS.ByteString a

fmapPartialParser :: (a -> Either Text b) -> PartialParser s a -> PartialParser s b
fmapPartialParser f p = PartialParser $ \t -> do
  res <- (extractPartialParser p) t
  case res of
    (Left newP) -> pure (Left $ fmapPartialParser f newP)
    (Right (s, a)) -> do
      b <- f a
      pure $ Right (s, b)

deserialiseEventStreamWithPartialParser :: forall ss nn m. (Monad m) => PartialParserBS (PartialParserBS (WrappedEvent ss nn)) -> (ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ())
deserialiseEventStreamWithPartialParser initialParser = awaitForever (loop (Left initialParser))
  where
    loop :: (Either (PartialParserBS (PartialParserBS (WrappedEvent ss nn))) (PartialParserBS (WrappedEvent ss nn))) -> BS.ByteString ->  ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ()
    loop (Left p) t = do
      case runPartialParser p t of
        Left err -> yield (Left err) >> awaitForever (loop (Left p))
        Right (Left newParser) -> do
          mt <- await
          case mt of
            Nothing -> yield $  Left $ "Unexpected end of conduit values when still looking for parser"
            Just nt -> loop (Left newParser) nt
        Right (Right (bs, foundP)) -> (loop (Right foundP) bs)
    loop (Right p) t =
      case runPartialParser p t of
        Left err -> yield (Left err) >> awaitForever (loop (Left initialParser))
        Right (Left newParser) -> do
          mt <- await
          case mt of
            Nothing -> yield $  Left $ "Unexpected end of conduit values when named event has only been partially parsed"
            Just nt -> loop (Right newParser) nt
        Right (Right (bs, e)) -> yield (Right e) >> (if BS.null bs then  awaitForever (loop (Left initialParser)) else loop (Left initialParser) bs)

deserialiseWrappedEvent :: forall t ss (nn :: [Symbol]). (AcidSerialiseEvent t, ValidEventNames ss nn, AcidSerialiseConstraintAll t ss nn) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> Either Text (WrappedEvent ss nn)
deserialiseWrappedEvent o s = deserialiseWrappedEventWithParsers o (makeDeserialiseParsers o (Proxy :: Proxy ss) (Proxy :: Proxy nn)) (toConduitType o s)

deserialiseWrappedEventWithParsers :: (AcidSerialiseEvent t) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> AcidSerialiseConduitT t -> Either Text (WrappedEvent ss nn)
deserialiseWrappedEventWithParsers o ps s =
  case runIdentity . runConduit $ yieldMany [s] .| (deserialiseEventStream o ps) .| sinkList of
    [] -> Left "Expected to deserialise one event, got none"
    [Left err] -> Left err
    [Right a] -> Right a
    xs -> Left $ "Expected to deserialise one event, got " <> (T.pack (show (length xs)))