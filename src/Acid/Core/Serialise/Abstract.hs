
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Serialise.Abstract  where


import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T
import qualified  RIO.ByteString as BS
import qualified  RIO.ByteString.Lazy as BL
import qualified  Data.ByteString.Lazy.Internal as BL

import Generics.SOP
import GHC.TypeLits
import GHC.Exts (Constraint)

import Data.Proxy(Proxy(..))

import Acid.Core.Segment
import Acid.Core.State
import Data.Typeable
import qualified  Data.Vinyl.TypeLevel as V
import Control.Arrow (left)
import Conduit
import Control.Monad.ST.Trans
import qualified  Data.Digest.CRC as CRC
import qualified  Data.Digest.CRC32 as CRC
import Data.ByteString.Builder
import Data.Serialize

{-

  This is a fiddly setup. The issue is that we want to abstract serialisation in a context where we are parsing to constrained types. The general approach is to construct a text indexed map of parsers specialised to a specific type and serialise the text key along with the event data
-}
class (Semigroup (AcidSerialiseT t), Monoid (AcidSerialiseT t)) => AcidSerialiseEvent (t :: k) where
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
  serialiseStorableEvent :: (AcidSerialiseConstraint t ss n) => AcidSerialiseEventOptions t -> StorableEvent ss nn n -> AcidSerialiseT t
  -- @todo we don't actually use this anywhere - perhaps just remmove it?
  deserialiseStorableEvent :: (AcidSerialiseConstraint t ss n) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> (Either Text (StorableEvent ss nn n))

  makeDeserialiseParsers :: (ValidEventNames ss nn, AcidSerialiseConstraintAll t ss nn) => AcidSerialiseEventOptions t -> Proxy ss -> Proxy nn -> AcidSerialiseParsers t ss nn
  deserialiseEventStream :: (Monad m) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> (ConduitT (AcidSerialiseConduitT t) (Either Text (WrappedEvent ss nn)) (m) ())

toConduitType :: (AcidSerialiseEvent t) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> AcidSerialiseConduitT t
toConduitType = fst . tConversions

fromConduitType :: (AcidSerialiseEvent t) => AcidSerialiseEventOptions t -> AcidSerialiseConduitT t -> AcidSerialiseT t
fromConduitType = snd . tConversions


serialiseEventNP :: (AcidSerialiseEvent t, AcidSerialiseConstraintAll t ss ns) =>  AcidSerialiseEventOptions t -> NP (StorableEvent ss nn) ns -> AcidSerialiseT t
serialiseEventNP _ Nil = mempty
serialiseEventNP t ((:*) se restNp) = serialiseEventNP t restNp <> serialiseStorableEvent t se

class AcidSerialiseSegment (t :: k) seg where
  serialiseSegment :: (Monad m) => AcidSerialiseEventOptions t -> seg -> ConduitT i (AcidSerialiseConduitT t) m ()
  -- we have to provide a solution here that allows proper incremental parsing, and the only way to do that with CBOR is to run the conduit with STT. It shouldn't make any difference to other serialisers.
  deserialiseSegment :: (Monad m) => AcidSerialiseEventOptions t -> ConduitT (AcidSerialiseConduitT t) o (STT s m) (Either Text seg)




addCRC :: BL.ByteString -> Builder
addCRC content =
  word64LE contentLength <>
  word32LE contentHash <>
  lazyByteString content
  where
    contentLength :: Word64
    contentLength = fromIntegral $ BL.length content
    contentHash :: Word32
    contentHash = CRC.crc32 $ lazyCRCDigest content

checkAndConsumeCRC :: Builder -> Either Text BL.ByteString
checkAndConsumeCRC b = do
  r <- left T.pack $ runGetLazy readWithCheckSum (toLazyByteString b)
  checkCRCLazy r

checkCRCLazy :: (CRC.CRC32, BL.ByteString) -> Either Text BL.ByteString
checkCRCLazy (check, content) =
  if check == lazyCRCDigest content
    then pure content
    else Left $ "Lazy bytestring failed checksum when reading"

checkCRCStrict :: (CRC.CRC32, BS.ByteString) -> Either Text BS.ByteString
checkCRCStrict (check, content) =
  if check == CRC.digest content
    then pure content
    else Left $ "Lazy bytestring failed checksum when reading"

-- we are using serialise here just because we are adapting this code from acid-state
readWithCheckSum :: Get (CRC.CRC32, BL.ByteString)
readWithCheckSum = do
  contentLength <- getWord64le
  contentChecksum <-getWord32le
  content <- getLazyByteString_fast (fromIntegral contentLength)
  pure (CRC.CRC32 contentChecksum, content)

readWithCheckSumStrict :: Get (CRC.CRC32, BS.ByteString)
readWithCheckSumStrict = do
  contentLength <- getWord64le
  contentChecksum <-getWord32le
  content <- getBytes (fromIntegral contentLength)
  pure (CRC.CRC32 contentChecksum, content)

-- this is directly copied from acid-state
-- | Read a lazy bytestring WITHOUT any copying or concatenation.
getLazyByteString_fast :: Int -> Get BL.ByteString
getLazyByteString_fast = worker 0 []
  where
    worker counter acc n = do
      remain <- remaining
      if n > remain then do
         chunk <- getBytes remain
         _ <- ensure 1
         worker (counter + remain) (chunk:acc) (n-remain)
      else do
         chunk <- getBytes n
         return $ BL.fromChunks (reverse $ chunk:acc)


handleDataDotSerializeParserResult :: Result a -> Either Text (Either (PartialParserBS a) (BS.ByteString, a))
handleDataDotSerializeParserResult (Fail err _) = Left . T.pack $ err
handleDataDotSerializeParserResult (Partial p) = Right . Left $ (PartialParser $ \bs -> handleDataDotSerializeParserResult $ p bs)
handleDataDotSerializeParserResult (Done a bs) = Right . Right $ (bs, a)


checkSumConduit :: (Monad m) => ConduitT BS.ByteString (Either Text BS.ByteString) m ()
checkSumConduit = deserialiseWithPartialParserTransformer $ fmapPartialParser checkCRCStrict $
  (PartialParser $ \t -> (handleDataDotSerializeParserResult $ runGetPartial readWithCheckSumStrict t))

lazyCRCDigest :: forall a. (CRC.CRC a) => BL.ByteString -> a
lazyCRCDigest = loop CRC.initCRC
  where
    loop :: a -> BL.ByteString -> a
    loop a (BL.Chunk chunk bs') = loop (CRC.updateDigest a chunk) bs'
    loop a (BL.Empty) = a


class AcidSerialiseC t where
  type AcidSerialiseConstraintP t (ss :: [Symbol]) :: Symbol -> Constraint

type AcidSerialiseConstraintAll t ss nn = All (AcidSerialiseConstraintP t ss) nn
type AcidSerialiseConstraint t ss n = AcidSerialiseConstraintP t ss n


class (AcidSerialiseSegment t (SegmentS fieldName), Segment fieldName) => AcidSerialiseSegmentNameConstraint t fieldName
instance (AcidSerialiseSegment t (SegmentS fieldName), Segment fieldName) => AcidSerialiseSegmentNameConstraint t fieldName


class (AcidSerialiseSegment t (V.Snd field), Segment (V.Fst field), KnownSymbol (V.Fst field)) => AcidSerialiseSegmentFieldConstraint t field
instance (AcidSerialiseSegment t (V.Snd field), Segment (V.Fst field), KnownSymbol (V.Fst field)) => AcidSerialiseSegmentFieldConstraint t field




class (SegmentFetching ss sField, AcidSerialiseSegmentFieldConstraint t sField) => SegmentFieldSerialise ss t sField
instance (SegmentFetching ss sField, AcidSerialiseSegmentFieldConstraint t sField) => SegmentFieldSerialise ss t sField

type ValidSegmentsSerialise t ss = (All (SegmentFieldSerialise ss t) (ToSegmentFields ss), All (AcidSerialiseSegmentFieldConstraint t) (ToSegmentFields ss), ValidSegments ss)



type AcidSerialiseParsers t ss nn = HM.HashMap Text (AcidSerialiseParser t ss nn)

newtype PartialParser s a = PartialParser {extractPartialParser :: (s -> Either Text (Either (PartialParser s a) (s, a)))}

runPartialParser :: PartialParser s a -> s ->  Either Text (Either (PartialParser s a) (s, a))
runPartialParser = extractPartialParser

runOrErrorPartialParser :: PartialParser s a -> s -> Either Text a
runOrErrorPartialParser pa s = do
  aRes <- runPartialParser pa s
  case aRes of
    Left _ -> Left $ "Not enough input for partial parser to consume"
    Right (_, a) -> pure a

consumeAndRunPartialParser :: PartialParser s a -> PartialParser s b -> s -> Either Text b
consumeAndRunPartialParser pa pb s = do
  aRes <- runPartialParser pa s
  case aRes of
    Left _ -> Left $ "Not enough input for partial parser to consume"
    Right (s2, _) -> runOrErrorPartialParser pb s2

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
-- @todo we may need some strictness annotations here - I'm not 100% sure how strictness works with conduit
deserialiseWithPartialParserSink :: forall a o m. (Monad m) => PartialParserBS a -> ConduitT BS.ByteString o m (Either Text a)
deserialiseWithPartialParserSink origParser = await >>= loop origParser
    where
      loop :: PartialParserBS a -> Maybe BS.ByteString -> ConduitT BS.ByteString o m (Either Text a)
      loop _ Nothing = pure . Left $ "No values received in conduit when trying to parse segment"
      loop p (Just t) = do
        case runPartialParser p t of
          Left err -> pure $ Left err
          Right (Left newParser) -> await >>= loop newParser
          Right (Right (_, a)) -> pure $ Right a


deserialiseWithPartialParserTransformer :: forall a m. (Monad m) => PartialParserBS a -> ConduitT BS.ByteString (Either Text a) m ()
deserialiseWithPartialParserTransformer origParser = awaitForever $ loop origParser
    where
      loop :: PartialParserBS a -> BS.ByteString -> ConduitT BS.ByteString (Either Text a) m ()
      loop p t = do
        case runPartialParser p t of
          Left err -> yield (Left err) >> awaitForever (loop origParser)
          Right (Left newParser) -> awaitForever $ loop newParser
          Right (Right (bs, a)) -> yield (Right a) >> (if BS.null bs then awaitForever (loop origParser) else loop origParser bs)


{-
this is a helper function for deserialising a single wrapped event - at the moment it is really just used for testing serialisation
-}
deserialiseWrappedEvent :: forall t ss (nn :: [Symbol]). (AcidSerialiseEvent t, ValidEventNames ss nn, AcidSerialiseConstraintAll t ss nn) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> Either Text (WrappedEvent ss nn)
deserialiseWrappedEvent o s = deserialiseWrappedEventWithParsers o (makeDeserialiseParsers o (Proxy :: Proxy ss) (Proxy :: Proxy nn)) (toConduitType o s)

deserialiseWrappedEventWithParsers :: (AcidSerialiseEvent t) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> AcidSerialiseConduitT t -> Either Text (WrappedEvent ss nn)
deserialiseWrappedEventWithParsers o ps s =
  case runIdentity . runConduit $ yieldMany [s] .| (deserialiseEventStream o ps) .| sinkList of
    [] -> Left "Expected to deserialise one event, got none"
    [Left err] -> Left err
    [Right a] -> Right a
    xs -> Left $ "Expected to deserialise one event, got " <> (T.pack (show (length xs)))