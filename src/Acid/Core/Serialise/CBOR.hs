{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Acid.Core.Serialise.CBOR  where


{-
implementation of a cbor serialiser
-}
import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T

import Acid.Core.Serialise.Abstract
import qualified  RIO.ByteString.Lazy as BL
import qualified  RIO.ByteString as BS
import qualified Codec.CBOR.Read as CBOR.Read
import Codec.Serialise
import Codec.Serialise.Encoding
import Codec.Serialise.Decoding
import Generics.SOP
import Generics.SOP.NP
import qualified Data.UUID  as UUID

import Control.Monad.ST
import Conduit

import Acid.Core.Event

data AcidSerialiserCBOR


type CBOREventParser ss nn = (BS.ByteString -> Either Text (Maybe (BS.ByteString, WrappedEvent ss nn)))
instance AcidSerialiseEvent AcidSerialiserCBOR where
  data AcidSerialiseEventOptions AcidSerialiserCBOR = AcidSerialiserCBOROptions
  type AcidSerialiseT AcidSerialiserCBOR = BL.ByteString
  data AcidSerialiseParsers AcidSerialiserCBOR ss nn = AcidSerialiseParsersCBOR (HM.HashMap Text (CBOREventParser ss nn))
  acidSerialiseMakeParsers _ _ _ = makeCBORParsers
  acidSerialiseEvent _ se = serialise se
  acidDeserialiseEvents :: forall ss nn. AcidSerialiseEventOptions AcidSerialiserCBOR -> AcidSerialiseParsers AcidSerialiserCBOR ss nn -> (ConduitT BL.ByteString (Either Text (WrappedEvent ss nn)) (ResourceT IO) ())
  acidDeserialiseEvents  _ ps = mapC BL.toStrict .| start
    where
      start :: ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (ResourceT IO) ()
      start = await >>= maybe (return ()) (loop Nothing)
      loop :: (Maybe (CBOREventParser ss nn)) -> BS.ByteString ->  ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (ResourceT IO) ()
      loop Nothing t = do
        case findParserForWrappedEvent ps t of
          Left err -> yield (Left err) >> await >>= maybe (return ()) (loop Nothing)
          Right Nothing -> do
            mt <- await
            case mt of
              Nothing -> yield $  Left $ "Unexpected end of conduit values when still looking for parser"
              Just nt -> loop Nothing (t <> nt)
          Right (Just (bs, p)) -> (loop (Just p) bs)
      loop (Just p) t =
        case p t of
          Left err -> yield (Left err) >> await >>= maybe (return ()) (loop Nothing)
          Right Nothing -> do
            mt <- await
            case mt of
              Nothing -> yield $  Left $ "Unexpected end of conduit values when named event has only been partially parsed"
              Just nt -> loop (Just p) (t <> nt)
          Right (Just (bs, e)) -> yield (Right e) >> (if BS.null bs then  await >>= maybe (return ()) (loop Nothing) else loop Nothing bs)





class (All Serialise (EventArgs n)) => EventFromCBOR n
instance (All Serialise (EventArgs n)) => EventFromCBOR n

class (ValidEventName ss n, EventFromCBOR n) => ValidEventNameAndFromCBOR ss n
instance (ValidEventName ss n, EventFromCBOR n) => ValidEventNameAndFromCBOR ss n

instance AcidDeserialiseC AcidSerialiserCBOR ss nn where
  type AcidDeserialiseConstraint AcidSerialiserCBOR ss nn = All (ValidEventNameAndFromCBOR ss) nn



findParserForWrappedEvent :: forall ss nn. AcidSerialiseParsers AcidSerialiserCBOR ss nn -> BS.ByteString -> Either Text (Maybe (BS.ByteString, CBOREventParser ss nn))
findParserForWrappedEvent (AcidSerialiseParsersCBOR ps) t = runST (supplyCurrentInput =<< CBOR.Read.deserialiseIncremental deserialiseNameAndFindParser)
    where
      supplyCurrentInput :: IDecode s (CBOREventParser ss nn) -> ST s (Either Text (Maybe (BS.ByteString, CBOREventParser ss nn)))
      supplyCurrentInput (CBOR.Read.Partial k) = handleEndOfCurrentInput =<< k (Just t)
      supplyCurrentInput a = handleEndOfCurrentInput a

      handleEndOfCurrentInput :: IDecode s (CBOREventParser ss nn) -> ST s (Either Text (Maybe (BS.ByteString, CBOREventParser ss nn)))
      handleEndOfCurrentInput (CBOR.Read.Done bs _ x) = pure $ Right (Just (bs, x))
      handleEndOfCurrentInput (CBOR.Read.Partial _) = pure $ Right Nothing
      handleEndOfCurrentInput (CBOR.Read.Fail _ _ err) = pure $ Left (T.pack $ show err)
      deserialiseNameAndFindParser :: Decoder s (CBOREventParser ss nn)
      deserialiseNameAndFindParser = do
        len <- decodeListLen
        tag <- decodeWord
        case (len, tag) of
          (5, 0) -> do
            name <- decode
            case HM.lookup name ps of
              Nothing -> fail $ "Could not find parser for event named " <> T.unpack name
              Just p -> pure p
          _ -> fail "Expected a fixed length list of 5 and a '0' tag when decoding WrappedEvent"




makeCBORParsers :: forall ss nn. (All (ValidEventNameAndFromCBOR ss) nn) => AcidSerialiseParsers AcidSerialiserCBOR ss nn
makeCBORParsers =
  let (wres) = cfoldMap_NP (Proxy :: Proxy (ValidEventNameAndFromCBOR ss)) (\p -> [toTaggedTuple p]) proxyRec
  in AcidSerialiseParsersCBOR $ HM.fromList wres
  where
    proxyRec :: NP Proxy nn
    proxyRec = pure_NP Proxy
    toTaggedTuple :: (ValidEventName ss n, EventFromCBOR n) => Proxy n -> (Text, CBOREventParser ss nn)
    toTaggedTuple p = (toUniqueText p, decodeWrappedEventCBOR p)


decodeWrappedEventCBOR :: forall n ss nn. (ValidEventName ss n, EventFromCBOR n) => Proxy n -> CBOREventParser ss nn
decodeWrappedEventCBOR _ t = runST (supplyCurrentInput =<< CBOR.Read.deserialiseIncremental deserialiseKnownEvent)
    where
      supplyCurrentInput :: IDecode s (WrappedEvent ss nn) -> ST s (Either Text (Maybe (BS.ByteString, WrappedEvent ss nn)))
      supplyCurrentInput (CBOR.Read.Partial k) = handleEndOfCurrentInput =<< k (Just t)
      supplyCurrentInput a = handleEndOfCurrentInput a

      handleEndOfCurrentInput :: IDecode s (WrappedEvent ss nn) -> ST s (Either Text (Maybe (BS.ByteString, WrappedEvent ss nn)))
      handleEndOfCurrentInput (CBOR.Read.Done bs _ x) = pure $ Right (Just (bs, x))
      handleEndOfCurrentInput (CBOR.Read.Partial _) = pure $ Right Nothing
      handleEndOfCurrentInput (CBOR.Read.Fail _ _ err) = pure $ Left (T.pack $ show err)
      deserialiseKnownEvent :: Decoder s (WrappedEvent ss nn)
      deserialiseKnownEvent = do
        ((WrappedEventT wr) :: (WrappedEventT ss nn n))  <- decode
        pure wr


instance AcidSerialiseC AcidSerialiserCBOR n where
  type AcidSerialiseConstraint AcidSerialiserCBOR n = (All Serialise (EventArgs n))

instance (All Serialise (EventArgs n)) => Serialise (StorableEvent ss nn n) where
  encode (StorableEvent t ui (Event xs :: Event n)) =
    encodeListLen 5 <>
    encodeWord 0 <>
    encode (toUniqueText (Proxy :: Proxy n)) <>
    encode t <>
    encode ui <>
    encode xs
  decode = fail "Storable events cannot be decoded - they have to be decoded to WrappedEvents"



instance Serialise EventId where
  encode = encodeBytes . BL.toStrict . UUID.toByteString . uuidFromEventId
  decode = do
    bs <- fmap BL.fromStrict decodeBytes
    case UUID.fromByteString bs of
      Nothing -> fail $ "Could not parse UUID from bytestring " <> show bs
      Just uuid -> pure . EventId $ uuid


instance (ValidEventNameAndFromCBOR ss n, EventArgs n ~ xs) => Serialise (WrappedEventT ss nn n) where
  encode = fail "WrappedEventTs cannot be encoded, use a StorableEvent instead"
  decode = do

    t <- decode
    uid <- decode
    args <- decode

    return $ WrappedEventT (WrappedEvent t uid ((Event args) :: Event n))


instance (All Serialise xs) => Serialise (EventArgsContainer xs) where
  encode (EventArgsContainer np) = encodeListLenIndef <>
    cfoldMap_NP (Proxy :: Proxy Serialise) (encode . unI) np <>
    encodeBreak
  decode = do
    _ <- decodeListLenIndef
    np <- npIFromCBOR
    fin <- decodeBreakOr
    if fin
      then pure $ EventArgsContainer np
      else fail "Expected to find a break token to mark the end of ListLenIndef when decoding EventArgsContainer"

npIFromCBOR :: forall s xs. (All Serialise xs) => Decoder s (NP I xs)
npIFromCBOR =
  case sList :: SList xs of
    SNil   -> pure $ Nil
    SCons -> do
      a <- decode
      r <- npIFromCBOR
      pure $ I a :* r