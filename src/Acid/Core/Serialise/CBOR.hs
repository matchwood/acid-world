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
import qualified  RIO.List as L

import Acid.Core.Serialise.Abstract
import qualified  RIO.ByteString.Lazy as BL
import qualified  Data.ByteString.Lazy.Internal as BL
import qualified  RIO.ByteString as BS
import qualified Codec.CBOR.Read as CBOR.Read
import Codec.Serialise
import Codec.Serialise.Encoding
import Codec.Serialise.Decoding
import Codec.CBOR.Write (toLazyByteString)
import Generics.SOP
import Generics.SOP.NP
import qualified Data.UUID  as UUID
import Control.Arrow (left)
import Control.Monad.ST
import Conduit

import Acid.Core.Utils
import Acid.Core.State

data AcidSerialiserCBOR

-- unfortunately we can't use partial parsing as effectively here as with safecopy - the use of the ST monad in CBOR seems to make it impossible to provide a way to loop a partially applied parser due to the (forall s. ) constraint of runST. so instead of partial parsing we loop and provide concatted bytestrings until we get a result. An alternative might be to use stToIO but it would require deserialiseEventStream to wrap a MonadIO m monad
type CBOREventParser ss nn = (BS.ByteString -> Either Text (Maybe (BS.ByteString, WrappedEvent ss nn)))
instance AcidSerialiseEvent AcidSerialiserCBOR where
  data AcidSerialiseEventOptions AcidSerialiserCBOR = AcidSerialiserCBOROptions
  type AcidSerialiseParser AcidSerialiserCBOR ss nn = CBOREventParser ss nn
  type AcidSerialiseT AcidSerialiserCBOR = BL.ByteString
  type AcidSerialiseConduitT AcidSerialiserCBOR = BS.ByteString
  serialiserFileExtension _ = ".cbor"
  serialiseStorableEvent o se = addCRC $ toLazyByteString $ serialiseCBOREvent o se
  deserialiseStorableEvent o t = (left (T.pack . show)) . decodeOrFail (deserialiseCBOREvent o) =<< checkAndConsumeCRC t
  makeDeserialiseParsers _ _ _ = makeCBORParsers
  deserialiseEventStream :: forall ss nn m. (Monad m) => AcidSerialiseEventOptions AcidSerialiserCBOR -> AcidSerialiseParsers AcidSerialiserCBOR ss nn -> (ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ())
  deserialiseEventStream  _ ps = connectEitherConduit checkSumConduit $ awaitForever (loop Nothing)
    where
      loop :: (Maybe (CBOREventParser ss nn)) -> BS.ByteString ->  ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ()
      loop Nothing t = do
        case findCBORParserForWrappedEvent ps t of
          Left err -> yield (Left err) >> awaitForever (loop Nothing)
          Right Nothing -> do
            mt <- await
            case mt of
              Nothing -> yield $  Left $ "Unexpected end of conduit values when still looking for parser"
              Just nt -> loop Nothing (t <> nt)
          Right (Just (bs, p)) -> (loop (Just p) bs)
      loop (Just p) t =
        case p t of
          Left err -> yield (Left err) >> awaitForever (loop Nothing)
          Right Nothing -> do
            mt <- await
            case mt of
              Nothing -> yield $  Left $ "Unexpected end of conduit values when named event has only been partially parsed"
              Just nt -> loop (Just p) (t <> nt)
          Right (Just (bs, e)) -> yield (Right e) >> (if BS.null bs then  awaitForever (loop Nothing) else loop Nothing bs)





class (ValidEventName ss n, All Serialise (EventArgs n)) => CanSerialiseCBOR ss n
instance (ValidEventName ss n, All Serialise (EventArgs n)) => CanSerialiseCBOR ss n


instance AcidSerialiseC AcidSerialiserCBOR where
  type AcidSerialiseConstraintP AcidSerialiserCBOR ss = CanSerialiseCBOR ss


instance (Serialise seg) => AcidSerialiseSegment AcidSerialiserCBOR seg where
  serialiseSegment _ seg = sourceLazy $ toLazyByteString $ encode seg
  deserialiseSegment _ = do
    sio <- liftIO $ stToIO deserialiseIncremental
    loop sio
    where
      loop :: MonadIO m => IDecode RealWorld seg -> ConduitT BS.ByteString o m (Either Text seg)
      loop res =
        case res of
          (CBOR.Read.Done _ _ x) -> pure . Right $ x
          (CBOR.Read.Fail _ _ err) -> pure $ Left (T.pack $ show err)
          (CBOR.Read.Partial k) -> loop =<< liftIO . stToIO . k =<< await


findCBORParserForWrappedEvent :: forall ss nn. AcidSerialiseParsers AcidSerialiserCBOR ss nn -> BS.ByteString -> Either Text (Maybe (BS.ByteString, CBOREventParser ss nn))
findCBORParserForWrappedEvent ps t =
  case runST $ decodePartial decode t of
    Left err -> Left err
    Right Nothing -> Right Nothing
    Right (Just (bs, name)) ->
      case HM.lookup name ps of
        Nothing -> Left $ "Could not find parser for event named " <> name
        Just p -> Right . Just $ (bs, p)



makeCBORParsers :: forall ss nn. (All (CanSerialiseCBOR ss) nn) => AcidSerialiseParsers AcidSerialiserCBOR ss nn
makeCBORParsers =
  let (wres) = cfoldMap_NP (Proxy :: Proxy (CanSerialiseCBOR ss)) (\p -> [toTaggedTuple p]) proxyRec
  in HM.fromList wres
  where
    proxyRec :: NP Proxy nn
    proxyRec = pure_NP Proxy
    toTaggedTuple :: (CanSerialiseCBOR ss n) => Proxy n -> (Text, CBOREventParser ss nn)
    toTaggedTuple p = (toUniqueText p, decodeWrappedEventCBOR p)


decodePartial :: Decoder s a ->  BS.ByteString -> ST s (Either Text (Maybe (BS.ByteString, a)))
decodePartial decoder t = (supplyCurrentInput =<< CBOR.Read.deserialiseIncremental decoder)
  where
    supplyCurrentInput :: IDecode s (a) -> ST s (Either Text (Maybe (BS.ByteString, a)))
    supplyCurrentInput (CBOR.Read.Partial k) = handleEndOfCurrentInput =<< k (Just t)
    supplyCurrentInput a = handleEndOfCurrentInput a
    handleEndOfCurrentInput :: IDecode s a -> ST s (Either Text (Maybe (BS.ByteString, a)))
    handleEndOfCurrentInput (CBOR.Read.Done bs _ x) = pure $ Right (Just (bs, x))
    handleEndOfCurrentInput (CBOR.Read.Partial _) = pure $ Right Nothing
    handleEndOfCurrentInput (CBOR.Read.Fail _ _ err) = pure $ Left (T.pack $ show err)



decodeOrFail :: (forall s. Decoder s a) -> BL.ByteString -> Either CBOR.Read.DeserialiseFailure a
decodeOrFail decoder bs0 =
    runST (supplyAllInput bs0 =<< CBOR.Read.deserialiseIncremental decoder)
  where
    supplyAllInput _bs (CBOR.Read.Done _ _ x) = return (Right x)
    supplyAllInput  bs (CBOR.Read.Partial k)  =
      case bs of
        BL.Chunk chunk bs' -> k (Just chunk) >>= supplyAllInput bs'
        BL.Empty           -> k Nothing      >>= supplyAllInput BL.Empty
    supplyAllInput _ (CBOR.Read.Fail _ _ exn) = return (Left exn)


decodeWrappedEventCBOR :: forall n ss nn. (CanSerialiseCBOR ss n) => Proxy n -> CBOREventParser ss nn
decodeWrappedEventCBOR _ t = runST $ decodePartial doDeserialiseWrappedEvent t
  where
    doDeserialiseWrappedEvent :: Decoder s (WrappedEvent ss nn)
    doDeserialiseWrappedEvent = do
        (se :: StorableEvent ss nn n) <- decode
        pure $ WrappedEvent se

serialiseCBOREvent :: forall ss nn n. (CanSerialiseCBOR ss n) => AcidSerialiseEventOptions AcidSerialiserCBOR -> StorableEvent ss nn n -> Encoding
serialiseCBOREvent _ se = encode (toUniqueText (Proxy :: Proxy n)) <> encode se



deserialiseCBOREvent :: forall ss nn n s. (CanSerialiseCBOR ss n) => AcidSerialiseEventOptions AcidSerialiserCBOR -> Decoder s (StorableEvent ss nn n)
deserialiseCBOREvent _ = do
  (a :: Text) <- decode
  if a == toUniqueText (Proxy :: Proxy n)
    then decode
    else  fail $ "Expected " <> T.unpack (toUniqueText (Proxy :: Proxy n)) <> " when consuming prefix, but got " <> show a



instance (CanSerialiseCBOR ss n) => Serialise (StorableEvent ss nn n) where
  encode (StorableEvent t ui (Event xs :: Event n)) =
    encodeListLen 4 <>
    encodeWord 0 <>
    encode t <>
    encode ui <>
    encode xs
  decode = do
    len <- decodeListLen
    tag <- decodeWord
    case (len, tag) of
      (4, 0) -> do
        t <- decode
        uid <- decode
        args <- decode
        pure $ StorableEvent t uid ((Event args) :: Event n)
      (_,_) -> fail $ "Expected (listLength, tag) of (4, 0)" <> "but got (" <> L.intercalate ", " [show len, show tag] <> ")"


instance Serialise EventId where
  encode = encodeBytes . BL.toStrict . UUID.toByteString . uuidFromEventId
  decode = do
    bs <- fmap BL.fromStrict decodeBytes
    case UUID.fromByteString bs of
      Nothing -> fail $ "Could not parse UUID from bytestring " <> show bs
      Just uuid -> pure . EventId $ uuid

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