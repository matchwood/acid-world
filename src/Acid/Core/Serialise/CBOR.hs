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

type CBOREventParser ss nn = (IDecode RealWorld (WrappedEvent ss nn))
instance AcidSerialiseEvent AcidSerialiserCBOR where
  data AcidSerialiseEventOptions AcidSerialiserCBOR = AcidSerialiserCBOROptions
  type AcidSerialiseParser AcidSerialiserCBOR ss nn = CBOREventParser ss nn
  type AcidSerialiseT AcidSerialiserCBOR = BL.ByteString
  type AcidSerialiseConduitT AcidSerialiserCBOR = BS.ByteString
  serialiserFileExtension _ = ".cbor"
  serialiseStorableEvent o se = addCRC $ toLazyByteString $ serialiseCBOREvent o se
  deserialiseStorableEvent o t = (left (T.pack . show)) . decodeOrFail (deserialiseCBOREvent o) =<< checkAndConsumeCRC t
  makeDeserialiseParsers _ _ _ = makeCBORParsers
  deserialiseEventStream :: forall ss nn m. (MonadIO m) => AcidSerialiseEventOptions AcidSerialiserCBOR -> AcidSerialiseParsers AcidSerialiserCBOR ss nn -> (ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ())
  deserialiseEventStream  _ ps = do
    pFinder <- liftIO $ stToIO (CBOR.Read.deserialiseIncremental $ findCBORParserForWrappedEventIO ps)
    connectEitherConduit checkSumConduit $ runLoop pFinder
    where
      runLoop :: IDecode RealWorld (CBOREventParser ss nn) -> ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ()
      runLoop pFinder = loop (Left pFinder) =<< await
        where
          loop :: (Either (IDecode RealWorld (CBOREventParser ss nn)) (CBOREventParser ss nn)) -> Maybe BS.ByteString ->  ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ()
          loop (Left pfinderRes) t = do
            case pfinderRes of
              (CBOR.Read.Done bs _ eParser) -> loop (Right eParser) (Just bs)
              (CBOR.Read.Fail _ _ err) -> yield (Left . T.pack $ "Could not deserialise an event name for finding a parser: " <> show err) >> (loop (Left pFinder) =<< await)
              (CBOR.Read.Partial k) -> do
                case t of
                  Nothing -> pure () -- no more values in the stream
                  Just _ -> do
                    nextRes <- liftIO . stToIO . k $ t
                    loop (Left nextRes) =<< await
          loop (Right res) t = do
            case res of
              (CBOR.Read.Done bs _ e) -> yield (Right e) >> (if BS.null bs then (loop (Left pFinder)) =<< await else loop (Left pFinder) (Just bs))
              (CBOR.Read.Fail _ _ err) -> yield (Left . T.pack . show $ err) >> (loop (Left pFinder) =<< await)
              (CBOR.Read.Partial k) -> do
                nextRes <- liftIO . stToIO . k $ t
                loop (Right nextRes) =<< await


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


findCBORParserForWrappedEventIO :: forall ss nn s. AcidSerialiseParsers AcidSerialiserCBOR ss nn -> Decoder s (CBOREventParser ss nn)
findCBORParserForWrappedEventIO ps = do
  name <- decode
  case HM.lookup name ps of
    Nothing -> fail $ "Could not find parser for event named " <> (T.unpack name)
    Just p -> pure p



makeCBORParsers :: forall ss nn m. (MonadIO m, All (CanSerialiseCBOR ss) nn) => m (AcidSerialiseParsers AcidSerialiserCBOR ss nn)
makeCBORParsers = do
  wres <- sequence $ cfoldMap_NP (Proxy :: Proxy (CanSerialiseCBOR ss)) (\p -> [toTaggedTuple p]) proxyRec
  pure $ HM.fromList wres
  where
    proxyRec :: NP Proxy nn
    proxyRec = pure_NP Proxy
    toTaggedTuple :: (CanSerialiseCBOR ss n) => Proxy n -> m (Text, CBOREventParser ss nn)
    toTaggedTuple p = do
      r <-decodeWrappedEventCBOR p
      pure (toUniqueText p, r)


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


decodeWrappedEventCBOR ::forall m n ss nn. (MonadIO m, CanSerialiseCBOR ss n) => Proxy n -> m (CBOREventParser ss nn)
decodeWrappedEventCBOR _ = liftIO . stToIO $ CBOR.Read.deserialiseIncremental doDeserialiseWrappedEvent
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