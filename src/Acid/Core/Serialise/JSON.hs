{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Acid.Core.Serialise.JSON where

import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T
import qualified  RIO.ByteString.Lazy as BL
import qualified  RIO.ByteString as BS
import qualified  RIO.Vector as V

--import Control.Arrow (left)
import Generics.SOP
import Generics.SOP.NP


import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import Data.Aeson(FromJSON(..), ToJSON(..), Value(..))
import Data.Aeson.Internal (ifromJSON, IResult(..), formatError)
import Acid.Core.Utils
import Acid.Core.State
import Acid.Core.Serialise.Abstract
import Conduit
import qualified Data.Attoparsec.ByteString as Atto
import qualified Data.Attoparsec.ByteString.Lazy as Atto.L

{-
implementation of a json serialiser
-}
data AcidSerialiserJSON


{- Partial decoding for json -}
eitherPartialDecode' :: (FromJSON a) => BS.ByteString -> JSONResult a
eitherPartialDecode' = eitherPartialDecodeWith Aeson.json' ifromJSON

eitherPartialDecodeWith :: forall a. Atto.Parser Value -> (Value -> IResult a) -> BS.ByteString
                 -> JSONResult a
eitherPartialDecodeWith p tra s = handleRes (Atto.parse p s)
  where
    handleRes :: Atto.IResult BS.ByteString Value -> JSONResult a
    handleRes (Atto.Done i v) =
        case tra v of
          ISuccess a      -> JSONResultDone (i, a)
          IError path msg -> JSONResultFail (T.pack $ formatError path msg)
    handleRes (Atto.Partial np) = JSONResultPartial (handleRes . np)
    handleRes (Atto.Fail _ _ msg) = JSONResultFail (T.pack msg)

data JSONResult a =
    JSONResultFail Text
  | JSONResultPartial (BS.ByteString -> JSONResult a)
  | JSONResultDone (BS.ByteString, a)


handleJSONParserResult :: JSONResult a -> Either Text (Either (PartialParserBS a) (BS.ByteString, a))
handleJSONParserResult (JSONResultFail err) = Left $ err
handleJSONParserResult (JSONResultPartial p) = Right . Left $ (PartialParser $ \bs -> handleJSONParserResult $ p bs)
handleJSONParserResult (JSONResultDone (bs, a)) = Right . Right $ (bs, a)


{- Decoding with remainder - needed because of how we serialise event names -}
eitherDecodeLeftover :: (FromJSON a) => BL.ByteString -> Either Text (BL.ByteString, a)
eitherDecodeLeftover = eitherDecodeLeftoverWith Aeson.json' ifromJSON

eitherDecodeLeftoverWith :: Atto.L.Parser Value -> (Value -> IResult a) -> BL.ByteString
                 -> Either Text (BL.ByteString, a)
eitherDecodeLeftoverWith p tra s =
    case Atto.L.parse p s of
      Atto.L.Done bs v     -> case tra v of
                          ISuccess a      -> pure (bs, a)
                          IError path msg -> Left (T.pack $ formatError path msg)
      Atto.L.Fail _ _ msg -> Left (T.pack msg)

consumeAndParse :: forall a b. (FromJSON a, FromJSON b) => Proxy a -> BL.ByteString -> Either Text b
consumeAndParse _ bs = do
  (bs', (_ :: a)) <- eitherDecodeLeftover bs
  fmap snd $ eitherDecodeLeftover bs'


instance AcidSerialiseEvent AcidSerialiserJSON where
  data AcidSerialiseEventOptions AcidSerialiserJSON = AcidSerialiserJSONOptions
  type AcidSerialiseParser AcidSerialiserJSON ss nn = PartialParserBS (WrappedEvent ss nn)
  type AcidSerialiseT AcidSerialiserJSON = BL.ByteString
  type AcidSerialiseConduitT AcidSerialiserJSON = BS.ByteString
  -- due to the constraints of json (single top level object) we have a choice between some kind of separation between events (newline) or to simply write out the event name followed by the event. we choose the latter because it should be more efficient from a parsing perspective, and it allows a common interface with other parsers (SafeCopy)
  serialiseEvent :: forall ss nn n.(AcidSerialiseConstraint AcidSerialiserJSON ss n) => AcidSerialiseEventOptions AcidSerialiserJSON -> StorableEvent ss nn n -> BL.ByteString
  serialiseEvent _ se = Aeson.encode (toUniqueText (Proxy :: Proxy n)) <> (Aeson.encode se)
  deserialiseEvent _ = consumeAndParse (Proxy :: Proxy Text)
  makeDeserialiseParsers _ _ _ = makeJSONParsers
  deserialiseEventStream :: forall ss nn m. (Monad m) => AcidSerialiseEventOptions AcidSerialiserJSON -> AcidSerialiseParsers AcidSerialiserJSON ss nn -> (ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ())
  deserialiseEventStream  _ ps = deserialiseEventStreamWithPartialParser (findJSONParserForWrappedEvent ps)


jsonPartialParser :: (FromJSON a) => PartialParserBS a
jsonPartialParser = PartialParser $ \t -> handleJSONParserResult $ eitherPartialDecode' t

findJSONParserForWrappedEvent :: forall ss nn. AcidSerialiseParsers AcidSerialiserJSON ss nn -> PartialParserBS ( PartialParserBS (WrappedEvent ss nn))
findJSONParserForWrappedEvent ps = fmapPartialParser findJSONParser jsonPartialParser
  where
    findJSONParser :: Text -> Either Text (PartialParserBS (WrappedEvent ss nn))
    findJSONParser n = do
      case HM.lookup n ps of
        Just p -> pure p
        Nothing -> Left $ "Could not find parser for event named " <> n


class (ValidEventName ss n, All FromJSON (EventArgs n), All ToJSON (EventArgs n)) => CanSerialiseJSON ss n
instance (ValidEventName ss n, All FromJSON (EventArgs n), All ToJSON (EventArgs n)) => CanSerialiseJSON ss n

instance AcidSerialiseC AcidSerialiserJSON where
  type AcidSerialiseConstraintP AcidSerialiserJSON ss = CanSerialiseJSON ss

instance (ToJSON seg, FromJSON seg) => AcidSerialiseSegment AcidSerialiserJSON seg where
  serialiseSegment _ seg = sourceLazy $ Aeson.encode seg
  deserialiseSegment _ = deserialiseSegmentWithPartialParser jsonPartialParser



makeJSONParsers :: forall ss nn. (All (CanSerialiseJSON ss) nn) => AcidSerialiseParsers AcidSerialiserJSON ss nn
makeJSONParsers =
  let (wres) = cfoldMap_NP (Proxy :: Proxy (CanSerialiseJSON ss)) (\p -> [toTaggedTuple p]) proxyRec
  in HM.fromList wres
  where
    proxyRec :: NP Proxy nn
    proxyRec = pure_NP Proxy
    toTaggedTuple :: (CanSerialiseJSON ss n) => Proxy n -> (Text, PartialParserBS (WrappedEvent ss nn))
    toTaggedTuple p = (toUniqueText p, decodeWrappedEventJSON p)

decodeWrappedEventJSON :: forall n ss nn. (CanSerialiseJSON ss n) => Proxy n -> PartialParserBS (WrappedEvent ss nn)
decodeWrappedEventJSON _ = fmapPartialParser (pure . (WrappedEvent :: StorableEvent ss nn n -> WrappedEvent ss nn)) jsonPartialParser


fromJSONEither :: FromJSON a => Value -> Either Text a
fromJSONEither v =
  case Aeson.fromJSON v of
    (Aeson.Success a) -> pure a
    (Aeson.Error e) -> fail e



instance (All ToJSON (EventArgs n)) => ToJSON (StorableEvent ss nn n) where
  toJSON (StorableEvent t ui (Event xs :: Event n)) = Object $ HM.fromList [
    ("t", toJSON t),
    ("i", toJSON ui),
    ("a", toJSON xs)]

instance (All ToJSON xs) => ToJSON (EventArgsContainer xs) where
  toJSON (EventArgsContainer np) =
    toJSON $ collapse_NP $ cmap_NP (Proxy :: Proxy ToJSON) (K . toJSON . unI) np


instance (CanSerialiseJSON ss n, EventArgs n ~ xs) => FromJSON (StorableEvent ss nn n) where
  parseJSON = Aeson.withObject "WrappedEventT" $ \o -> do
    t <- o Aeson..: "t"
    uid <- o Aeson..: "i"
    args <- o Aeson..: "a"

    return $ StorableEvent t uid ((Event args) :: Event n)

instance (All FromJSON xs) => FromJSON (EventArgsContainer xs) where
  parseJSON = Aeson.withArray "EventArgsContainer" $ \v -> fmap EventArgsContainer $ npIFromJSON (V.toList v)

npIFromJSON :: forall xs. (All FromJSON xs) => [Value] -> Aeson.Parser (NP I xs)
npIFromJSON [] =
  case sList :: SList xs of
    SNil   -> pure Nil
    SCons  -> fail "No values left but still expecting a type"
npIFromJSON (v:vs) =
  case sList :: SList xs of
    SNil   -> fail "More values than expected"
    SCons -> do
      r <- npIFromJSON vs
      a <- Aeson.parseJSON v
      pure $ I a :* r