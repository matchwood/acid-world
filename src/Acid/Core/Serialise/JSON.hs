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

import Control.Arrow (left)
import Generics.SOP
import Generics.SOP.NP


import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import Data.Aeson(FromJSON(..), ToJSON(..), Value(..), Object)

import Acid.Core.Utils
import Acid.Core.State
import Acid.Core.Serialise.Abstract
import Conduit

{-
implementation of a json serialiser
-}
data AcidSerialiserJSON


instance AcidSerialiseEvent AcidSerialiserJSON where
  data AcidSerialiseEventOptions AcidSerialiserJSON = AcidSerialiserJSONOptions
  type AcidSerialiseT AcidSerialiserJSON = BS.ByteString
  type AcidSerialiseParser AcidSerialiserJSON ss nn = (Object -> Either Text (WrappedEvent ss nn))
  serialiseEvent _ se = BL.toStrict $ (Aeson.encode se) <> "\n"
  deserialiseEvent _ t = left (T.pack) $ (Aeson.eitherDecode' (BL.fromStrict t))
  makeDeserialiseParsers _ _ _ = makeJSONParsers
  deserialiseEventStream :: forall ss nn m. (Monad m) => AcidSerialiseEventOptions AcidSerialiserJSON -> AcidSerialiseParsers AcidSerialiserJSON ss nn -> (ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ())
  deserialiseEventStream  _ ps =
        linesUnboundedAsciiC .|
          mapC deserialiser
    where
      deserialiser :: BS.ByteString -> Either Text (WrappedEvent ss nn )
      deserialiser bs = do
        (hm :: HM.HashMap Text Value) <- left (T.pack) $ Aeson.eitherDecode' (BL.fromStrict bs)
        case HM.lookup "n" hm of
          Just (String s) -> do
            case HM.lookup s ps of
              Nothing -> fail $ "Could not find parser for event named " <> show s
              Just p -> p hm
          _ -> fail $ "Expected to find a text n key in value " <> show hm



class (ValidEventName ss n, All FromJSON (EventArgs n), All ToJSON (EventArgs n)) => CanSerialiseJSON ss n
instance (ValidEventName ss n, All FromJSON (EventArgs n), All ToJSON (EventArgs n)) => CanSerialiseJSON ss n

instance AcidSerialiseC AcidSerialiserJSON where
  type AcidSerialiseConstraint AcidSerialiserJSON ss n = CanSerialiseJSON ss n
  type AcidSerialiseConstraintAll AcidSerialiserJSON ss nn = All (CanSerialiseJSON ss) nn

instance (ToJSON seg) => AcidSerialiseSegment AcidSerialiserJSON seg where
  serialiseSegment _ seg = BL.toStrict $ (Aeson.encode seg)




makeJSONParsers :: forall ss nn. (All (CanSerialiseJSON ss) nn) => AcidSerialiseParsers AcidSerialiserJSON ss nn
makeJSONParsers =
  let (wres) = cfoldMap_NP (Proxy :: Proxy (CanSerialiseJSON ss)) (\p -> [toTaggedTuple p]) proxyRec
  in HM.fromList wres
  where
    proxyRec :: NP Proxy nn
    proxyRec = pure_NP Proxy
    toTaggedTuple :: (CanSerialiseJSON ss n) => Proxy n -> (Text, Object -> Either Text (WrappedEvent ss nn))
    toTaggedTuple p = (toUniqueText p, decodeWrappedEventJSON p)

decodeWrappedEventJSON :: forall n ss nn. (CanSerialiseJSON ss n) => Proxy n -> Object -> Either Text (WrappedEvent ss nn)
decodeWrappedEventJSON _ hm = do
  (se :: (StorableEvent ss nn n))  <- fromJSONEither (Object hm)
  pure $ WrappedEvent se

fromJSONEither :: FromJSON a => Value -> Either Text a
fromJSONEither v =
  case Aeson.fromJSON v of
    (Aeson.Success a) -> pure a
    (Aeson.Error e) -> fail e



instance (All ToJSON (EventArgs n)) => ToJSON (StorableEvent ss nn n) where
  toJSON (StorableEvent t ui (Event xs :: Event n)) = Object $ HM.fromList [
    ("n", toJSON (toUniqueText (Proxy :: Proxy n))),
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