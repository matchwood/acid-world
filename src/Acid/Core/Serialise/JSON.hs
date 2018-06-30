{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Acid.Core.Serialise.JSON where

import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T
import qualified  RIO.ByteString.Lazy as BL
import qualified  RIO.Vector as V

import Control.Arrow (left)
import Generics.SOP
import Generics.SOP.NP


import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import Data.Aeson(FromJSON(..), ToJSON(..), Value(..), Object)


import Acid.Core.Event
import Acid.Core.Serialise.Abstract
import Conduit

{-
implementation of a json serialiser
-}
data AcidSerialiserJSON

instance AcidSerialiseEvent AcidSerialiserJSON where
  data AcidSerialiseEventOptions AcidSerialiserJSON = AcidSerialiserJSONOptions
  type AcidSerialiseT AcidSerialiserJSON = BL.ByteString
  data AcidSerialiseParsers AcidSerialiserJSON ss nn = AcidSerialiseParsersJSON (HM.HashMap Text (Object -> Either Text (WrappedEvent ss nn)))
  acidSerialiseMakeParsers _ _ _ = makeJSONParsers
  acidSerialiseEvent _ se = (Aeson.encode se) <> "\n"
  acidDeserialiseEvents :: forall ss nn. AcidSerialiseEventOptions AcidSerialiserJSON -> AcidSerialiseParsers AcidSerialiserJSON ss nn -> (ConduitT BL.ByteString (Either Text (WrappedEvent ss nn)) (ResourceT IO) ())
  acidDeserialiseEvents  _ (AcidSerialiseParsersJSON ps) =
        linesUnboundedAsciiC .|
          mapC deserialiser
    where
      deserialiser :: BL.ByteString -> Either Text (WrappedEvent ss nn )
      deserialiser bs = do
        (hm :: HM.HashMap Text Value) <- left (T.pack) $ Aeson.eitherDecode' bs
        case HM.lookup "n" hm of
          Just (String s) -> do
            case HM.lookup s ps of
              Nothing -> fail $ "Could not find parser for event named " <> show s
              Just p -> p hm
          _ -> fail $ "Expected to find a text n key in value " <> show hm

instance AcidSerialiseC AcidSerialiserJSON n where
  type AcidSerialiseConstraint AcidSerialiserJSON n = (All ToJSON (EventArgs n), All FromJSON (EventArgs n))

class (All FromJSON (EventArgs n)) => EventFromJSON n
instance (All FromJSON (EventArgs n)) => EventFromJSON n

class (ValidEventName ss n, EventFromJSON n) => ValidEventNameAndFromJSON ss n
instance (ValidEventName ss n, EventFromJSON n) => ValidEventNameAndFromJSON ss n


instance AcidDeserialiseC AcidSerialiserJSON ss nn where
  type AcidDeserialiseConstraint AcidSerialiserJSON ss nn = All (ValidEventNameAndFromJSON ss) nn




makeJSONParsers :: forall ss nn. (All (ValidEventNameAndFromJSON ss) nn) => AcidSerialiseParsers AcidSerialiserJSON ss nn
makeJSONParsers =
  let (wres) = cfoldMap_NP (Proxy :: Proxy (ValidEventNameAndFromJSON ss)) (\p -> [toTaggedTuple p]) proxyRec
  in AcidSerialiseParsersJSON $ HM.fromList wres
  where
    proxyRec :: NP Proxy nn
    proxyRec = pure_NP Proxy
    toTaggedTuple :: (ValidEventName ss n, EventFromJSON n) => Proxy n -> (Text, Object -> Either Text (WrappedEvent ss nn))
    toTaggedTuple p = (toUniqueText p, decodeWrappedEventJSON p)

decodeWrappedEventJSON :: forall n ss nn. (ValidEventName ss n, EventFromJSON n) => Proxy n -> Object -> Either Text (WrappedEvent ss nn)
decodeWrappedEventJSON _ hm = do
  ((WrappedEventT wr) :: (WrappedEventT ss nn n))  <- fromJSONEither (Object hm)
  pure $ wr

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


instance (ValidEventNameAndFromJSON ss n, EventArgs n ~ xs) => FromJSON (WrappedEventT ss nn n) where
  parseJSON = Aeson.withObject "WrappedEventT" $ \o -> do
    t <- o Aeson..: "t"
    uid <- o Aeson..: "i"
    args <- o Aeson..: "a"

    return $ WrappedEventT (WrappedEvent t uid ((Event args) :: Event n))

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