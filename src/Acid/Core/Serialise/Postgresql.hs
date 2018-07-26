{-# LANGUAGE UndecidableSuperClasses #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Acid.Core.Serialise.Postgresql where


import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T
import qualified  RIO.List.Partial as Partial

import Generics.SOP
import Generics.SOP.NP

import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.ToRow
import Database.PostgreSQL.Simple.Ok
import Data.Aeson(ToJSON(..), FromJSON(..))
import qualified Data.Aeson.Types as Aeson
import System.IO.Unsafe (unsafePerformIO)

import Conduit

import Acid.Core.Serialise.JSON()
import Acid.Core.State
import Acid.Core.Utils
import Acid.Core.Serialise.Abstract

data PostgresRow where
  PostgresRow :: ToRow a => a -> PostgresRow

type PostgresConduitT = [(Field, Maybe ByteString)]

instance ToRow PostgresRow where
  toRow (PostgresRow a) = toRow a


storableEventCreateTable :: Query
storableEventCreateTable = "CREATE TABLE storableevent (id SERIAL PRIMARY KEY, eventName Text NOT NULL, eventDate timestamptz NOT NULL, eventId uuid NOT NULL, eventArgs json NOT NULL, eventCheckpointed bool NOT NULL DEFAULT FALSE);"

data AcidSerialiserPostgresql


instance AcidSerialiseEvent AcidSerialiserPostgresql where
  data AcidSerialiseEventOptions AcidSerialiserPostgresql = AcidSerialiserPostgresqlOptions
  type AcidSerialiseT AcidSerialiserPostgresql = [PostgresRow]
  type AcidSerialiseConduitT AcidSerialiserPostgresql = PostgresConduitT
  type AcidSerialiseParser AcidSerialiserPostgresql ss nn = PostgresConduitT -> Conversion (WrappedEvent ss nn)
  tConversions _ = (undefined, undefined)


  serialiseStorableEvent _ n = [PostgresRow n]
  deserialiseStorableEvent = error "deserialiseStorableEvent"
  makeDeserialiseParsers _ _ _ = makePostgresParsers
  deserialiseEventStream :: forall ss nn m. (Monad m) => AcidSerialiseEventOptions AcidSerialiserPostgresql -> AcidSerialiseParsers AcidSerialiserPostgresql ss nn -> (ConduitT PostgresConduitT (Either Text (WrappedEvent ss nn)) (m) ())
  deserialiseEventStream _ ps = awaitForever loop
    where
      loop :: PostgresConduitT -> ConduitT PostgresConduitT (Either Text (WrappedEvent ss nn)) m ()
      loop fs = do
        en <- runConversionToEither (fromIdx fs 1)
        case en of
          Left err -> yield (Left err) >> awaitForever loop
          Right n ->
            case HM.lookup n ps of
              Nothing -> yield (Left $ "Could not find parser for event named " <> n) >> (awaitForever loop)
              Just p -> do
                we <- runConversionToEither (p fs)
                yield we >> (awaitForever loop)


class (ValidEventName ss n, All FromJSON (EventArgs n), All ToJSON (EventArgs n)) => CanSerialisePostgresql ss n
instance (ValidEventName ss n, All FromJSON (EventArgs n), All ToJSON (EventArgs n)) => CanSerialisePostgresql ss n

instance AcidSerialiseC AcidSerialiserPostgresql where
  type AcidSerialiseConstraintP AcidSerialiserPostgresql ss = CanSerialisePostgresql ss

instance (AcidSerialisePostgres a) => AcidSerialiseSegment AcidSerialiserPostgresql a where
  type AcidSerialiseSegmentT AcidSerialiserPostgresql = (PostgresRow)
  type AcidDeserialiseSegmentT AcidSerialiserPostgresql = PostgresConduitT
  serialiseSegment _ seg = yieldMany $ (toPostgresRows seg)

  deserialiseSegment _ = sinkList >>= \l -> runConversionToEither (fromPostgresConduitT l)

tableName :: (ToUniqueText a) => Proxy a -> String
tableName p = "app_" <>  (T.unpack . T.toLower $ (toUniqueText p))


class AcidSerialisePostgres a where
  toPostgresRows :: a -> [PostgresRow]
  createTable :: Proxy a -> Query
  fromPostgresConduitT :: [PostgresConduitT] -> Conversion a


runConversionToEither :: (Monad m) => Conversion a -> m (Either Text a)
runConversionToEither c = do
  -- horrific but this is just a poc
  eC <- pure $ unsafePerformIO $ (runConversion c) (error "Attempt to access db connection in runConversionToEither")
  case eC of
    Errors es -> pure . Left . showT $ es
    Ok a -> pure . Right $ a


instance FromField (Field, Maybe ByteString) where
  fromField f bs = pure (f, bs)




makePostgresParsers :: forall ss nn. (All (CanSerialisePostgresql ss) nn) => AcidSerialiseParsers AcidSerialiserPostgresql ss nn
makePostgresParsers =
  let (wres) = cfoldMap_NP (Proxy :: Proxy (CanSerialisePostgresql ss)) (\p -> [toTaggedTuple p]) proxyRec
  in HM.fromList wres
  where
    proxyRec :: NP Proxy nn
    proxyRec = pure_NP Proxy
    toTaggedTuple :: (CanSerialisePostgresql ss n) => Proxy n -> (Text, PostgresConduitT -> Conversion (WrappedEvent ss nn))
    toTaggedTuple p = (toUniqueText p, decodeWrappedEventPostgres p)

decodeWrappedEventPostgres :: forall n ss nn. (CanSerialisePostgresql ss n) => Proxy n -> PostgresConduitT -> Conversion (WrappedEvent ss nn)
decodeWrappedEventPostgres _ fs = do
  (se :: StorableEvent ss nn n) <- fromFields fs
  pure $ WrappedEvent se

class FromFields a where
  fromFields :: PostgresConduitT -> Conversion a


fromIdx :: (FromField a) => PostgresConduitT -> Int -> Conversion a
fromIdx fs i =
  let (f, bs) = fs Partial.!! i
  in fromField f bs

instance (CanSerialisePostgresql ss n, EventArgs n ~ xs) => FromFields (StorableEvent ss nn n) where
  fromFields fs = do
    t <- fromIdx fs 2
    uid <- fromIdx fs 3
    jsonV <- fromIdx fs 4
    case Aeson.parseEither parseJSON jsonV of
      Left err -> conversionError $ AWExceptionEventDeserialisationError (T.pack err)
      Right args -> return $ StorableEvent t (EventId uid) ((Event args) :: Event n)


{-  deserialiseStorableEvent :: (AcidSerialiseConstraint t ss n) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> (Either Text (StorableEvent ss nn n))

  makeDeserialiseParsers :: (ValidEventNames ss nn, AcidSerialiseConstraintAll t ss nn) => AcidSerialiseEventOptions t -> Proxy ss -> Proxy nn -> AcidSerialiseParsers t ss nn
  deserialiseEventStream :: (Monad m) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> (ConduitT (AcidSerialiseConduitT t) (Either Text (WrappedEvent ss nn)) (m) ())
-}


instance ToField EventId where
  toField = toField . uuidFromEventId

instance (All ToJSON (EventArgs n)) => ToRow (StorableEvent ss nn n) where
  toRow (StorableEvent t ui (Event xs :: Event n)) =
    [Plain "DEFAULT", toField (toUniqueText (Proxy :: Proxy n)), toField t, toField ui, toField $ toJSON xs]