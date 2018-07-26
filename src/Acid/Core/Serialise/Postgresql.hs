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
import Data.Aeson(ToJSON(..), FromJSON(..))
import qualified Data.Aeson as Aeson
import qualified Data.Aeson.Types as Aeson
import Acid.Core.Serialise.JSON()

import Acid.Core.State
import Acid.Core.Utils
import Acid.Core.Serialise.Abstract

data PostgresRow where
  PostgresRow :: ToRow a => a -> PostgresRow

type PostgresConduitT = [(Field, Maybe ByteString)]

instance ToRow PostgresRow where
  toRow (PostgresRow a) = toRow a

class ToPostgres a where
  createTable :: Proxy a -> Query

storableEventCreateTable :: Query
storableEventCreateTable = "CREATE TABLE storableevent (id SERIAL PRIMARY KEY, eventName Text NOT NULL, eventDate timestamptz NOT NULL, eventId uuid NOT NULL, eventArgs json NOT NULL);"

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
  deserialiseEventStream = error "deserialiseEventStream"


class (ValidEventName ss n, All FromJSON (EventArgs n), All ToJSON (EventArgs n)) => CanSerialisePostgresql ss n
instance (ValidEventName ss n, All FromJSON (EventArgs n), All ToJSON (EventArgs n)) => CanSerialisePostgresql ss n



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


instance AcidSerialiseC AcidSerialiserPostgresql where
  type AcidSerialiseConstraintP AcidSerialiserPostgresql ss = CanSerialisePostgresql ss

instance AcidSerialiseSegment AcidSerialiserPostgresql seg where
  serialiseSegment = error "serialiseSegment"

  deserialiseSegment = error "deserialiseSegment"


{-  deserialiseStorableEvent :: (AcidSerialiseConstraint t ss n) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> (Either Text (StorableEvent ss nn n))

  makeDeserialiseParsers :: (ValidEventNames ss nn, AcidSerialiseConstraintAll t ss nn) => AcidSerialiseEventOptions t -> Proxy ss -> Proxy nn -> AcidSerialiseParsers t ss nn
  deserialiseEventStream :: (Monad m) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> (ConduitT (AcidSerialiseConduitT t) (Either Text (WrappedEvent ss nn)) (m) ())
-}


instance ToField EventId where
  toField = toField . uuidFromEventId

instance (All ToJSON (EventArgs n)) => ToRow (StorableEvent ss nn n) where
  toRow (StorableEvent t ui (Event xs :: Event n)) =
    [Plain "DEFAULT", toField (toUniqueText (Proxy :: Proxy n)), toField t, toField ui, toField $ toJSON xs]