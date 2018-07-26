{-# LANGUAGE UndecidableSuperClasses #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Acid.Core.Serialise.Postgresql where

import RIO
import Generics.SOP
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.ToRow
import Data.Aeson(ToJSON(..))
import Acid.Core.Serialise.JSON()

import Acid.Core.State
import Acid.Core.Utils
import Acid.Core.Serialise.Abstract

data PostgresRow where
  PostgresRow :: ToRow a => a -> PostgresRow

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
  type AcidSerialiseConduitT AcidSerialiserPostgresql = [PostgresRow]
  type AcidSerialiseParser AcidSerialiserPostgresql ss nn = ()
  tConversions _ = (id, id)


  serialiseStorableEvent _ n = [PostgresRow n]
  deserialiseStorableEvent = error "deserialiseStorableEvent"
  makeDeserialiseParsers = error "makeDeserialiseParsers"
  deserialiseEventStream = error "deserialiseEventStream"


class (ValidEventName ss n, All ToJSON (EventArgs n)) => CanSerialisePostgesql ss n
instance (ValidEventName ss n, All ToJSON (EventArgs n)) => CanSerialisePostgesql ss n


instance AcidSerialiseC AcidSerialiserPostgresql where
  type AcidSerialiseConstraintP AcidSerialiserPostgresql ss = CanSerialisePostgesql ss

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
    [toField (toUniqueText (Proxy :: Proxy n)), toField t, toField ui, toField $ toJSON xs]