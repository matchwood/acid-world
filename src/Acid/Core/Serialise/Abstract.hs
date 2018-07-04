
module Acid.Core.Serialise.Abstract  where


import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T

import GHC.TypeLits
import GHC.Exts (Constraint)

import Data.Proxy(Proxy(..))

import Acid.Core.State

import Conduit


{-

  This is a fiddly setup. The issue is that we want to abstract serialisation in a context where we are parsing to constrained types. The general approach here is to

-}
class AcidSerialiseEvent t where
  data AcidSerialiseEventOptions t :: *
  type AcidSerialiseT t :: *
  type AcidSerialiseParser t (ss :: [Symbol]) (nn :: [Symbol]) :: *
  serialiserName :: Proxy t -> Text
  serialiseEvent :: (AcidSerialiseConstraint t ss n) => AcidSerialiseEventOptions t -> StorableEvent ss nn n -> AcidSerialiseT t
  deserialiseEvent :: (AcidSerialiseConstraint t ss n) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> (Either Text (StorableEvent ss nn n))
  makeDeserialiseParsers :: (ValidEventNames ss nn, AcidSerialiseConstraintAll t ss nn) => AcidSerialiseEventOptions t -> Proxy ss -> Proxy nn -> AcidSerialiseParsers t ss nn
  deserialiseEventStream :: (Monad m) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> (ConduitT (AcidSerialiseT t) (Either Text (WrappedEvent ss nn)) (m) ())



class AcidSerialiseC t where
  type AcidSerialiseConstraint t (ss :: [Symbol]) (n :: Symbol) :: Constraint
  type AcidSerialiseConstraintAll t (ss :: [Symbol]) (nn :: [Symbol]) :: Constraint

type AcidSerialiseParsers t ss nn = HM.HashMap Text (AcidSerialiseParser t ss nn)

{-lookupParser :: (AcidSerialiseEvent t) =>  Proxy t -> Proxy ss -> Proxy nn -> Text -> AcidSerialiseParsers t ss nn -> Either Text (AcidSerialiseParser t ss nn)
lookupParser tp _ _ name ps =
  case HM.lookup name ps of
    Nothing -> Left $ "Could not find parser for event named " <>  name <> " for serialiser " <> serialiserName tp
    Just p -> pure p-}


deserialiseWrappedEvent :: forall t ss (nn :: [Symbol]). (AcidSerialiseEvent t, ValidEventNames ss nn, AcidSerialiseConstraintAll t ss nn) => AcidSerialiseEventOptions t -> AcidSerialiseT t -> Either Text (WrappedEvent ss nn)
deserialiseWrappedEvent o s = deserialiseWrappedEventWithParsers o (makeDeserialiseParsers o (Proxy :: Proxy ss) (Proxy :: Proxy nn)) s

deserialiseWrappedEventWithParsers :: (AcidSerialiseEvent t) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> AcidSerialiseT t -> Either Text (WrappedEvent ss nn)
deserialiseWrappedEventWithParsers o ps s =
  case runIdentity . runConduit $ yieldMany [s] .| (deserialiseEventStream o ps) .| sinkList of
    [] -> Left "Expected to deserialise one event, got none"
    [Left err] -> Left err
    [Right a] -> Right a
    xs -> Left $ "Expected to deserialise one event, got " <> (T.pack (show (length xs)))