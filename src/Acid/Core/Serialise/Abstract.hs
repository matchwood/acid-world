
module Acid.Core.Serialise.Abstract  where


import RIO

import GHC.TypeLits
import GHC.Exts (Constraint)

import Data.Proxy(Proxy(..))

import Acid.Core.Event

import Conduit


{-

  This is a fiddly setup. The issue is that we want to abstract serialisation in a context where we are parsing to constrained types. The general approach here is to

-}
class AcidSerialiseEvent t where
  data AcidSerialiseEventOptions t :: *
  type AcidSerialiseT t :: *
  data AcidSerialiseParsers t (ss :: [Symbol]) (nn :: [Symbol]) :: *
  serialiserName :: Proxy t -> Text
  acidSerialiseMakeParsers :: (ValidEventNames ss nn, AcidDeserialiseConstraint t ss nn) => AcidSerialiseEventOptions t -> Proxy ss -> Proxy nn -> AcidSerialiseParsers t ss nn

  acidSerialiseEvent :: (AcidSerialiseConstraint t ss nn n) => AcidSerialiseEventOptions t -> StorableEvent ss nn n -> AcidSerialiseT t
  acidDeserialiseEvents :: (Monad m) => AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> (ConduitT (AcidSerialiseT t) (Either Text (WrappedEvent ss nn)) (m) ())
  acidDeserialiseEvent :: AcidSerialiseEventOptions t -> AcidSerialiseT t -> (Either Text (StorableEvent ss nn n))

class AcidSerialiseC t (ss :: [Symbol]) (nn :: [Symbol]) (n :: Symbol) where
  type AcidSerialiseConstraint t ss nn n :: Constraint


class AcidDeserialiseC t (ss :: [Symbol]) (nn :: [Symbol]) where
  type AcidDeserialiseConstraint t ss nn :: Constraint

