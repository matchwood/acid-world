
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
  acidSerialiseMakeParsers :: (ValidEventNames ss nn, AcidDeserialiseConstraint t ss nn) => AcidSerialiseEventOptions t -> Proxy ss -> Proxy nn -> AcidSerialiseParsers t ss nn

  acidSerialiseEvent :: (AcidSerialiseConstraint t n) => AcidSerialiseEventOptions t -> StorableEvent ss nn n -> AcidSerialiseT t
  acidDeserialiseEvents :: AcidSerialiseEventOptions t -> AcidSerialiseParsers t ss nn -> (ConduitT (AcidSerialiseT t) (Either Text (WrappedEvent ss nn)) (ResourceT IO) ())

class AcidSerialiseC t n where
  type AcidSerialiseConstraint t n :: Constraint


class AcidDeserialiseC t (ss :: [Symbol]) (nn :: [Symbol]) where
  type AcidDeserialiseConstraint t ss nn :: Constraint


