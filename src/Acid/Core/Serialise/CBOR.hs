module Acid.Core.Serialise.CBOR  where


{-
implementation of a cbor serialiser
-}
import RIO

import Acid.Core.Serialise.Abstract
import qualified  RIO.ByteString.Lazy as BL

data AcidSerialiserCBOR

instance AcidSerialiseEvent AcidSerialiserCBOR where
  data AcidSerialiseEventOptions AcidSerialiserCBOR = AcidSerialiserCBOROptions
  type AcidSerialiseT AcidSerialiserCBOR = BL.ByteString
  type AcidSerialiseParsers AcidSerialiserCBOR ss nn = ()
  acidSerialiseMakeParsers _ _ _ = ()
  acidSerialiseEvent _  _ = undefined
  acidDeserialiseEvent  _ _ = undefined

instance AcidSerialiseC AcidSerialiserCBOR n where
  type AcidSerialiseConstraint AcidSerialiserCBOR n = ()