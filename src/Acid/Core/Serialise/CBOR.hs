module Acid.Core.Serialise.CBOR  where


{-
implementation of a cbor serialiser
-}
import RIO

import Acid.Core.Serialise.Abstract
import qualified  RIO.ByteString.Lazy as BL
import Codec.Serialise
import Codec.Serialise.Encoding
import Codec.Serialise.Decoding
import qualified  RIO.Map as Map
import Generics.SOP
import Generics.SOP.NP
import qualified Data.UUID  as UUID


import Acid.Core.Event

data AcidSerialiserCBOR

instance AcidSerialiseEvent AcidSerialiserCBOR where
  data AcidSerialiseEventOptions AcidSerialiserCBOR = AcidSerialiserCBOROptions
  type AcidSerialiseT AcidSerialiserCBOR = BL.ByteString
  type AcidSerialiseParsers AcidSerialiserCBOR ss nn = ()
  acidSerialiseMakeParsers _ _ _ = ()
  acidSerialiseEvent _ se = serialise se
  acidDeserialiseEvent  _ _ = undefined

instance AcidSerialiseC AcidSerialiserCBOR n where
  type AcidSerialiseConstraint AcidSerialiserCBOR n = (All Serialise (EventArgs n))

instance (All Serialise (EventArgs n)) => Serialise (StorableEvent ss nn n) where
  encode (StorableEvent t ui (Event xs :: Event n)) = encodeListLen 5 <> encodeWord 0 <> encode (toUniqueText (Proxy :: Proxy n)) <> encode t <> encode ui <> encode xs
  decode = fail "Storable events cannot be decoded - they have to be decoded to WrappedEvents"



instance Serialise UUID.UUID where
  encode = encodeBytes . BL.toStrict . UUID.toByteString
  decode = do
    bs <- fmap BL.fromStrict decodeBytes
    case UUID.fromByteString bs of
      Nothing -> fail $ "Could not parse UUID from bytestring " <> show bs
      Just uuid -> pure uuid