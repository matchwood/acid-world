{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Acid.Core.Serialise.SafeCopy where

import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T
import qualified  RIO.ByteString.Lazy as BL
import qualified  RIO.Vector as V

import Control.Arrow (left)
import Generics.SOP
import Generics.SOP.NP
import Data.SafeCopy
import Data.SafeCopy.Internal hiding (Proxy)
import Data.Serialize
import Data.Serialize.Put
import Data.Serialize.Get


import Acid.Core.Event
import Acid.Core.Serialise.Abstract
import Conduit

{-
implementation of a json serialiser
-}
data AcidSerialiserSafeCopy


instance AcidSerialiseEvent AcidSerialiserSafeCopy where
  data AcidSerialiseEventOptions AcidSerialiserSafeCopy = AcidSerialiserSafeCopyOptions
  type AcidSerialiseT AcidSerialiserSafeCopy = BL.ByteString
  type AcidSerialiseParser AcidSerialiserSafeCopy ss nn = ()
  serialiserName _ = "SafeCopy"
  serialiseEvent _ se = runPutLazy . safePut $ se
  deserialiseEvent _ t = undefined
  makeDeserialiseParsers _ _ _ = undefined
  deserialiseEventStream :: forall ss nn m. (Monad m) => AcidSerialiseEventOptions AcidSerialiserSafeCopy -> AcidSerialiseParsers AcidSerialiserSafeCopy ss nn -> (ConduitT BL.ByteString (Either Text (WrappedEvent ss nn)) (m) ())
  deserialiseEventStream  _ _ = undefined




class (ValidEventName ss n, All SafeCopy (EventArgs n)) => CanSerialiseSafeCopy ss n
instance (ValidEventName ss n, All SafeCopy (EventArgs n)) => CanSerialiseSafeCopy ss n

instance AcidSerialiseC AcidSerialiserSafeCopy where
  type AcidSerialiseConstraint AcidSerialiserSafeCopy ss n = CanSerialiseSafeCopy ss n
  type AcidSerialiseConstraintAll AcidSerialiserSafeCopy ss nn = All (CanSerialiseSafeCopy ss) nn

instance (CanSerialiseSafeCopy ss n) => SafeCopy (StorableEvent ss nn n) where
  version = Version 0
  kind = Base
  errorTypeName _ = "StorableEvent ss nn " ++ (T.unpack $ toUniqueText (Proxy :: Proxy n))

instance (CanSerialiseSafeCopy ss n) => Serialize (StorableEvent ss nn n) where
  put (StorableEvent t ui (Event xs)) = do
    put $ toUniqueText (Proxy :: Proxy n)
  get = undefined
