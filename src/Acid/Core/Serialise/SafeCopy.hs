{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Acid.Core.Serialise.SafeCopy where

import RIO
import qualified  RIO.HashMap as HM
import qualified  RIO.Text as T
import qualified  RIO.ByteString as BS
import qualified  RIO.ByteString.Lazy as BL

import Control.Arrow (left)
import Generics.SOP
import Generics.SOP.NP
import Data.SafeCopy
import Data.SafeCopy.Internal hiding (Proxy)
import Data.Serialize
import qualified Data.UUID  as UUID


import Acid.Core.Utils
import Acid.Core.State
import Acid.Core.Serialise.Abstract
import Conduit

{-
implementation of a json serialiser
-}
data AcidSerialiserSafeCopy



instance AcidSerialiseEvent AcidSerialiserSafeCopy where
  data AcidSerialiseEventOptions AcidSerialiserSafeCopy = AcidSerialiserSafeCopyOptions
  type AcidSerialiseParser AcidSerialiserSafeCopy ss nn = PartialParserBS (WrappedEvent ss nn)
  type AcidSerialiseT AcidSerialiserSafeCopy = BL.ByteString
  type AcidSerialiseConduitT AcidSerialiserSafeCopy = BS.ByteString
  serialiseEvent o se = runPutLazy $ serialiseSafeCopyEvent o se
  deserialiseEvent o t = left (T.pack) $ runGetLazy (deserialiseSafeCopyEvent o) t
  makeDeserialiseParsers _ _ _ = makeSafeCopyParsers
  deserialiseEventStream :: forall ss nn m. (Monad m) => AcidSerialiseEventOptions AcidSerialiserSafeCopy -> AcidSerialiseParsers AcidSerialiserSafeCopy ss nn -> (ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ())
  deserialiseEventStream  _ ps = deserialiseEventStreamWithPartialParser (findSafeCopyParserForWrappedEvent ps)


safeCopyPartialParser :: (SafeCopy a) => PartialParserBS a
safeCopyPartialParser = PartialParser $ \t -> handleSafeCopyParserResult $ runGetPartial safeGet t

findSafeCopyParserForWrappedEvent :: forall ss nn. AcidSerialiseParsers AcidSerialiserSafeCopy ss nn -> PartialParserBS ( PartialParserBS (WrappedEvent ss nn))
findSafeCopyParserForWrappedEvent ps = fmapPartialParser findSafeCopyParser safeCopyPartialParser
  where
    findSafeCopyParser :: BS.ByteString -> Either Text (PartialParserBS (WrappedEvent ss nn))
    findSafeCopyParser n = do
      let eName = (T.decodeUtf8' n)
      case eName of
        Left err -> Left $ "Could not decode event name" <> T.pack (show err)
        Right name ->
          case HM.lookup name ps of
            Just p -> pure p
            Nothing -> Left $ "Could not find parser for event named " <> name





class (ValidEventName ss n, All SafeCopy (EventArgs n)) => CanSerialiseSafeCopy ss n
instance (ValidEventName ss n, All SafeCopy (EventArgs n)) => CanSerialiseSafeCopy ss n

instance AcidSerialiseC AcidSerialiserSafeCopy where
  type AcidSerialiseConstraint AcidSerialiserSafeCopy ss n = CanSerialiseSafeCopy ss n
  type AcidSerialiseConstraintAll AcidSerialiserSafeCopy ss nn = All (CanSerialiseSafeCopy ss) nn

instance (SafeCopy seg) => AcidSerialiseSegment AcidSerialiserSafeCopy seg where
  serialiseSegment _ seg = sourceLazy $ runPutLazy $ safePut seg
  deserialiseSegment _ = undefined --left (T.pack) $ runGetLazy safeGet bs


makeSafeCopyParsers :: forall ss nn. (All (CanSerialiseSafeCopy ss) nn) => AcidSerialiseParsers AcidSerialiserSafeCopy ss nn
makeSafeCopyParsers =
  let (wres) = cfoldMap_NP (Proxy :: Proxy (CanSerialiseSafeCopy ss)) (\p -> [toTaggedTuple p]) proxyRec
  in HM.fromList wres
  where
    proxyRec :: NP Proxy nn
    proxyRec = pure_NP Proxy
    toTaggedTuple :: (CanSerialiseSafeCopy ss n) => Proxy n -> (Text, PartialParserBS (WrappedEvent ss nn))
    toTaggedTuple p = (toUniqueText p, decodeWrappedEventSafeCopy p)

decodeWrappedEventSafeCopy :: forall n ss nn. (CanSerialiseSafeCopy ss n) => Proxy n -> PartialParserBS (WrappedEvent ss nn)
decodeWrappedEventSafeCopy _ = fmapPartialParser (pure . (WrappedEvent :: StorableEvent ss nn n -> WrappedEvent ss nn)) safeCopyPartialParser




handleSafeCopyParserResult :: Result a -> Either Text (Either (PartialParserBS a) (BS.ByteString, a))
handleSafeCopyParserResult (Fail err _) = Left . T.pack $ err
handleSafeCopyParserResult (Partial p) = Right . Left $ (PartialParser $ \bs -> handleSafeCopyParserResult $ p bs)
handleSafeCopyParserResult (Done a bs) = Right . Right $ (bs, a)


serialiseSafeCopyEvent :: forall ss nn n. (CanSerialiseSafeCopy ss n) => AcidSerialiseEventOptions AcidSerialiserSafeCopy -> StorableEvent ss nn n -> Put
serialiseSafeCopyEvent _ se = do
  safePut $ T.encodeUtf8 . toUniqueText $ (Proxy :: Proxy n)
  safePut $ se


deserialiseSafeCopyEvent :: forall ss nn n. (CanSerialiseSafeCopy ss n) => AcidSerialiseEventOptions AcidSerialiserSafeCopy -> Get (StorableEvent ss nn n)
deserialiseSafeCopyEvent _ = do
  (_ :: BS.ByteString) <- safeGet
  safeGet


instance (CanSerialiseSafeCopy ss n) => SafeCopy (StorableEvent ss nn n) where
  version = Version 0
  kind = Base
  errorTypeName _ = "StorableEvent ss nn " ++ (T.unpack $ toUniqueText (Proxy :: Proxy n))
  putCopy (StorableEvent t ui (Event xs)) = contain $ do
    safePut $ t
    safePut $ ui
    safePut xs

  getCopy = contain $ do
    t <- safeGet
    ui <- safeGet
    args <- safeGet
    pure $ StorableEvent t ui ((Event args) :: Event n)


instance SafeCopy EventId where
  version = Version 0
  kind = Base
  errorTypeName _ = "EventId"
  putCopy = contain . safePut . UUID.toByteString . uuidFromEventId
  getCopy = contain $ do
    bs <- safeGet
    case UUID.fromByteString bs of
      Nothing -> fail $ "Could not parse UUID from bytestring " <> show bs
      Just uuid -> pure . EventId $ uuid

instance (All SafeCopy xs) => SafeCopy (EventArgsContainer xs) where
  version = Version 0
  kind = Base
  errorTypeName _ = "EventArgsContainer xs"
  putCopy (EventArgsContainer np) = contain $ do
    sequence_ $ cfoldMap_NP (Proxy :: Proxy SafeCopy) ((:[]) . safePut . unI) np
  getCopy = contain $ do
    np <- npIFromSafeCopy
    pure $ EventArgsContainer np

npIFromSafeCopy :: forall xs. (All SafeCopy xs) => Get (NP I xs)
npIFromSafeCopy =
  case sList :: SList xs of
    SNil   -> pure $ Nil
    SCons -> do
      a <- safeGet
      r <- npIFromSafeCopy
      pure $ I a :* r
