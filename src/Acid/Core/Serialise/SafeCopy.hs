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


import Acid.Core.State
import Acid.Core.Serialise.Abstract
import Conduit

{-
implementation of a json serialiser
-}
data AcidSerialiserSafeCopy

type SafeCopyEventParser ss nn = (BS.ByteString -> Either Text (Maybe (BS.ByteString, WrappedEvent ss nn)))

instance AcidSerialiseEvent AcidSerialiserSafeCopy where
  data AcidSerialiseEventOptions AcidSerialiserSafeCopy = AcidSerialiserSafeCopyOptions
  type AcidSerialiseT AcidSerialiserSafeCopy = BL.ByteString
  type AcidSerialiseParser AcidSerialiserSafeCopy ss nn = SafeCopyEventParser ss nn
  serialiseEvent o se = runPutLazy $ serialiseSafeCopyEvent o se
  deserialiseEvent o t = left (T.pack) $ runGetLazy (deserialiseSafeCopyEvent o) t
  makeDeserialiseParsers _ _ _ = makeSafeCopyParsers
  deserialiseEventStream :: forall ss nn m. (Monad m) => AcidSerialiseEventOptions AcidSerialiserSafeCopy -> AcidSerialiseParsers AcidSerialiserSafeCopy ss nn -> (ConduitT BL.ByteString (Either Text (WrappedEvent ss nn)) (m) ())
  deserialiseEventStream  _ ps = mapC BL.toStrict .| start
    where
      start :: ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ()
      start = await >>= maybe (return ()) (loop Nothing)
      loop :: (Maybe (SafeCopyEventParser ss nn)) -> BS.ByteString ->  ConduitT BS.ByteString (Either Text (WrappedEvent ss nn)) (m) ()
      loop Nothing t = do
        case findSafeCopyParserForWrappedEvent ps t of
          Left err -> yield (Left err) >> await >>= maybe (return ()) (loop Nothing)
          Right Nothing -> do
            mt <- await
            case mt of
              Nothing -> yield $  Left $ "Unexpected end of conduit values when still looking for parser"
              Just nt -> loop Nothing (t <> nt)
          Right (Just (bs, p)) -> (loop (Just p) bs)
      loop (Just p) t =
        case p t of
          Left err -> yield (Left err) >> await >>= maybe (return ()) (loop Nothing)
          Right Nothing -> do
            mt <- await
            case mt of
              Nothing -> yield $  Left $ "Unexpected end of conduit values when named event has only been partially parsed"
              Just nt -> loop (Just p) (t <> nt)
          Right (Just (bs, e)) -> yield (Right e) >> (if BS.null bs then  await >>= maybe (return ()) (loop Nothing) else loop Nothing bs)


findSafeCopyParserForWrappedEvent :: forall ss nn. AcidSerialiseParsers AcidSerialiserSafeCopy ss nn -> BS.ByteString -> Either Text (Maybe (BS.ByteString, SafeCopyEventParser ss nn))
findSafeCopyParserForWrappedEvent ps t =
  case runGetPartial safeGet t of
    Fail err _ -> Left . T.pack $ err
    Partial _ -> Right Nothing
    Done n bs -> do
      let eName = (T.decodeUtf8' n)
      case eName of
        Left err -> Left $ "Could not decode event name" <> T.pack (show err)
        Right name ->
          case HM.lookup name ps of
            Just p -> Right (Just (bs, p))
            Nothing -> Left $ "Could not find parser for event named " <> name


class (ValidEventName ss n, All SafeCopy (EventArgs n)) => CanSerialiseSafeCopy ss n
instance (ValidEventName ss n, All SafeCopy (EventArgs n)) => CanSerialiseSafeCopy ss n

instance AcidSerialiseC AcidSerialiserSafeCopy where
  type AcidSerialiseConstraint AcidSerialiserSafeCopy ss n = CanSerialiseSafeCopy ss n
  type AcidSerialiseConstraintAll AcidSerialiserSafeCopy ss nn = All (CanSerialiseSafeCopy ss) nn




makeSafeCopyParsers :: forall ss nn. (All (CanSerialiseSafeCopy ss) nn) => AcidSerialiseParsers AcidSerialiserSafeCopy ss nn
makeSafeCopyParsers =
  let (wres) = cfoldMap_NP (Proxy :: Proxy (CanSerialiseSafeCopy ss)) (\p -> [toTaggedTuple p]) proxyRec
  in HM.fromList wres
  where
    proxyRec :: NP Proxy nn
    proxyRec = pure_NP Proxy
    toTaggedTuple :: (CanSerialiseSafeCopy ss n) => Proxy n -> (Text, SafeCopyEventParser ss nn)
    toTaggedTuple p = (toUniqueText p, decodeWrappedEventSafeCopy p)

decodeWrappedEventSafeCopy :: forall n ss nn. (CanSerialiseSafeCopy ss n) => Proxy n -> SafeCopyEventParser ss nn
decodeWrappedEventSafeCopy _ t =
  case runGetPartial safeGet t of
    Fail err _ -> Left . T.pack $ err
    Partial _ -> Right Nothing
    Done (se :: StorableEvent ss nn n) bs -> Right (Just (bs, WrappedEvent se))


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
