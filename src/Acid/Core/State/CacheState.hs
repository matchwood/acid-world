{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Acid.Core.State.CacheState where

import RIO
import qualified RIO.HashMap as HM
import qualified RIO.ByteString.Lazy as BL
import qualified RIO.ByteString as BS
import qualified RIO.Text as T
import Database.LMDB.Simple
import qualified Control.Concurrent.STM  as STM
import qualified Control.Monad.State.Strict as St
import Generics.SOP
import Generics.SOP.NP
import Codec.Serialise
import Codec.Serialise.Encoding
import Codec.Serialise.Decoding
import qualified  Data.Vinyl.TypeLevel as V
import qualified  Data.Vinyl.Derived as V
import qualified  Data.Vinyl as V
import qualified Control.Concurrent.STM.TMVar as  TMVar

import GHC.TypeLits

import qualified Database.LMDB.Simple.Extra as LMDB
import Acid.Core.State.Abstract
import Acid.Core.Utils
import Acid.Core.Segment
import qualified Control.Monad.Reader as Re

import qualified Data.IxSet.Typed as IxSet


data CacheMode =
    CacheModeNone
  | CacheModeAll
  deriving (Eq, Show)

data SegmentCacheMode =
    SegmentCacheModeGlobal
  | SegmentCacheModeNone
  | SegmentCacheModeAll
  deriving (Eq, Show)

class (IsCMap (SegmentS segmentName), Segment segmentName) => SegmentC segmentName where
  segmentCacheMode :: Proxy segmentName -> SegmentCacheMode
  segmentCacheMode _ = SegmentCacheModeGlobal

class (SegmentC segmentName) => ValidCSegment segmentName
instance (SegmentC segmentName) => ValidCSegment segmentName


class (V.KnownField a, SegmentC (V.Fst a), SegmentDb (V.Fst a) ~ (V.Snd a)) => KnownSegmentDBField a
instance (V.KnownField a, SegmentC (V.Fst a), SegmentDb (V.Fst a) ~ (V.Snd a)) => KnownSegmentDBField a

class (IsCMap (V.Snd sf), SegmentC (V.Fst sf), KnownSymbol (V.Fst sf),  SegmentFetching ss sf) => ValidCSegmentField ss sf
instance (IsCMap (V.Snd sf), SegmentC (V.Fst sf), KnownSymbol (V.Fst sf),  SegmentFetching ss sf) => ValidCSegmentField ss sf

class (KnownSegmentDBField sf, ValidCSegment (V.Fst sf)) => ValidDBSegmentField ss sf
instance (KnownSegmentDBField sf, ValidCSegment (V.Fst sf)) => ValidDBSegmentField ss sf

type ValidSegmentsCacheState ss =
  (All ValidCSegment ss,
   ValidSegmentNames ss,
   V.NatToInt (V.RLength (ToSegmentDBFields ss)),
   ValidSegments ss,
   All (ValidCSegmentField ss) (ToSegmentFields ss),
   All (ValidDBSegmentField ss) (ToSegmentDBFields ss)
   )

class (V.HasField V.ARec s (ToSegmentDBFields segmentNames) (SegmentDb s), KnownSymbol s) => HasSegmentDb segmentNames s
instance (V.HasField V.ARec s (ToSegmentDBFields segmentNames) (SegmentDb s), KnownSymbol s) => HasSegmentDb segmentNames s

newtype SegmentDb s = SegmentDb {segmentDbDatabase :: (CacheMode, Database (CDBMapKey (SegmentS s)) (CDBMapValue (SegmentS s)))}

type family ToSegmentDBFields (segmentNames :: [Symbol]) = (segmentFields :: [(Symbol, *)]) where
  ToSegmentDBFields '[] = '[]
  ToSegmentDBFields (s ': ss) = '(s, SegmentDb s) ': ToSegmentDBFields ss

newtype SegmentsDb segmentNames = SegmentsDb {segmentsDbFieldRec :: V.AFieldRec (ToSegmentDBFields segmentNames)}

getSegmentDbP :: forall s ss. (HasSegmentDb ss s) => Proxy s ->  SegmentsDb ss -> SegmentDb s
getSegmentDbP _ (SegmentsDb fr) = V.getField $ V.rgetf (V.Label :: V.Label s) fr

npToSegmentsDb :: forall ss. (ValidSegmentsCacheState ss) => NP V.ElField (ToSegmentDBFields ss) -> SegmentsDb ss
npToSegmentsDb np = SegmentsDb $ (npToVinylARec id np)








class (Serialise (CDBMapKey a), Serialise (CDBMapValue a)) => IsCMap a where
  type CMapKey a
  type CMapValue a
  type CMapExpanded a
  type CDBMapKey a
  type CDBMapKey a = CMapKey a
  type CDBMapValue a
  type CDBMapValue a = CMapValue a
  insertMapC :: CacheMode -> CMapKey a ->  CMapValue a -> Database (CDBMapKey a) (CDBMapValue a) -> a -> Transaction ReadWrite (a)
  insertManyMapC :: CacheMode -> [(CMapKey a, CMapValue a)] ->  Database (CDBMapKey a) (CDBMapValue a) -> a -> Transaction ReadWrite (a)
  expandMap ::  Database (CDBMapKey a) (CDBMapValue a) -> a -> Transaction ReadOnly (CMapExpanded a)
  restoreMapC :: CacheMode -> Database (CDBMapKey a) (CDBMapValue a) -> Transaction ReadOnly a

{- cacheable hashmaps -}

data CVal a =
    CVal a
  | CValRef

instance (Serialise k, Serialise v, Eq k, Hashable k) => IsCMap (HM.HashMap k (CVal v)) where
  type CMapKey (HM.HashMap k (CVal v)) = k
  type CMapValue (HM.HashMap k (CVal v)) = v
  type CMapExpanded (HM.HashMap k (CVal v)) = HM.HashMap k v
  insertMapC cm k v db hm = do
    put db k (Just v)
    pure $ HM.insert k (toCachedCVal cm v) hm
  insertManyMapC cm vs db hm = do
    mapM_ (\(k, v) -> put db k (Just v)) vs
    pure $ foldl' (\s (k,v) -> HM.insert k (toCachedCVal cm v) s) hm vs

  expandMap db hm = sequence $ HM.mapWithKey expandVal hm
    where
      expandVal :: k -> (CVal v) -> Transaction ReadOnly v
      expandVal _  (CVal a) = pure a
      expandVal k CValRef = do
        mV <- get db k
        case mV of
          Nothing -> throwIO $ AWExceptionSegmentDeserialisationError "Database did not contain value for key"
          Just v -> pure v
  restoreMapC cm db =
    case cm of
      CacheModeNone -> do
        ks <- LMDB.keys db
        pure $ HM.fromList $ (map (\k -> (k, CValRef))) ks
      CacheModeAll -> do
        kvs <- LMDB.toList db
        pure $ HM.fromList $ (map (\(k,v) -> (k, CVal v))) kvs

toCachedCVal :: CacheMode -> v -> CVal v
toCachedCVal CacheModeAll v = CVal v
toCachedCVal CacheModeNone _ = CValRef


{- cacheable IxSets -}


class IxsetPrimaryKeyClass a where
  type IxsetPrimaryKey a

data CValIxs ixs a =
    CValIxs ByteString a
    -- for performance reasons it would be better to use a V.ARec here maybe (but with what keys...?)
  | CValIxsRef ByteString (NP [] ixs)

pKeyFromCValIxs :: CValIxs ixs a -> ByteString
pKeyFromCValIxs (CValIxs bs _) = bs
pKeyFromCValIxs (CValIxsRef bs _) = bs

instance Eq (CValIxs ixs a) where
  (==) = (==) `on` pKeyFromCValIxs


instance Ord (CValIxs ixs a) where
  compare = compare `on` pKeyFromCValIxs

instance (All Serialise xs) => Serialise (NP [] xs) where
  encode np = encodeListLenIndef <>
    cfoldMap_NP (Proxy :: Proxy Serialise) (encode) np <>
    encodeBreak
  decode = do
    _ <- decodeListLenIndef
    np <- npListFromCBOR
    fin <- decodeBreakOr
    if fin
      then pure $ np
      else fail "Expected to find a break token to mark the end of ListLenIndef when decoding NP [] xs"

npListFromCBOR :: forall s xs. (All Serialise xs) => Decoder s (NP [] xs)
npListFromCBOR =
  case sList :: SList xs of
    SNil   -> pure $ Nil
    SCons -> do
      a <- decode
      r <- npListFromCBOR
      pure $ a :* r



type CIndexable ixs a = (
  IxSet.Indexable ixs a, All Ord ixs, All (ExtractFromNP ixs) ixs

  )

instance (CIndexable ixs a) => IxSet.Indexable ixs (CValIxs ixs a) where
  indices = transformIndices IxSet.indices

transformIndices :: forall ixs a es. (All Ord ixs, All (ExtractFromNP es) ixs) => IxSet.IxList ixs a -> IxSet.IxList ixs (CValIxs es a)
transformIndices IxSet.Nil = IxSet.Nil
transformIndices ((IxSet.:::) ix ilist) = IxSet.ixFun (wrappedIxFun ix) IxSet.::: transformIndices ilist
  where
    wrappedIxFun :: (ExtractFromNP es ix) => IxSet.Ix ix a -> CValIxs es a -> [ix]
    wrappedIxFun (IxSet.Ix _ f) (CValIxs _ a) = f a
    wrappedIxFun _ (CValIxsRef _ np) = extractFromNP np


ixsetIdxsKeyPrefix :: ByteString
ixsetIdxsKeyPrefix = "ixs____"


indexableToIndexes :: IxSet.Indexable ixs a => a -> NP [] ixs
indexableToIndexes a = buildNp IxSet.indices a
  where
    buildNp :: IxSet.IxList ixs a -> a -> NP [] ixs
    buildNp IxSet.Nil _ = Nil
    buildNp ((IxSet.:::) (IxSet.Ix _ f) ilist) aa = f aa :* (buildNp ilist aa)

instance (CIndexable ixs v, All Serialise ixs, IxSet.IsIndexOf (IxsetPrimaryKey (IxSet.IxSet ixs (CValIxs ixs v))) ixs, Serialise (IxsetPrimaryKey (IxSet.IxSet ixs (CValIxs ixs v))), Serialise v) => IsCMap (IxSet.IxSet ixs (CValIxs ixs v)) where
  type CMapKey (IxSet.IxSet ixs (CValIxs ixs v)) = IxsetPrimaryKey (IxSet.IxSet ixs (CValIxs ixs v))
  type CMapValue (IxSet.IxSet ixs (CValIxs ixs v)) = v
  type CDBMapKey (IxSet.IxSet ixs (CValIxs ixs v)) = ByteString
  type CDBMapValue (IxSet.IxSet ixs (CValIxs ixs v)) = ByteString
  type CMapExpanded (IxSet.IxSet ixs (CValIxs ixs v)) = IxSet.IxSet ixs v
  insertMapC cm k v db ixset = do
    let pKey = BL.toStrict $ serialise k
    put db pKey (Just (BL.toStrict $ serialise v))
    let inds = indexableToIndexes v :: NP [] ixs
    put db (ixsetIdxsKeyPrefix <> pKey) (Just (BL.toStrict . serialise $ inds))
    pure $ IxSet.updateIx k (toCachedCValIxs cm pKey v inds) ixset

  insertManyMapC cm vs db ixset = do
    foldM (\is (k, v) -> insertMapC cm k v db is) ixset vs




  restoreMapC cm db =
    case cm of
      CacheModeNone -> do
        ks <- LMDB.keys db
        -- restrict to index keys
        let idxKeys = filter (BS.isPrefixOf ixsetIdxsKeyPrefix) ks
        vs <- mapM deserialiseIxRef idxKeys
        pure $ IxSet.fromList vs
      CacheModeAll -> do
        kvs <- LMDB.toList db
        -- restrict to value keys
        let kvsr = filter (not . BS.isPrefixOf ixsetIdxsKeyPrefix . fst) kvs
        vs <- mapM deserialiseIxVal kvsr
        pure $ IxSet.fromList vs
    where
      deserialiseIxVal :: (ByteString, ByteString) -> Transaction ReadOnly (CValIxs ixs v)
      deserialiseIxVal (bs, bsV) = do
        case deserialiseOrFail (BL.fromStrict bsV) of
          Left err -> throwIO $ AWExceptionSegmentDeserialisationError ("Could not deserialise value: " <> showT err)
          Right v -> pure $ CValIxs bs v

      deserialiseIxRef :: ByteString -> Transaction ReadOnly (CValIxs ixs v)
      deserialiseIxRef k = do
        mIndexesBS <- get db k
        case mIndexesBS of
          Nothing -> throwIO $ AWExceptionSegmentDeserialisationError "Database did not contain value for idx key"
          Just bs -> do
            case deserialiseOrFail (BL.fromStrict bs) of
              Left err -> throwIO $ AWExceptionSegmentDeserialisationError ("Could not deserialise indexes: " <> showT err)
              Right np -> pure $ CValIxsRef (BS.drop (BS.length ixsetIdxsKeyPrefix) k) np

  expandMap db ixset = do
    expanded <- mapM expandVal $ IxSet.toList ixset
    pure $ IxSet.fromList expanded
    where
      expandVal :: (CValIxs ixs v) -> Transaction ReadOnly v
      expandVal (CValIxs _ a) = pure a
      expandVal (CValIxsRef k _) = do
        mBs <- get db k
        case mBs of
          Nothing -> throwIO $ AWExceptionSegmentDeserialisationError "Database did not contain value for key"
          Just bs -> do
            case deserialiseOrFail (BL.fromStrict bs) of
              Left err -> throwIO $ AWExceptionSegmentDeserialisationError ("Could not deserialise when expanding: " <> showT err)
              Right v -> pure v

toCachedCValIxs :: CacheMode -> ByteString -> v -> NP [] ixs -> CValIxs ixs v
toCachedCValIxs CacheModeAll bs v _ = CValIxs bs v
toCachedCValIxs CacheModeNone bs _ ixs = CValIxsRef bs ixs



{- the cache state itself-}

data CacheState ss = CacheState {
  cacheStatePath :: FilePath,
  cacheStateCacheMode :: CacheMode,
  cacheState :: TMVar (SegmentsState ss, Environment ReadWrite, SegmentsDb ss)
}


newtype CUpdate ss a = CUpdate {extractCUpdate :: Re.ReaderT (SegmentsDb ss, CacheMode) (St.StateT (SegmentsState ss) (Transaction ReadWrite)) a}
  deriving (Functor, Applicative, Monad)

newtype CQuery ss a = CQuery {extractCQuery :: Re.ReaderT (SegmentsDb ss, SegmentsState ss) (Transaction ReadOnly) a}
  deriving (Functor, Applicative, Monad)


openCacheState :: (ValidSegmentsCacheState ss, MonadIO m) => FilePath -> CacheMode -> m (Either AWException (CacheState ss))
openCacheState fp cm = do
  env <- liftIO $ openEnvironment fp (defaultLimits{maxDatabases = 200, mapSize = 1024 * 1024 * 1000})
  eBind (getSegmentDbs cm env) $ \dbs -> do
    eBind (restoreSegments cm env ) $ \segs -> do
      csVar <- liftIO $ STM.atomically $ newTMVar (segs, env, dbs)
      pure . pure $ CacheState fp cm csVar


closeCacheState :: (MonadUnliftIO m) => CacheState ss -> m ()
closeCacheState cs = do
  (a, env, b) <- liftIO $ atomically $ TMVar.takeTMVar (cacheState cs)
  onException (liftIO $ closeEnvironment env) (liftIO . atomically $ TMVar.putTMVar (cacheState cs) (a, env, b))
  liftIO $ atomically $ TMVar.putTMVar (cacheState cs) (a, error "Environment has been closed", b)


reopenCacheState :: (ValidSegmentsCacheState ss, MonadIO m) => CacheState ss -> m (Either AWException (CacheState ss))
reopenCacheState cs = openCacheState (cacheStatePath cs) (cacheStateCacheMode cs)


getSegmentDbs :: forall m ss. (MonadIO m, ValidSegmentsCacheState ss) => CacheMode -> Environment ReadWrite -> m (Either AWException (SegmentsDb ss))
getSegmentDbs cm env = (fmap . fmap) (npToSegmentsDb) segsNpE
  where
    segsNpE :: m (Either AWException (NP V.ElField (ToSegmentDBFields ss)))
    segsNpE = unComp $ sequence'_NP segsNp
    segsNp :: NP ((m  :.: Either AWException) :.: V.ElField) (ToSegmentDBFields ss)
    segsNp = cmap_NP (Proxy :: Proxy (ValidDBSegmentField ss)) restoreSegmentFromProxy proxyNp

    restoreSegmentFromProxy :: forall a b . (ValidDBSegmentField ss '(a, b), b ~ SegmentDb a) => Proxy '(a, b) -> ((m :.: Either AWException) :.: V.ElField) '(a, b)
    restoreSegmentFromProxy _ =  Comp $  fmap V.Field $  Comp $ restoreSegment (Proxy :: Proxy a)
    restoreSegment :: ValidCSegment sName => Proxy sName -> m (Either AWException (SegmentDb sName))
    restoreSegment ps = liftIO $ fmap Right $ transaction env (fmap (\s -> SegmentDb (getCacheModePure cm ps, s)) $ getSegmentDbTrans ps)
    proxyNp :: NP Proxy (ToSegmentDBFields ss)
    proxyNp = pure_NP Proxy
    getSegmentDbTrans :: ValidCSegment segmentName => Proxy segmentName -> Transaction ReadWrite (Database k v)
    getSegmentDbTrans ps = getDatabase (Just . T.unpack $ toUniqueText ps)
-- note that we are not using the default state for the SegmentS at the moment, because we don't have a way to determine between empty segments and new segments (perhaps we should track this in our own lmdb admin table?)
restoreSegments :: forall m ss. (MonadIO m, ValidSegmentsCacheState ss) => CacheMode -> Environment ReadWrite -> m (Either AWException (SegmentsState ss))
restoreSegments cm env = (fmap . fmap) (npToSegmentsState) segsNpE
  where
    segsNpE :: m (Either AWException (NP V.ElField (ToSegmentFields ss)))
    segsNpE = unComp $ sequence'_NP segsNp
    segsNp :: NP ((m  :.: Either AWException) :.: V.ElField) (ToSegmentFields ss)
    segsNp = cmap_NP (Proxy :: Proxy (ValidCSegmentField ss)) restoreSegmentFromProxy proxyNp

    restoreSegmentFromProxy :: forall a b . (ValidCSegmentField ss '(a, b), b ~ SegmentS a) => Proxy '(a, b) -> ((m :.: Either AWException) :.: V.ElField) '(a, b)
    restoreSegmentFromProxy _ =  Comp $  fmap V.Field $  Comp $ restoreSegment (Proxy :: Proxy a)
    restoreSegment :: ValidCSegment sName => Proxy sName -> m (Either AWException (SegmentS sName))
    restoreSegment ps = liftIO $ fmap Right $ transaction env (restoreSegmentTrans ps)
    proxyNp :: NP Proxy (ToSegmentFields ss)
    proxyNp = pure_NP Proxy

    restoreSegmentTrans :: ValidCSegment sName => Proxy sName -> Transaction ReadOnly (SegmentS sName)
    restoreSegmentTrans ps = do
      db <- getDatabase (Just . T.unpack $ toUniqueText ps)
      restoreMapC (getCacheModePure cm ps) db


getSegmentDb :: (HasSegmentDb ss segmentName) => Proxy segmentName -> CUpdate ss (SegmentDb segmentName)
getSegmentDb ps = CUpdate $ (fmap $ getSegmentDbP ps . fst) ask

getSegmentC :: (HasSegment ss segmentName) => Proxy segmentName -> CUpdate ss (SegmentS segmentName)
getSegmentC ps = CUpdate $ getSegmentP ps `fmap` St.get

askSegmentDb :: (HasSegmentDb ss segmentName) => Proxy segmentName -> CQuery ss (SegmentDb segmentName)
askSegmentDb ps = CQuery $ (fmap $ getSegmentDbP ps . fst) ask

askSegmentC :: (HasSegment ss segmentName) => Proxy segmentName -> CQuery ss (SegmentS segmentName)
askSegmentC ps = CQuery $ (fmap $ getSegmentP ps . snd) ask


putSegmentC :: (HasSegment ss segmentName) => Proxy segmentName -> SegmentS segmentName -> CUpdate ss ()
putSegmentC ps seg = CUpdate $ St.modify' (putSegmentP ps seg)


insertC :: (ValidCSegment segmentName, HasSegment ss segmentName, HasSegmentDb ss segmentName) => Proxy segmentName -> CMapKey (SegmentS segmentName)  -> CMapValue (SegmentS segmentName)  -> CUpdate ss ()
insertC ps k v = do
  (cm, db) <- fmap segmentDbDatabase $ getSegmentDb ps
  seg <- getSegmentC ps
  newSeg <- CUpdate . lift . lift $ insertMapC cm k v db seg
  putSegmentC ps newSeg

insertManyC :: (ValidCSegment segmentName, HasSegment ss segmentName, HasSegmentDb ss segmentName) => Proxy segmentName -> [(CMapKey (SegmentS segmentName), CMapValue (SegmentS segmentName))]  -> CUpdate ss ()
insertManyC ps vs = do
  (cm, db) <- fmap segmentDbDatabase $ getSegmentDb ps
  seg <- getSegmentC ps
  newSeg <- CUpdate . lift . lift $ insertManyMapC cm vs db seg
  putSegmentC ps newSeg




getCacheModePure :: (SegmentC segmentName) => CacheMode -> Proxy segmentName -> CacheMode
getCacheModePure gcm ps =
  case segmentCacheMode ps of
    SegmentCacheModeGlobal -> gcm
    SegmentCacheModeAll -> CacheModeAll
    SegmentCacheModeNone -> CacheModeNone



fetchMapC :: (ValidCSegment segmentName, HasSegment ss segmentName, HasSegmentDb ss segmentName) => Proxy segmentName -> CQuery ss (CMapExpanded (SegmentS segmentName))
fetchMapC ps = fetchMapCWith ps id



fetchMapCWith :: (ValidCSegment segmentName, HasSegment ss segmentName, HasSegmentDb ss segmentName) => Proxy segmentName -> (SegmentS segmentName -> SegmentS segmentName) -> CQuery ss (CMapExpanded (SegmentS segmentName))
fetchMapCWith ps f = do
  (_, db) <- fmap segmentDbDatabase $ askSegmentDb ps
  seg <- askSegmentC ps
  CQuery . lift  $ expandMap db (f seg)



runUpdateCS :: (MonadUnliftIO m) => CacheState ss -> CUpdate ss a ->  m a
runUpdateCS cs act =
  modifyTMVarSafe (cacheState cs) $ \(segS, env, dbs) -> do
    (a, segS') <- liftIO $ transaction env (St.runStateT (Re.runReaderT (extractCUpdate act) (dbs, cacheStateCacheMode cs)) segS)
    pure ((segS', env, dbs), a)


runQueryCS :: (MonadUnliftIO m) => CacheState ss -> CQuery ss a ->  m a
runQueryCS cs act =
  withTMVarSafe (cacheState cs) $ \(segS, env, dbs) -> do
    liftIO $ transaction env (Re.runReaderT (extractCQuery act) (dbs, segS))
