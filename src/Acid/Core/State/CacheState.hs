{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.State.CacheState where

import RIO
import qualified RIO.HashMap as HM
import qualified RIO.Text as T
import qualified RIO.ByteString as BS
import Database.LMDB.Simple
import qualified Control.Concurrent.STM  as STM
import qualified Control.Monad.State.Strict as St
import Generics.SOP
import Generics.SOP.NP
import Codec.Serialise
import qualified  Data.Vinyl.TypeLevel as V
import qualified  Data.Vinyl.Derived as V
import GHC.TypeLits

import qualified Database.LMDB.Simple.Extra as LMDB
import Acid.Core.State.Abstract
import Acid.Core.Utils
import Acid.Core.Segment


newtype Ref = Ref BS.ByteString

data CVal a =
    CVal a
  | CValRef

runCVal :: (ValidCSegment segmentName) => Proxy segmentName -> ((CMapKey (SegmentS segmentName)), CVal (CMapValue (SegmentS segmentName))) -> CUpdate ss (CMapValue (SegmentS segmentName))
runCVal _ (_, CVal a) = pure a
runCVal ps (k, CValRef) = do
  db <- getSegmentDb ps
  mV <- CUpdate . lift $ get db k
  case mV of
    Nothing -> CUpdate . lift $ throwIO $ AWExceptionSegmentDeserialisationError "Database did not contain value for key"
    Just v -> pure v


class (IsCMap (SegmentS segmentName), Segment segmentName) => ValidCSegment segmentName
instance (IsCMap (SegmentS segmentName), Segment segmentName) => ValidCSegment segmentName

class (IsCMap (V.Snd sf), Segment (V.Fst sf), KnownSymbol (V.Fst sf),  SegmentFetching ss sf) => ValidCSegmentField ss sf
instance (IsCMap (V.Snd sf), Segment (V.Fst sf), KnownSymbol (V.Fst sf),  SegmentFetching ss sf) => ValidCSegmentField ss sf

type ValidSegmentsCacheState ss =
  (All ValidCSegment ss,
   ValidSegmentNames ss,
   ValidSegments ss,
   All (ValidCSegmentField ss) (ToSegmentFields ss)
   )


newtype SegmentDb s = SegmentDb (Database (CMapKey (SegmentS s)) (CMapValue (SegmentS s)))

class (Show (CMapKey a), Serialise (CMapKey a), Serialise (CMapValue a)) => IsCMap a where
  type CMapKey a
  type CMapValue a
  insertMapC :: (CMapKey a) -> CVal (CMapValue a) -> a -> a
  emptyMapC :: a
  restoreMapC :: [CMapKey a] -> a
  toListMapC :: a -> [(CMapKey a, CVal (CMapValue a))]


instance (Show k, Serialise k, Serialise v, Eq k, Hashable k) => IsCMap (HM.HashMap k (CVal v)) where
  type CMapKey (HM.HashMap k (CVal v)) = k
  type CMapValue (HM.HashMap k (CVal v)) = v
  insertMapC = HM.insert
  emptyMapC = HM.empty
  restoreMapC = HM.fromList . (map (\k -> (k, CValRef)))
  toListMapC = HM.toList

data CacheState ss = CacheState {
  cacheStatePath :: FilePath,
  cacheState :: TMVar (SegmentsState ss, Environment ReadWrite),
  cacheDbs :: NP SegmentDb ss
}

openCacheState :: (ValidSegmentsCacheState ss, MonadIO m) => FilePath -> m (Either AWException (CacheState ss))
openCacheState fp = do
  env <- liftIO $ openEnvironment fp (defaultLimits{maxDatabases = 200, mapSize = 1024 * 1024 * 1000})
  eBind (getSegmentDbs env) $ \dbs -> do
    eBind (restoreSegments env) $ \segs -> do
      csVar <- liftIO $ STM.atomically $ newTMVar (segs, env)
      pure . pure $ CacheState fp csVar dbs


reopenCacheState :: (ValidSegmentsCacheState ss, MonadIO m) => CacheState ss -> m (Either AWException (CacheState ss))
reopenCacheState cs = openCacheState (cacheStatePath cs)

getSegmentDbs :: forall m ss. (MonadIO m, ValidSegmentsCacheState ss) => Environment ReadWrite -> m (Either AWException (NP SegmentDb ss))
getSegmentDbs env = segsNpE
  where
    segsNpE :: m (Either AWException (NP SegmentDb ss))
    segsNpE = unComp $ sequence'_NP segsNp
    segsNp :: NP ((m  :.: Either AWException) :.: SegmentDb) ss
    segsNp = cmap_NP (Proxy :: Proxy (ValidCSegment)) getSegmentDbFromProxy proxyNp

    getSegmentDbFromProxy :: forall s. ValidCSegment s => Proxy s -> ((m :.: Either AWException) :.: SegmentDb) s
    getSegmentDbFromProxy _ =  Comp $  fmap SegmentDb $  Comp $ getSegmentDbFromP (Proxy :: Proxy s)
    getSegmentDbFromP :: forall sName. ValidCSegment sName => Proxy sName -> m (Either AWException (Database (CMapKey (SegmentS sName)) (CMapValue (SegmentS sName))))
    getSegmentDbFromP ps = liftIO $ fmap Right $ transaction env (getSegmentDbTrans ps)



    proxyNp :: NP Proxy ss
    proxyNp = pure_NP Proxy

restoreSegments :: forall m ss. (MonadIO m, ValidSegmentsCacheState ss) => Environment ReadWrite -> m (Either AWException (SegmentsState ss))
restoreSegments env = (fmap . fmap) (npToSegmentsState) segsNpE
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

restoreSegmentTrans :: ValidCSegment sName => Proxy sName -> Transaction ReadWrite (SegmentS sName)
restoreSegmentTrans ps = do
  db <- getDatabase (Just . T.unpack $ toUniqueText ps)
  ks <- LMDB.keys db
  pure $ restoreMapC ks

newtype CUpdate ss a = CUpdate {extractCUpdate :: St.StateT (SegmentsState ss) (Transaction ReadWrite) a}
  deriving (Functor, Applicative, Monad)


getSegmentDb :: ( ValidCSegment segmentName) => Proxy segmentName -> CUpdate ss (Database k v)
getSegmentDb ps = CUpdate . lift $ getSegmentDbTrans ps

getSegmentDbTrans :: ValidCSegment segmentName => Proxy segmentName -> Transaction ReadWrite (Database k v)
getSegmentDbTrans ps = getDatabase (Just . T.unpack $ toUniqueText ps)

getSegmentC :: (HasSegment ss segmentName) => Proxy segmentName -> CUpdate ss (SegmentS segmentName)
getSegmentC ps = CUpdate $ getSegmentP ps `fmap` St.get

putSegmentC :: (HasSegment ss segmentName) => Proxy segmentName -> SegmentS segmentName -> CUpdate ss ()
putSegmentC ps seg = CUpdate $ St.modify' (putSegmentP ps seg)


insertC :: (ValidCSegment segmentName, HasSegment ss segmentName) => Proxy segmentName -> CMapKey (SegmentS segmentName)  -> CMapValue (SegmentS segmentName)  -> CUpdate ss ()
insertC ps k v = do

  db <- getSegmentDb ps
  CUpdate . lift $ put db k (Just v)
  seg <- getSegmentC ps
  let newSeg = insertMapC k (CVal v) seg
  putSegmentC ps newSeg

insertManyC :: (ValidCSegment segmentName, HasSegment ss segmentName) => Proxy segmentName -> [(CMapKey (SegmentS segmentName),CMapValue (SegmentS segmentName))]  -> CUpdate ss ()
insertManyC ps vs = do
  db <- getSegmentDb ps
  CUpdate . lift $ mapM_ (\(k, v) -> put db k (Just v)) vs

  seg <- getSegmentC ps
  let newSeg = foldl' (\s (k,v) -> insertMapC k (CVal v) s) seg vs
  --let newSeg = insertMapC k (CVal v) seg
  putSegmentC ps newSeg


fetchAllC :: (ValidCSegment segmentName, HasSegment ss segmentName) => Proxy segmentName -> CUpdate ss [CMapValue (SegmentS segmentName)]
fetchAllC ps = do
  seg <- getSegmentC ps
  let tups = toListMapC seg
  mapM (runCVal ps) tups




runUpdateCS :: (MonadIO m, MonadUnliftIO m) => CacheState ss -> CUpdate ss a ->  m a
runUpdateCS cs act =
  modifyTMVarSafe (cacheState cs) $ \(segS, env) -> do
    (a, segS') <- liftIO $ transaction env (St.runStateT (extractCUpdate act) segS)
    pure ((segS', env), a)

