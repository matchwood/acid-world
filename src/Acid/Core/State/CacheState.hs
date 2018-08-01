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

import Acid.Core.Utils
import Acid.Core.Segment


newtype Ref = Ref BS.ByteString

data CVal a =
    CVal a
  | CValRef


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



class (Serialise (CMapKey a), Serialise (CMapValue a)) => IsCMap a where
  type CMapKey a
  type CMapValue a
  insertMapC :: (CMapKey a) -> CVal (CMapValue a) -> a -> a
  emptyMapC :: a
  restoreMapC :: [CMapKey a] -> a

instance (Serialise k, Serialise v, Eq k, Hashable k) => IsCMap (HM.HashMap k (CVal v)) where
  type CMapKey (HM.HashMap k (CVal v)) = k
  type CMapValue (HM.HashMap k (CVal v)) = v
  insertMapC = HM.insert
  emptyMapC = HM.empty
  restoreMapC = HM.fromList . (map (\k -> (k, CValRef)))

data CacheState ss = CacheState {
  cacheState :: TMVar (SegmentsState ss, Environment ReadWrite)
}

initCacheState :: (ValidSegmentsCacheState ss, MonadIO m) => FilePath -> m (Either Text (CacheState ss))
initCacheState fp = do
  env <- liftIO $ openEnvironment fp (defaultLimits{maxDatabases = 200, mapSize = 1024 * 1024 * 1000})
  eBind (restoreSegments env) $ \segs -> do
     csVar <- liftIO $ STM.atomically $ newTMVar (segs, env)
     pure . pure $ CacheState csVar

restoreSegments :: forall m ss. (MonadIO m, ValidSegmentsCacheState ss) => Environment ReadWrite -> m (Either Text (SegmentsState ss))
restoreSegments env = (fmap . fmap) (npToSegmentsState) segsNpE
  where
    segsNpE :: m (Either Text (NP V.ElField (ToSegmentFields ss)))
    segsNpE = unComp $ sequence'_NP segsNp
    segsNp :: NP ((m  :.: Either Text) :.: V.ElField) (ToSegmentFields ss)
    segsNp = cmap_NP (Proxy :: Proxy (ValidCSegmentField ss)) restoreSegmentFromProxy proxyNp

    restoreSegmentFromProxy :: forall a b . (ValidCSegmentField ss '(a, b), b ~ SegmentS a) => Proxy '(a, b) -> ((m :.: Either Text) :.: V.ElField) '(a, b)
    restoreSegmentFromProxy _ =  Comp $  fmap V.Field $  Comp $ restoreSegment (Proxy :: Proxy a)
    restoreSegment :: ValidCSegment sName => Proxy sName -> m (Either Text (SegmentS sName))
    restoreSegment ps = liftIO $ fmap Right $ transaction env (restoreSegmentTrans ps)



    proxyNp :: NP Proxy (ToSegmentFields ss)
    proxyNp = pure_NP Proxy

restoreSegmentTrans :: ValidCSegment sName => Proxy sName -> Transaction ReadOnly (SegmentS sName)
restoreSegmentTrans ps = do
  db <- getDatabase (Just . T.unpack $ toUniqueText ps)
  ks <- LMDB.keys db
  pure $ restoreMapC ks

newtype CUpdate ss a = CUpdate {extractCUpdate :: St.StateT (SegmentsState ss) (Transaction ReadWrite) a}
  deriving (Functor, Applicative, Monad)


getSegmentDb :: (HasSegment ss segmentName) => Proxy segmentName -> CUpdate ss (Database k v)
getSegmentDb ps = CUpdate . lift $ getDatabase (Just . T.unpack $ toUniqueText ps)

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

runUpdateC :: (MonadIO m, MonadUnliftIO m) => CUpdate ss a -> CacheState ss -> m a
runUpdateC act cs =
  withTMVarSafe (cacheState cs) $ \(segS, env) -> do

    fmap fst $ liftIO $ transaction env (St.runStateT (extractCUpdate act) segS)
