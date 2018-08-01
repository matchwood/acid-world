{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.State.CacheState where

import RIO
import qualified RIO.HashMap as HM
import qualified RIO.Text as T
import qualified RIO.ByteString as BS
import qualified Data.IxSet.Typed as IxSet
import Database.LMDB.Simple

import qualified Control.Concurrent.STM.TMVar as  TMVar
import qualified Control.Monad.State.Strict as St
import Generics.SOP
import Codec.Serialise

import Acid.Core.Utils
import Acid.Core.Segment


newtype Ref = Ref BS.ByteString

data CVal a =
    CVal a
  | CValRef


class IsCMap a k v | a -> k, a -> v where
  insertMapC :: k -> CVal v -> a -> a
  emptyMapC :: a


instance (Eq k, Hashable k) => IsCMap (HM.HashMap k (CVal v)) k v where
  insertMapC = HM.insert
  emptyMapC = HM.empty

data CacheState ss = CacheState {
  cacheState :: TMVar (SegmentsState ss, Environment ReadWrite)
}

newtype CUpdate ss a = CUpdate {extractCUpdate :: St.StateT (SegmentsState ss) (Transaction ReadWrite) a}
  deriving (Functor, Applicative, Monad)

class (IsCMap (SegmentS segmentName) k v, Serialise k, Serialise v) => ValidCSegment segmentName k v
instance (IsCMap (SegmentS segmentName) k v, Serialise k, Serialise v) => ValidCSegment segmentName k v

getSegmentDb :: (HasSegment ss segmentName) => Proxy segmentName -> CUpdate ss (Database k v)
getSegmentDb ps = CUpdate . lift $ getDatabase (Just . T.unpack $ toUniqueText ps)

getSegmentC :: (HasSegment ss segmentName) => Proxy segmentName -> CUpdate ss (SegmentS segmentName)
getSegmentC ps = CUpdate $ getSegmentP ps `fmap` St.get

putSegmentC :: (HasSegment ss segmentName) => Proxy segmentName -> SegmentS segmentName -> CUpdate ss ()
putSegmentC ps seg = CUpdate $ St.modify' (putSegmentP ps seg)


insertC :: (ValidCSegment segmentName k v, HasSegment ss segmentName) => Proxy segmentName -> k -> v -> CUpdate ss ()
insertC ps k v = do
  db <- getSegmentDb ps
  CUpdate . lift $ put db k (Just v)
  seg <- getSegmentC ps
  let newSeg = insertMapC k (CVal v) seg
  putSegmentC ps newSeg

runUpdateC :: (MonadIO m) => CUpdate ss a -> CacheState ss -> m a
runUpdateC act cs =
  withTMVarSafe (cacheState cs) $ \(segS, env) -> do

    fmap fst $ liftIO $ transaction env (St.runStateT (extractCUpdate act) segS)
