{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Utils where

import RIO
import qualified  RIO.Text as T
import GHC.TypeLits
import GHC.Exts
import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.Functor as V
import qualified  Data.Vinyl.Curry as V
import qualified  Data.Vinyl.ARec as V
import qualified  Data.Vinyl.TypeLevel as V
import Data.Proxy(Proxy(..))

import Generics.SOP.NP
import qualified Generics.SOP as SOP
import qualified Control.Concurrent.STM.TMVar as  TMVar


withTMVarSafe :: (MonadIO m, MonadUnliftIO m) => TMVar a -> (a -> m b) -> m b
withTMVarSafe m io = do
  a <- liftIO . atomically $ TMVar.takeTMVar m
  b <- onException (io a) (liftIO . atomically $ TMVar.putTMVar m a)
  liftIO . atomically $ TMVar.putTMVar m a
  pure b


modifyTMVar :: (MonadIO m) => TMVar a -> (a -> m (a, b)) -> m b
modifyTMVar m io = do
  a <- liftIO $ atomically $ TMVar.takeTMVar m
  (a', b) <- io a
  liftIO $ atomically $ TMVar.putTMVar m a'
  pure b

modifyTMVarSafe :: (MonadIO m, MonadUnliftIO m) => TMVar a -> (a -> m (a, b)) -> m b
modifyTMVarSafe m io = do
  a <- liftIO $ atomically $ TMVar.takeTMVar m
  (a', b) <- onException (io a) (liftIO . atomically $ TMVar.putTMVar m a)
  liftIO $ atomically $ TMVar.putTMVar m a'
  pure b



eBind :: (Monad m) => m (Either a b) -> (b -> m (Either a c)) -> m (Either a c)
eBind  act f = do
  r <- act
  case r of
    Left e -> pure $ Left e
    Right b -> f b


class ToUniqueText (a :: k) where
  toUniqueText :: Proxy a -> Text

instance (KnownSymbol a) => ToUniqueText (a :: Symbol) where
  toUniqueText = T.pack . symbolVal

showT :: (Show a) => a -> T.Text
showT = utf8BuilderToText . displayShow

showSymbol :: (KnownSymbol a) => proxy a -> T.Text
showSymbol p = T.pack $ symbolVal p

type family Sort (xs :: [Symbol]) where
  Sort '[] = '[]
  Sort (x ': xs) = Insert x (Sort xs)

type family Insert x xs where
  Insert x '[] = x ': '[]
  Insert x (y ': ys) = Insert' (CmpSymbol x y) x y ys

type family Insert' b x y ys where
  Insert' 'LT  x y ys = x ': (y ': ys)
  Insert' _    x y ys = y ': Insert x ys



type family Last (a :: [k]) :: k where
  Last '[] = TypeError ('Text "Expected a type level list with at least one element")
  Last '[a] = a
  Last (a ': xs) = Last xs

type family First (a :: [k]) :: k where
  First '[] = TypeError ('Text "Expected a type level list with at least one element")
  First (a ': _) = a

type family Union (a :: [k]) (b :: [k]) = (res :: [k]) where
  Union '[] b = b
  Union (a ': xs) b = a ': Union xs b

type family Elem (a :: k) (b :: [k]) :: Bool where
    Elem a '[] = 'False
    Elem a (a ': xs) = 'True
    Elem a (b ': xs) = Elem a xs

type family ElemOrErr (a :: k) (b :: [k]) :: Constraint where
  ElemOrErr a xs = IfOrErrC (Elem a xs) ((
    'Text "Type " ':<>:
    'ShowType a ':$$:
    'Text "not found in the type level list: " ':<>:
    'ShowType xs))

type family IfOrErrC (a :: Bool) (err :: ErrorMessage) :: Constraint where
  IfOrErrC 'True _ = ()
  IfOrErrC 'False err = TypeError err

class (Elem a b ~ 'True) => IsElem (a :: k) (b :: [k])
instance  (Elem a b ~ 'True) => IsElem a b

type family And (a :: Bool) (b :: Bool) :: Bool where
    And 'False a = 'False
    And 'True a = a
    And a 'False = 'False
    And a 'True = a
    And a a = a

type family UniqueElementsWithErr (a :: [k]) :: Bool where
  UniqueElementsWithErr a = UniqueElementsWithErr' a a

type family UniqueElementsWithErr' (a :: [k]) (b :: [k]) :: Bool where
  UniqueElementsWithErr' '[] _ = 'True
  UniqueElementsWithErr' (a ': xs) b = And (NotWithErr (Elem a xs) (
    'Text "Duplicate element found: " ':<>:
    'ShowType a ':$$:
    'Text " in type list: " ':<>: 'ShowType b
    )) (UniqueElementsWithErr' xs b)

type family NotWithErr (a :: Bool) (err :: ErrorMessage) = (res :: Bool)  where
    NotWithErr 'False _ = 'True
    NotWithErr 'True err = TypeError err


npToVinylRec :: (forall a. f a -> g a) -> NP f xs -> V.Rec g xs
npToVinylRec _ Nil = V.RNil
npToVinylRec f ((:*) a restNp) = f a V.:& (npToVinylRec f restNp)

vinylRecToNp :: (forall a. f a -> g a) -> V.Rec f xs -> NP g xs
vinylRecToNp _ V.RNil = Nil
vinylRecToNp f ((V.:&) a restRec) = f a :* (vinylRecToNp f restRec)

npToVinylARec :: (V.NatToInt (V.RLength xs)) => (forall a. f a -> g a) -> NP f xs -> V.ARec g xs
npToVinylARec f np = V.toARec $ npToVinylRec f np

vinylARecToNp :: (V.RecApplicative xs, V.AllConstrained (V.IndexableField xs) xs) => (forall a. f a -> g a) -> V.ARec f xs -> NP g xs
vinylARecToNp f arec =  vinylRecToNp f $ V.fromARec arec


npIToVinylHList :: NP SOP.I xs -> V.HList xs
npIToVinylHList np = npToVinylRec (V.Identity . SOP.unI) np

vinylHListToNpI :: V.HList xs -> NP SOP.I xs
vinylHListToNpI hl = vinylRecToNp (SOP.I . V.getIdentity) hl

-- steal these from vinyl
type NPCurried ts a = V.Curried ts a
type NPCurriedF f ts a = V.CurriedF f ts a

class NPCurry ts where
  npCurry :: (NP f ts -> a) -> NPCurriedF f ts a
  npICurry :: (NP SOP.I ts -> a) -> NPCurried ts a


instance NPCurry '[] where
  npCurry f = f Nil
  {-# INLINABLE npCurry #-}
  npICurry f = f Nil
  {-# INLINABLE npICurry #-}

instance NPCurry ts => NPCurry (t ': ts) where
  npCurry f x = npCurry (\xs -> f (x :* xs))
  {-# INLINABLE npCurry #-}
  npICurry f x = npICurry (\xs -> f (SOP.I x :* xs))
  {-# INLINABLE npICurry #-}



npUncurry :: NPCurriedF f ts a -> NP f ts -> a
npUncurry x Nil      = x
npUncurry f (x :* xs) = npUncurry (f x) xs
{-# INLINABLE npUncurry #-}

npIUncurry :: NPCurried ts a -> NP SOP.I ts -> a
npIUncurry x Nil      = x
npIUncurry f (SOP.I x :* xs) = npIUncurry (f x) xs
{-# INLINABLE npIUncurry #-}
