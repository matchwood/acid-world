{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Utils where

import RIO
import qualified  RIO.Text as T
import GHC.TypeLits
import GHC.Exts
import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.Functor as V

import Generics.SOP.NP
import Generics.SOP

showSymbol :: (KnownSymbol a) => proxy a -> T.Text
showSymbol p = T.pack $ symbolVal p


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


npToVinylRec :: (forall a. f a -> g a) -> NP f xs -> V.Rec g xs
npToVinylRec _ Nil = V.RNil
npToVinylRec f ((:*) a restNp) = f a V.:& (npToVinylRec f restNp)

npIToVinylHList :: NP I xs -> V.HList xs
npIToVinylHList np = npToVinylRec (V.Identity . unI) np
