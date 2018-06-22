
module Acid.Core.Utils where

import RIO
import qualified  RIO.Text as T
import GHC.TypeLits
import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.Functor as V

import Generics.SOP.NP
import Generics.SOP

showSymbol :: (KnownSymbol a) => proxy a -> T.Text
showSymbol p = T.pack $ symbolVal p



type family Elem (a :: k) (b :: [k]) :: Bool where
    Elem a '[] = 'False
    Elem a (a ': xs) = 'True
    Elem a (b ': xs) = Elem a xs

class (Elem a b ~ 'True) => IsElem (a :: k) (b :: [k])
instance  (Elem a b ~ 'True) => IsElem a b



npToVinylRec :: (forall a. f a -> g a) -> NP f xs -> V.Rec g xs
npToVinylRec _ Nil = V.RNil
npToVinylRec f ((:*) a restNp) = f a V.:& (npToVinylRec f restNp)

npIToVinylHList :: NP I xs -> V.HList xs
npIToVinylHList np = npToVinylRec (V.Identity . unI) np
