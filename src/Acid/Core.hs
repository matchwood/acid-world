module Acid.Core where
import RIO

import Generics.SOP
--import Generics.SOP.NP
import GHC.TypeLits


{-
something :: NP I '[Text, String, Int] -> Bool
something np = runEvent (Event (Proxy :: Proxy "anotherFunc") np)


instance Eventable "someFunc" where
  type EventT "someFunc" = Bool -> Int -> Text
  event _ = undefined

instance Eventable "anotherFunc" where
  type EventT "anotherFunc" = Text -> String -> Int -> Bool
  event _ = undefined-}




type family FunctionArgs a = (res :: [*]) where
  FunctionArgs (a -> b) = (a ': FunctionArgs b)
  FunctionArgs (a) = '[]

type family DropResult a where
  DropResult (a -> b -> c) = a -> DropResult (b -> c)
  DropResult (a -> b) = a

type family FunctionResult a = (res :: *) where
  FunctionResult (a -> b) = FunctionResult b
  FunctionResult a = a


class BackendMonad m where


data Event (n :: Symbol) xs where
  Event :: (Eventable n, xs ~  FunctionArgs (EventT n)) => (Proxy n) -> (NP I xs) -> Event n xs

class Eventable  (n :: Symbol) where
  type EventT n :: *
  event :: (ApplyM m ) BackendMonad m => (Proxy n) -> DropResult (EventT n) -> m (FunctionResult (EventT n))


runEvent :: ( BackendMonad m) => Event n xs -> m (ApplyResult ((DropResult (EventT n) -> (FunctionResult (EventT n)))) xs)
runEvent (Event pn np) = applyM (event pn) np

class ApplyM m f xs where
  applyM :: f -> (NP I xs) ->  m (ApplyResult f xs)


instance ApplyM m (m a) ('[]) where
  applyM f _ = f

instance ApplyM m (a -> b) (a ': '[]) where
  applyM f np = f (unI . hd $ np)

instance (ApplyM m b (x ': xs)) => ApplyM m (a -> b) (a ': x ': xs) where
  applyM f np = apply (f (unI . hd $ np)) (tl np)




class Apply f xs where
  type ApplyResult f xs
  apply :: f -> (NP I xs) ->  ApplyResult f xs


instance Apply (a) ('[]) where
  type ApplyResult a  '[] = a
  apply f _ = f

instance Apply (a -> b) (a ': '[]) where
  type ApplyResult (a -> b) (a ': '[]) = b
  apply f np = f (unI . hd $ np)

instance (Apply b (x ': xs)) => Apply (a -> b) (a ': x ': xs) where
  type ApplyResult (a -> b) (a ': x ': xs) = ApplyResult b (x ': xs)
  apply f np = apply (f (unI . hd $ np)) (tl np)







{-class Apply (f :: * -> k) (xs :: [*]) where
  apply :: ((a ': '[moreXs]) ~ xs)=> f -> (NP I xs) ->  f a-}


{-apply ::((a ': moreXs ) ~ xs) => (a -> b) -> NP I xs -> b
apply = undefined-}

