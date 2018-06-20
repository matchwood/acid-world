module Acid.Core where
import RIO

import Generics.SOP
--import Generics.SOP.NP
import GHC.TypeLits



data Event (n :: Symbol) xs where
  Event :: (Eventable n, xs ~  FunctionArgs (EventT n)) => (Proxy n) -> (NP I xs) -> Event n xs


class (Apply (EventT n) (FunctionArgs (EventT n))) => Eventable  (n :: Symbol) where
  type EventT n :: k
  runEvent :: (Proxy n) -> EventT n



someFunc :: Bool -> Int -> Text
someFunc = undefined

anotherFunc :: Text -> String -> Int -> Bool
anotherFunc = undefined

instance Eventable "someFunc" where
  type EventT "someFunc" = Bool -> Int -> Text
  runEvent _ = someFunc

instance Eventable "anotherFunc" where
  type EventT "anotherFunc" = Text -> String -> Int -> Bool
  runEvent _ = anotherFunc

doRunEvent :: Event n xs -> (ApplyResult (EventT n) xs)
doRunEvent (Event pn np) = apply (runEvent pn) np

something :: NP I '[Text, String, Int] -> Bool
something np = doRunEvent (Event (Proxy :: Proxy "anotherFunc") np)


type family FunctionArgs a = (res :: [*]) where
  FunctionArgs (a -> b) = (a ': FunctionArgs b)
  FunctionArgs (a) = '[]



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

