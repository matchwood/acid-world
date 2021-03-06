{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}

module Acid.Core.Segment where

import RIO
import Generics.SOP
import GHC.TypeLits

import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.TypeLevel as V
import Acid.Core.Utils



class (ToUniqueText segmentName) => Segment (segmentName :: Symbol) where
  type SegmentS segmentName :: *
  defaultState :: Proxy segmentName -> SegmentS segmentName

type family ToSegmentFields (segmentNames :: [Symbol]) = (segmentFields :: [(Symbol, *)]) where
  ToSegmentFields '[] = '[]
  ToSegmentFields (s ': ss) = '(s, SegmentS s) ': ToSegmentFields ss

type family ToSegmentElFields (segmentNames :: [Symbol]) = (segmentFields :: [(Symbol, *)]) where
  ToSegmentElFields '[] = '[]
  ToSegmentElFields (s ': ss) = '(s, SegmentS s) ': ToSegmentElFields ss

type family ToSegmentTypes (segmentNames :: [Symbol]) :: [*] where
  ToSegmentTypes '[] = '[]
  ToSegmentTypes (s ': ss) = (SegmentS s) ': ToSegmentTypes ss

prettySegment :: (Segment s) => Proxy s -> Text
prettySegment ps = "*Segment '" <> toUniqueText ps <> "'* "

newtype SegmentsState segmentNames = SegmentsState {segmentsStateFieldRec :: V.AFieldRec (ToSegmentFields segmentNames)}


class (V.KnownField a, Segment (V.Fst a), SegmentS (V.Fst a) ~ (V.Snd a)) => KnownSegmentField a
instance (V.KnownField a, Segment (V.Fst a), SegmentS (V.Fst a) ~ (V.Snd a)) => KnownSegmentField a


class (V.HasField V.ARec s (ToSegmentFields segmentNames) (SegmentS s), KnownSymbol s) => HasSegment segmentNames s
instance (V.HasField V.ARec s (ToSegmentFields segmentNames) (SegmentS s), KnownSymbol s) => HasSegment segmentNames s


class (SegmentS n ~ s) => SegmentNameToState n s
instance (SegmentS n ~ s) => SegmentNameToState n s


class (KnownSegmentField sField, HasSegment ss (V.Fst sField)) => SegmentFetching ss sField
instance (KnownSegmentField sField, HasSegment ss (V.Fst sField)) => SegmentFetching ss sField


type ValidSegmentNames segmentNames =
  ( V.AllFields (ToSegmentFields segmentNames)
  , V.AllConstrained KnownSegmentField (ToSegmentFields segmentNames)
  , V.NatToInt (V.RLength (ToSegmentFields segmentNames))
  , UniqueElementsWithErr segmentNames ~ 'True
  )

class ( AllZip SegmentNameToState ss (ToSegmentTypes ss)
      , All (SegmentFetching ss) (ToSegmentFields ss)
      , All (HasSegment ss) ss
      , ValidSegmentNames ss)
      => ValidSegments ss

instance ( AllZip SegmentNameToState ss (ToSegmentTypes ss)
      , All (SegmentFetching ss) (ToSegmentFields ss)
      , All (HasSegment ss) ss
      , ValidSegmentNames ss)
      => ValidSegments ss


npToSegmentsState :: forall ss. (ValidSegmentNames ss) => NP V.ElField (ToSegmentFields ss) -> SegmentsState ss
npToSegmentsState np = SegmentsState $ npToARec (Proxy :: Proxy ss) np

npToARec :: (ValidSegmentNames ss) => Proxy ss -> NP V.ElField (ToSegmentFields ss) -> V.AFieldRec (ToSegmentFields ss)
npToARec _ np = npToVinylARec id np

makeDefaultSegment :: forall a. KnownSegmentField a => V.ElField '(V.Fst a, (V.Snd a))
makeDefaultSegment = (V.Label :: V.Label (V.Fst a)) V.=: (defaultState (Proxy :: Proxy (V.Fst a)))


defaultSegmentsState :: forall segmentNames. (ValidSegmentNames segmentNames) => SegmentsState segmentNames
defaultSegmentsState =  SegmentsState $ V.toARec $ V.rpureConstrained (Proxy :: Proxy KnownSegmentField) makeDefaultSegment

putSegmentP :: forall s ss. (HasSegment ss s) => Proxy s -> SegmentS s -> SegmentsState ss -> SegmentsState ss
putSegmentP _ seg (SegmentsState fr) = SegmentsState $ V.rputf (V.Label :: V.Label s) seg fr

getSegmentP :: forall s ss. (HasSegment ss s) => Proxy s ->  SegmentsState ss -> SegmentS s
getSegmentP _ (SegmentsState fr) = V.getField $ V.rgetf (V.Label :: V.Label s) fr


