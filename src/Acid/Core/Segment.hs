{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Segment where



import Generics.SOP
import GHC.TypeLits
import GHC.Exts (Constraint)

import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.TypeLevel as V




class Segment (segmentName :: Symbol) where
  type SegmentS segmentName :: *
  defaultState :: Proxy segmentName -> SegmentS segmentName

type family ToSegmentFields (segmentNames :: [Symbol]) = (segmentFields :: [(Symbol, *)]) where
  ToSegmentFields '[] = '[]
  ToSegmentFields (s ': ss) = '(s, SegmentS s) ': ToSegmentFields ss


type SegmentsState segmentNames = V.FieldRec (ToSegmentFields segmentNames)


class (V.KnownField a, Segment (V.Fst a), SegmentS (V.Fst a) ~ (V.Snd a)) => KnownSegmentField a
instance (V.KnownField a, Segment (V.Fst a), SegmentS (V.Fst a) ~ (V.Snd a)) => KnownSegmentField a


class (V.HasField V.Rec s ss (SegmentS s), KnownSymbol s) => HasSegment ss s
instance (V.HasField V.Rec s ss (SegmentS s), KnownSymbol s) => HasSegment ss s

type family HasSegments (allSegmentNames :: [Symbol]) (segmentNames :: [Symbol]) :: Constraint where
  HasSegments allSegmentNames segmentNames = V.AllConstrained (HasSegment (ToSegmentFields allSegmentNames)) segmentNames


type ValidSegmentNames segmentNames =
  ( V.AllFields (ToSegmentFields segmentNames)
  , V.AllConstrained KnownSegmentField (ToSegmentFields segmentNames))


makeDefaultSegment :: forall a. KnownSegmentField a => V.ElField '(V.Fst a, (V.Snd a))
makeDefaultSegment = (V.Label :: V.Label (V.Fst a)) V.=: (defaultState (Proxy :: Proxy (V.Fst a)))


defaultSegmentsState :: forall segmentNames. (ValidSegmentNames segmentNames) => SegmentsState segmentNames
defaultSegmentsState =  (V.rpureConstrained (Proxy :: Proxy KnownSegmentField) makeDefaultSegment :: SegmentsState segmentNames)