{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Segment where


import RIO
import Generics.SOP
import GHC.TypeLits
import GHC.Exts (Constraint)

import qualified  Data.Vinyl as V
import qualified  Data.Vinyl.TypeLevel as V
import qualified  Data.Vinyl.Curry as V




class Segment (segmentName :: Symbol) where
  type SegmentS segmentName :: *
  defaultState :: Proxy segmentName -> SegmentS segmentName

type family ToSegmentFields (segmentNames :: [Symbol]) = (segmentFields :: [(Symbol, *)]) where
  ToSegmentFields '[] = '[]
  ToSegmentFields (s ': ss) = '(s, SegmentS s) ': ToSegmentFields ss


type SegmentsState segmentNames = V.FieldRec (ToSegmentFields segmentNames)


class (V.KnownField a, Segment (V.Fst a), SegmentS (V.Fst a) ~ (V.Snd a)) => KnownSegmentField a
instance (V.KnownField a, Segment (V.Fst a), SegmentS (V.Fst a) ~ (V.Snd a)) => KnownSegmentField a


class (V.HasField V.Rec s (ToSegmentFields segmentNames) (SegmentS s), KnownSymbol s) => HasSegment segmentNames s
instance (V.HasField V.Rec s (ToSegmentFields segmentNames) (SegmentS s), KnownSymbol s) => HasSegment segmentNames s

type family HasSegments (allSegmentNames :: [Symbol]) (segmentNames :: [Symbol]) :: Constraint where
  HasSegments allSegmentNames segmentNames = V.AllConstrained (HasSegment allSegmentNames) segmentNames


type ValidSegmentNames segmentNames =
  ( V.AllFields (ToSegmentFields segmentNames)
  , V.AllConstrained KnownSegmentField (ToSegmentFields segmentNames))


makeDefaultSegment :: forall a. KnownSegmentField a => V.ElField '(V.Fst a, (V.Snd a))
makeDefaultSegment = (V.Label :: V.Label (V.Fst a)) V.=: (defaultState (Proxy :: Proxy (V.Fst a)))


defaultSegmentsState :: forall segmentNames. (ValidSegmentNames segmentNames) => Proxy segmentNames -> SegmentsState segmentNames
defaultSegmentsState _ =  (V.rpureConstrained (Proxy :: Proxy KnownSegmentField) makeDefaultSegment :: SegmentsState segmentNames)


putSegmentP :: forall s ss. (ValidSegmentNames ss, HasSegment ss s) => Proxy s -> Proxy ss -> SegmentS s -> SegmentsState ss -> SegmentsState ss
putSegmentP _ _ seg st = V.rputf (V.Label :: V.Label s) seg st