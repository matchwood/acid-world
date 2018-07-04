
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.Memory where
import RIO
import qualified  RIO.ByteString.Lazy as BL



import Acid.Core.Segment
import Acid.Core.State
import Acid.Core.Backend.Abstract

newtype AcidWorldBackendMemory ss nn a = AcidWorldBackendMemory (IO a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadThrow)





instance ( ValidSegmentNames ss
         , ValidEventNames ss nn ) =>
  AcidWorldBackend AcidWorldBackendMemory ss nn where
  data AWBState AcidWorldBackendMemory ss nn = AWBStateMemory
  data AWBConfig AcidWorldBackendMemory ss nn = AWBConfigMemory
  type AWBSerialiseT AcidWorldBackendMemory ss nn = BL.ByteString
  initialiseBackend _ _  = pure . pure $ AWBStateMemory
  handleUpdateEvent _ _ awu e = runUpdate awu e

