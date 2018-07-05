
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.Memory where
import RIO
import qualified  RIO.ByteString.Lazy as BL



import Acid.Core.State
import Acid.Core.Backend.Abstract

data AcidWorldBackendMemory





instance AcidWorldBackend AcidWorldBackendMemory where
  data AWBState AcidWorldBackendMemory = AWBStateMemory
  data AWBConfig AcidWorldBackendMemory = AWBConfigMemory
  type AWBSerialiseT AcidWorldBackendMemory = BL.ByteString
  backendName _ = "Memory"
  initialiseBackend _ _ _  = pure . pure $ AWBStateMemory
  handleUpdateEvent _ _ awu e = runUpdate awu e

