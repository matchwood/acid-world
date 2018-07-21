
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.Memory where
import RIO
import qualified  RIO.ByteString as BS
import qualified  RIO.ByteString.Lazy as BL



import Acid.Core.State
import Acid.Core.Backend.Abstract

data AcidWorldBackendMemory





instance AcidWorldBackend AcidWorldBackendMemory where
  data AWBState AcidWorldBackendMemory = AWBStateMemory
  data AWBConfig AcidWorldBackendMemory = AWBConfigMemory deriving Show
  type AWBSerialiseT AcidWorldBackendMemory = BL.ByteString
  type AWBSerialiseConduitT AcidWorldBackendMemory = BS.ByteString
  initialiseBackend _ _ _  = pure . pure $ AWBStateMemory
  handleUpdateEventC _ _ awu ec = fmap snd $ runUpdateC awu ec

