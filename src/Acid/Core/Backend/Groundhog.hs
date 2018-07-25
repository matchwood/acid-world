
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.Groundhog where
import RIO
import qualified  RIO.ByteString as BS
import qualified  RIO.ByteString.Lazy as BL
import Database.Groundhog.Postgresql
import Database.Groundhog
import Data.Pool
import Conduit


import Acid.Core.State
import Acid.Core.Utils
import Acid.Core.Backend.Abstract

data AcidWorldBackendGroundhog





instance AcidWorldBackend AcidWorldBackendGroundhog where
  data AWBState AcidWorldBackendGroundhog = AWBStateGroundhog {awbStateGroundhogPool :: Pool Postgresql}
  data AWBConfig AcidWorldBackendGroundhog = AWBConfigGroundhog {awvConfigGroundhogConnectionConfig :: String} deriving Show
  type AWBSerialiseT AcidWorldBackendGroundhog = BL.ByteString
  type AWBSerialiseConduitT AcidWorldBackendGroundhog = BS.ByteString
  initialiseBackend c _  = do
    p <- createPostgresqlPool (awvConfigGroundhogConnectionConfig c) 1
    pure . pure $ AWBStateGroundhog p
  closeBackend s = liftIO $ destroyAllResources (awbStateGroundhogPool s)
  createCheckpointBackend _ _ _ = pure ()
  getInitialState defState _ _ = pure . pure $ defState
  loadEvents _ _ _ = pure . pure $ LoadEventsConduit $ \rest -> liftIO $ runConduitRes $ yieldMany [] .| rest


  handleUpdateEventC _ _ awu _ ec act = undefined

