
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.FS where
import RIO
import qualified RIO.Directory as Dir
import qualified  RIO.ByteString.Lazy as BL
import qualified  RIO.Vector.Boxed as VB




import Acid.Core.Segment
import Acid.Core.Event
import Acid.Core.Backend.Abstract
import Acid.Core.Inner.Abstract


newtype AcidWorldBackendFS ss nn a = AcidWorldBackendFS (IO a)
  deriving (Functor, Applicative, Monad, MonadIO, MonadThrow)





instance ( ValidSegmentNames ss
         , ValidEventNames ss nn ) =>
  AcidWorldBackend AcidWorldBackendFS ss nn where
  data AWBState AcidWorldBackendFS ss nn = AWBStateBackendFS {
    sWBStateBackendConfig :: AWBConfig AcidWorldBackendFS ss nn
  }
  data AWBConfig AcidWorldBackendFS ss nn = AWBConfigBackendFS {
    aWBConfigBackendFSStateDir :: FilePath
  }
  type AWBSerialiseT AcidWorldBackendFS ss nn = BL.ByteString
  initialiseBackend c _  = do
    stateP <- Dir.makeAbsolute (aWBConfigBackendFSStateDir c)
    Dir.createDirectoryIfMissing True stateP
    let eventPath = makeEventPath stateP
    b <- Dir.doesFileExist eventPath
    when (not b) (BL.writeFile eventPath "")
    pure . pure $ AWBStateBackendFS c{aWBConfigBackendFSStateDir = stateP}
  loadEvents deserialiser s = do
    let eventPath = makeEventPath (aWBConfigBackendFSStateDir . sWBStateBackendConfig $ s)



    bl <- BL.readFile eventPath
    let bs = VB.filter (not . BL.null) $ VB.fromList $ BL.split 10 bl

    pure $ sequence $ VB.map deserialiser bs

  -- this should be bracketed and so forth @todo
  handleUpdateEvent serializer awb awu (e :: Event n) = do
    let eventPath = makeEventPath (aWBConfigBackendFSStateDir . sWBStateBackendConfig $ awb)
    stE <- mkStorableEvent e
    BL.appendFile eventPath (serializer stE <> "\n")

    let (AcidWorldBackendFS m :: AcidWorldBackendFS ss nn (EventResult n)) = runUpdateEvent awu e
    liftIO $ m

makeEventPath :: FilePath -> FilePath
makeEventPath fp = fp <> "/" <> "events.json"