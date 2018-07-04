
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.FS where
import RIO
import qualified RIO.Directory as Dir
import qualified RIO.Text as T
import qualified  RIO.ByteString.Lazy as BL


import Prelude(userError)

import Acid.Core.State
import Acid.Core.Backend.Abstract
import Conduit

data AcidWorldBackendFS





instance AcidWorldBackend AcidWorldBackendFS where
  data AWBState AcidWorldBackendFS = AWBStateFS {
    aWBStateFSConfig :: AWBConfig AcidWorldBackendFS
  }
  data AWBConfig AcidWorldBackendFS = AWBConfigFS {
    aWBConfigFSStateDir :: FilePath
  }
  type AWBSerialiseT AcidWorldBackendFS  = BL.ByteString
  initialiseBackend _ c _  = do
    stateP <- Dir.makeAbsolute (aWBConfigFSStateDir c)
    Dir.createDirectoryIfMissing True stateP
    let eventPath = makeEventPath stateP
    b <- Dir.doesFileExist eventPath
    when (not b) (BL.writeFile eventPath "")
    pure . pure $ AWBStateFS c{aWBConfigFSStateDir = stateP}
  loadEvents deserialiseConduit s = do
    let eventPath = makeEventPath (aWBConfigFSStateDir . aWBStateFSConfig $ s)


    pure $
         sourceFile eventPath .|
         mapC BL.fromStrict .|
         deserialiseConduit .|
         mapMC throwOnEither
    where
      throwOnEither :: (MonadThrow m) => Either Text a -> m a
      throwOnEither (Left t) = throwM $ userError (T.unpack t)
      throwOnEither (Right a) = pure a

  -- this should be bracketed and so forth @todo
  handleUpdateEvent serializer awb awu (e :: Event n) = do
    let eventPath = makeEventPath (aWBConfigFSStateDir . aWBStateFSConfig $ awb)
    stE <- mkStorableEvent e
    BL.appendFile eventPath (serializer stE)
    runUpdate awu e


makeEventPath :: FilePath -> FilePath
makeEventPath fp = fp <> "/" <> "events"