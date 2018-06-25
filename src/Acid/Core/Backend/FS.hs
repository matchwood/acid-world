
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.FS where
import RIO
import qualified RIO.Directory as Dir
import qualified RIO.Text as T
import qualified  RIO.ByteString.Lazy as BL


import Prelude(userError)

import Acid.Core.Segment
import Acid.Core.Event
import Acid.Core.Backend.Abstract
import Acid.Core.Inner.Abstract
import Conduit

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
  loadEvents deserialiseConduit s = do
    let eventPath = makeEventPath (aWBConfigBackendFSStateDir . sWBStateBackendConfig $ s)


    let blSrc =
         sourceFile eventPath .|
         mapC BL.fromStrict .|
         deserialiseConduit .|
         mapMC throwOnEither
    pure blSrc

    where


      throwOnEither :: (MonadThrow m) => Either Text a -> m a
      throwOnEither (Left t) = throwM $ userError (T.unpack t)
      throwOnEither (Right a) = pure a
    {-
    a <- liftIO $ runConduitRes blSrc

    pure $ sequence a
-}
  -- this should be bracketed and so forth @todo
  handleUpdateEvent serializer awb awu (e :: Event n) = do
    let eventPath = makeEventPath (aWBConfigBackendFSStateDir . sWBStateBackendConfig $ awb)
    stE <- mkStorableEvent e
    BL.appendFile eventPath (serializer stE <> "\n")

    let (AcidWorldBackendFS m :: AcidWorldBackendFS ss nn (EventResult n)) = runUpdateEvent awu e
    liftIO $ m

makeEventPath :: FilePath -> FilePath
makeEventPath fp = fp <> "/" <> "events.json"