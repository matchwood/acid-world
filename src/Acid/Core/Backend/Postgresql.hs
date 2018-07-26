
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.Postgresql where
import RIO
import Conduit

import Database.PostgreSQL.Simple as PSQL

import Acid.Core.Serialise.Postgresql
import Acid.Core.State
import Acid.Core.Utils
import Acid.Core.Backend.Abstract

data AcidWorldBackendPostgresql





instance AcidWorldBackend AcidWorldBackendPostgresql where
  data AWBState AcidWorldBackendPostgresql = AWBStatePostgresql {awbStatePostgresqlConnection :: Connection}
  data AWBConfig AcidWorldBackendPostgresql = AWBConfigPostgresql {awvConfigPostgresqlConnectionConfig :: ConnectInfo} deriving Show
  type AWBSerialiseT AcidWorldBackendPostgresql = [PostgresRow]
  type AWBSerialiseConduitT AcidWorldBackendPostgresql = [PostgresRow]
  initialiseBackend c _  = do
    p <- liftIO $ PSQL.connect (awvConfigPostgresqlConnectionConfig c)
    pure . pure $ AWBStatePostgresql p
  closeBackend s = liftIO $ close (awbStatePostgresqlConnection s)
  createCheckpointBackend _ _ _ = pure ()
  getInitialState defState _ _ = pure . pure $ defState
  loadEvents deserialiseConduit s _ = pure . pure $ LoadEventsConduit $ \restConduit -> liftIO $
    runConduitRes $
      sourceDatabase .|
      deserialiseConduit .|
      restConduit


  handleUpdateEventC serializer s awu _ ec act = do
    eBind (runUpdateC awu ec) $ \(es, r, onSuccess, onFail) -> do
      stEs <- mkStorableEvents es
      ioR <- act r
      let rows = serializer stEs
      successIO <- toIO onSuccess
      failIO <- toIO onFail

      liftIO $ withTransaction  (awbStatePostgresqlConnection s) $ do
        res <- executeMany (awbStatePostgresqlConnection s) "insert into storableEvent VALUES (?,?,?,?,?)" rows

        if (fromIntegral res) /= length rows
          then do
            failIO
            throwIO $ AWExceptionEventSerialisationError $ "Expected to write " <> showT (length rows) <> " but only wrote " <> showT res
          else do
            successIO
            pure . Right $ (r, ioR)

