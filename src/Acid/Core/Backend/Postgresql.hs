
{-# LANGUAGE UndecidableInstances #-}

module Acid.Core.Backend.Postgresql where
import RIO
import qualified RIO.List as L
import Conduit

import Database.PostgreSQL.Simple as PSQL
import Database.PostgreSQL.Simple.ToField
import Database.PostgreSQL.Simple.ToRow


import Generics.SOP
import Generics.SOP.NP
import qualified  Data.Vinyl.Derived as V

import Acid.Core.Serialise.Abstract
import Acid.Core.Serialise.Postgresql
import Acid.Core.State
import Acid.Core.Utils
import Acid.Core.Backend.Abstract

data AcidWorldBackendPostgresql





instance AcidWorldBackend AcidWorldBackendPostgresql where
  data AWBState AcidWorldBackendPostgresql = AWBStatePostgresql {awbStatePostgresqlConnection :: Connection}
  data AWBConfig AcidWorldBackendPostgresql = AWBConfigPostgresql {awvConfigPostgresqlConnectionConfig :: ConnectInfo} deriving Show
  type AWBSerialiseT AcidWorldBackendPostgresql = [PostgresRow]
  type AWBSerialiseConduitT AcidWorldBackendPostgresql = PostgresConduitT
  type AWBSerialiseSegmentT AcidWorldBackendPostgresql = (String, PostgresRow)
  initialiseBackend c _  = do
    p <- liftIO $ PSQL.connect (awvConfigPostgresqlConnectionConfig c)
    pure . pure $ AWBStatePostgresql p
  closeBackend s = liftIO $ close (awbStatePostgresqlConnection s)
  createCheckpointBackend s awu t = do
    st <- runQuery awu askStateNp
    insertCheckpoint s t st



  getInitialState defState _ _ = pure . pure $ defState
  loadEvents deserialiseConduit s _ = pure . pure $ LoadEventsConduit $ \restConduit -> liftIO $
    runConduitRes $
      sourceDatabase .|
      deserialiseConduit .|
      restConduit
    where
      sourceDatabase :: ConduitT i PostgresConduitT (ResourceT IO) ()
      sourceDatabase = do
        -- the api for Postgresql-simple does not really allow composing a conduit from a fold or similar, though it should be easy
        res <- liftIO $ query_ (awbStatePostgresqlConnection s) "select * from storableEvent ORDER BY id ASC"
        yieldMany $ res

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


insertCheckpoint :: forall t sFields m. ( AcidSerialiseSegmentT t ~ (String, PostgresRow), MonadUnliftIO m, All (AcidSerialiseSegmentFieldConstraint t) sFields)  => AWBState AcidWorldBackendPostgresql -> AcidSerialiseEventOptions t -> NP V.ElField sFields -> m ()
insertCheckpoint s t np = do

  let acts =  cfoldMap_NP (Proxy :: Proxy (AcidSerialiseSegmentFieldConstraint t)) ((:[]) . insertSegment s t) np
  sequence_ acts


insertSegment :: forall t m fs. ( AcidSerialiseSegmentT t ~ (String, PostgresRow), MonadUnliftIO m, AcidSerialiseSegmentFieldConstraint t fs) => AWBState AcidWorldBackendPostgresql -> AcidSerialiseEventOptions t -> V.ElField fs -> m ()
insertSegment s t (V.Field seg) = do

  runConduitRes $
    serialiseSegment t seg .|
    sinkDatabase

  where
    sinkDatabase :: ConduitT (String, PostgresRow) o (ResourceT m) ()
    sinkDatabase = awaitForever loop
      where
        loop :: (String, PostgresRow) -> ConduitT (String, PostgresRow) o (ResourceT m) ()
        loop (tbl, (PostgresRow a)) = do
          let q = mconcat ["insert into ", fromString tbl, " values ( ", valueFill, ")"]
          void $ liftIO $ execute (awbStatePostgresqlConnection s) q rowActions

          where
            valueFill :: Query
            valueFill = fromString $ L.intercalate "," $ replicate (length rowActions) "?"
            rowActions :: [Action]
            rowActions = toRow a



