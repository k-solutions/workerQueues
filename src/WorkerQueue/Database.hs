{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}

module WorkerQueue.Database
  ( DbPools (..)
  , emptyDbPools
  , newPostgresPool
  , newSqliteWalPool
  , closeDbPools
  , withPostgresConn
  , withSqliteConn
  , runBeamPostgresPool
  , runBeamSqlitePool
  ) where

import Control.Concurrent (getNumCapabilities)
import Control.Exception (throwIO)
import Data.ByteString (ByteString)
import Data.Pool
  ( Pool
  , defaultPoolConfig
  , destroyAllResources
  , newPool
  , setNumStripes
  , withResource
  )
import Database.Beam.Postgres (Pg, runBeamPostgres)
import Database.Beam.Sqlite (SqliteM, runBeamSqlite)
import Database.PostgreSQL.Simple qualified as Pg
import Database.SQLite.Simple qualified as SQLite

-- | Resource pools for PostgreSQL (`beam-postgres`) and SQLite WAL (`beam-sqlite`).
data DbPools = DbPools
  { pgPool     :: !(Maybe (Pool Pg.Connection))
  , sqlitePool :: !(Maybe (Pool SQLite.Connection))
  }

emptyDbPools :: DbPools
emptyDbPools = DbPools
  { pgPool     = Nothing
  , sqlitePool = Nothing
  }

-- | Create a PostgreSQL connection pool from a libpq connection string.
newPostgresPool :: ByteString -> Double -> Int -> IO (Pool Pg.Connection)
newPostgresPool connStr idleSecs maxConns = do
  caps <- getNumCapabilities
  let nConns   = max 1 maxConns
      nStripes = max 1 (min nConns caps)
      cfg      = setNumStripes (Just nStripes) (defaultPoolConfig (Pg.connectPostgreSQL connStr) Pg.close idleSecs nConns)
  newPool cfg

-- | Create a high-concurrency SQLite connection pool configured with WAL mode,
-- `synchronous = NORMAL`, and `busy_timeout = 5000`.
-- Uses a single GHC-managed pool stripe to serialize connection checkout cleanly
-- without triggering C-level SQLite writer lock (`SQLITE_BUSY`) sleep backoff.
newSqliteWalPool :: FilePath -> Double -> Int -> IO (Pool SQLite.Connection)
newSqliteWalPool dbPath idleSecs maxConns = do
  let nConns = max 1 maxConns
      cfg    = setNumStripes (Just 1) (defaultPoolConfig openWal SQLite.close idleSecs nConns)
  newPool cfg
  where
    openWal = do
      conn <- SQLite.open dbPath
      SQLite.execute_ conn "PRAGMA journal_mode = WAL;"
      SQLite.execute_ conn "PRAGMA synchronous = NORMAL;"
      SQLite.execute_ conn "PRAGMA busy_timeout = 5000;"
      pure conn

closeDbPools :: DbPools -> IO ()
closeDbPools pools = do
  maybe (pure ()) destroyAllResources (pgPool pools)
  maybe (pure ()) destroyAllResources (sqlitePool pools)

withPostgresConn :: DbPools -> (Pg.Connection -> IO a) -> IO a
withPostgresConn pools action =
  case pgPool pools of
    Just pool -> withResource pool action
    Nothing   -> throwIO (userError "withPostgresConn: PostgreSQL pool is not configured in DbPools")
{-# INLINE withPostgresConn #-}

withSqliteConn :: DbPools -> (SQLite.Connection -> IO a) -> IO a
withSqliteConn pools action =
  case sqlitePool pools of
    Just pool -> withResource pool action
    Nothing   -> throwIO (userError "withSqliteConn: SQLite pool is not configured in DbPools")
{-# INLINE withSqliteConn #-}

runBeamPostgresPool :: DbPools -> Pg a -> IO a
runBeamPostgresPool pools pgAction =
  withPostgresConn pools $ \conn ->
    runBeamPostgres conn pgAction
{-# INLINE runBeamPostgresPool #-}

runBeamSqlitePool :: DbPools -> SqliteM a -> IO a
runBeamSqlitePool pools sqliteAction =
  withSqliteConn pools $ \conn ->
    runBeamSqlite conn sqliteAction
{-# INLINE runBeamSqlitePool #-}
