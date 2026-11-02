{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}

module WorkerQueue.Monad
  ( -- * Environment & ReaderT Handler Monad (`WorkerM ctx`)
    WorkerEnv (..)
  , JobContext (..)
  , WorkerM (..)
  , runWorkerM
  , JobHandler

    -- * Ergonomic Combinators in `WorkerM ctx`
  , getAppContext
  , getWorkerEnv
  , getJobHeader
  , getJobTicket
  , tickleLease
  , checkLease
  , withPostgres
  , withSqlite
  , runPgBeam
  , runSqliteBeam
  , logInfo
  , logWarn
  , logError
  ) where

import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, ReaderT (..), asks)
import Data.IORef (IORef)
import Data.Text (Text)
import Data.Word (Word64)
import Database.Beam.Postgres (Pg)
import Database.Beam.Sqlite (SqliteM)
import Database.PostgreSQL.Simple qualified as Pg
import Database.SQLite.Simple qualified as SQLite
import WorkerQueue.Database
  ( DbPools
  , runBeamPostgresPool
  , runBeamSqlitePool
  , withPostgresConn
  , withSqliteConn
  )
import WorkerQueue.Frame (JobHeader)
import WorkerQueue.Metrics
  ( Logger
  , Metrics
  , logErrorIO
  , logInfoIO
  , logWarnIO
  )
import WorkerQueue.Queue (MultiPriorityQueue)
import WorkerQueue.Redis (RedisSyncHandle)
import WorkerQueue.TimeoutManager
  ( JobState
  , JobTicket
  , TimeoutManager
  , checkLeaseState
  , tickleJob
  )

-- | Shared runtime environment parameterized over downstream application context `ctx`.
data WorkerEnv ctx = WorkerEnv
  { envLogger     :: !Logger
  , envMetrics    :: !Metrics
  , envDbPools    :: !DbPools
  , envTimeoutMgr :: !TimeoutManager
  , envRedisSync  :: !RedisSyncHandle
  , envQueue      :: !MultiPriorityQueue
  , envJobCounter :: !(IORef Word64)
  , envAppContext :: !ctx
  }

-- | Per-job execution context supplied to `WorkerM ctx` via `ReaderT`.
data JobContext ctx = JobContext
  { jcEnv    :: !(WorkerEnv ctx)
  , jcTicket :: !JobTicket
  , jcHeader :: !JobHeader
  }

-- | Zero-cost `newtype` over `ReaderT (JobContext ctx) IO`.
newtype WorkerM ctx a = WorkerM
  { unWorkerM :: ReaderT (JobContext ctx) IO a
  } deriving newtype
      ( Functor
      , Applicative
      , Monad
      , MonadIO
      , MonadReader (JobContext ctx)
      , MonadThrow
      , MonadCatch
      , MonadMask
      )

runWorkerM :: JobContext ctx -> WorkerM ctx a -> IO a
runWorkerM jctx (WorkerM r) = runReaderT r jctx
{-# INLINE runWorkerM #-}

-- | Single-argument typed handler signature.
type JobHandler ctx a = a -> WorkerM ctx ()

--------------------------------------------------------------------------------
-- Ergonomic Zero-Boilerplate Combinators in `WorkerM ctx`
--------------------------------------------------------------------------------

getAppContext :: WorkerM ctx ctx
getAppContext = asks (envAppContext . jcEnv)
{-# INLINE getAppContext #-}

getWorkerEnv :: WorkerM ctx (WorkerEnv ctx)
getWorkerEnv = asks jcEnv
{-# INLINE getWorkerEnv #-}

getJobHeader :: WorkerM ctx JobHeader
getJobHeader = asks jcHeader
{-# INLINE getJobHeader #-}

getJobTicket :: WorkerM ctx JobTicket
getJobTicket = asks jcTicket
{-# INLINE getJobTicket #-}

-- | Renew the current job's lease heartbeat in the Warp-style `TimeoutManager`.
tickleLease :: WorkerM ctx ()
tickleLease = do
  t <- asks jcTicket
  liftIO (tickleJob t)
{-# INLINE tickleLease #-}

-- | Inspect the current job's lease state (e.g., to abort cleanly on Stage-1 `SoftExpired`).
checkLease :: WorkerM ctx JobState
checkLease = do
  t <- asks jcTicket
  liftIO (checkLeaseState t)
{-# INLINE checkLease #-}

-- | Borrow a PostgreSQL connection from `envDbPools` inside `WorkerM ctx`.
withPostgres :: (Pg.Connection -> IO b) -> WorkerM ctx b
withPostgres action = do
  pools <- asks (envDbPools . jcEnv)
  liftIO (withPostgresConn pools action)
{-# INLINE withPostgres #-}

-- | Borrow a SQLite WAL connection from `envDbPools` inside `WorkerM ctx`.
withSqlite :: (SQLite.Connection -> IO b) -> WorkerM ctx b
withSqlite action = do
  pools <- asks (envDbPools . jcEnv)
  liftIO (withSqliteConn pools action)
{-# INLINE withSqlite #-}

-- | Execute a `beam-postgres` `Pg` action using a pooled connection.
runPgBeam :: Pg b -> WorkerM ctx b
runPgBeam pgAction = do
  pools <- asks (envDbPools . jcEnv)
  liftIO (runBeamPostgresPool pools pgAction)
{-# INLINE runPgBeam #-}

-- | Execute a `beam-sqlite` `SqliteM` action using a pooled SQLite WAL connection.
runSqliteBeam :: SqliteM b -> WorkerM ctx b
runSqliteBeam sqliteAction = do
  pools <- asks (envDbPools . jcEnv)
  liftIO (runBeamSqlitePool pools sqliteAction)
{-# INLINE runSqliteBeam #-}

logInfo :: Text -> WorkerM ctx ()
logInfo msg = do
  l <- asks (envLogger . jcEnv)
  liftIO (logInfoIO l msg)
{-# INLINE logInfo #-}

logWarn :: Text -> WorkerM ctx ()
logWarn msg = do
  l <- asks (envLogger . jcEnv)
  liftIO (logWarnIO l msg)
{-# INLINE logWarn #-}

logError :: Text -> WorkerM ctx ()
logError msg = do
  l <- asks (envLogger . jcEnv)
  liftIO (logErrorIO l msg)
{-# INLINE logError #-}
