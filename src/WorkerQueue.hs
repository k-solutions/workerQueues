{-# LANGUAGE GHC2024 #-}

module WorkerQueue
  ( -- * Zero-Copy 64-Byte Frame & Typed `JobPayload` Codec (`WorkerQueue.Frame`)
    Priority (..)
  , Source (..)
  , toQueueName
  , parsePriority
  , headerSize
  , magic
  , version
  , Params (..)
  , Header (..)
  , encode
  , decodeHeader
  , bumpRetry
  , JobPayload (..)
  , encodeJob

    -- * `WorkerM ctx` (`ReaderT (JobContext ctx) IO`) & Combinators
  , WorkerEnv (..)
  , JobContext (..)
  , WorkerM (..)
  , runWorkerM
  , JobHandler
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

    -- * Capability-Aware Dispatcher & Registration
  , HandlerRegistration
  , Dispatcher (..)
  , registerWorker
  , newDispatcher
  , registeredClassTags
  , dispatchZeroCopy
  , getDispatcherCacheStats

    -- * Zero-Copy JSON Scanner & Two-Level Parameter Cache (`WorkerQueue.Json`)
  , FastSidekiqScan (..)
  , scanJsonStringField
  , scanJsonIntField
  , scanSidekiqClassFast
  , scanSidekiqJidFast
  , scanSidekiqArgsSlice
  , scanSidekiqEnvelopeFast
  , JsonParamCache (..)
  , JsonCacheStats (..)
  , newJsonParamCache
  , decodeWithParamCache
  , lookupPrewarmedOrDecode
  , getJsonCacheStats

    -- * Two-Stage Warp-Style Timeout Manager
  , TimeoutManager (..)
  , JobTicket (..)
  , JobLeaseExpired (..)
  , JobState (..)
  , startTimeoutManager
  , stopTimeoutManager
  , sweepAndSwap
  , registerJob
  , tickleJob
  , checkLeaseState
  , unregisterJob

    -- * Multi-Priority In-Memory `TBQueue`
  , MultiPriorityQueue (..)
  , QueueDepths (..)
  , newMultiPriorityQueue
  , enqueueFrame
  , enqueueFramesBatch
  , tryEnqueueFrame
  , dequeueWeighted
  , tryDequeueWeighted
  , getQueueDepths
  , isQueueEmpty

    -- * Sidekiq / Rails JSON Wire Interoperability
  , SidekiqEnvelope (..)
  , extractSidekiqClass
  , parseSidekiqEnvelope
  , sidekiqEnvelopeToFrame
  , frameToSidekiqJson
  , encodeSidekiqJobJson
  , computeSidekiqBackoffSecs

    -- * Capability-Aware Redis/Valkey Store & Async Sync Engine
  , claimExecutableBatchLua
  , promoteExecutableZSetLua
  , RedisStore (..)
  , ZSetEntry (..)
  , ClaimBatchParams (..)
  , newRedisStore
  , pushSidekiqQueueJob
  , pushSidekiqScheduledJob
  , claimExecutableBatch
  , promoteDueZSetJobs
  , acquireCronLock
  , getQueueItems
  , getWorkingLeases
  , getRetryZSet
  , getDeadZSet
  , RedisOp (..)
  , RedisSyncHandle (..)
  , startRedisSyncEngine
  , stopRedisSyncEngine
  , flushRedisSyncNow
  , acknowledgeJobAsync
  , scheduleRetryAsync
  , sendToDlqAsync
  , recordWorkingLeaseAsync

    -- * Built-In YAML Cron Scheduler (`config/schedule.yml`)
  , Field (..)
  , Expr (..)
  , JobSpec (..)
  , ScheduleFile (..)
  , CronM
  , runCronM
  , fromWorkerEnv
  , parseExpr
  , matchesTime
  , parseScheduleYaml
  , loadScheduleFile
  , validateCapabilities
  , executeTick

    -- * Database Connection Pools (`beam-postgres` & `beam-sqlite`)
  , DbPools (..)
  , emptyDbPools
  , newPostgresPool
  , newSqliteWalPool
  , closeDbPools
  , withPostgresConn
  , withSqliteConn
  , runBeamPostgresPool
  , runBeamSqlitePool

    -- * Telemetry, Latency Histograms & Structured Logging
  , Logger (..)
  , LogLevel (..)
  , newStderrLogger
  , newSilentLogger
  , logInfoIO
  , logWarnIO
  , logErrorIO
  , Metrics (..)
  , MetricsSnapshot (..)
  , LatencySummary (..)
  , newMetrics
  , snapshotMetrics
  , computeLatencySummary

    -- * Worker Engine Lifecycle (`WorkerQueue.Engine`)
  , EngineConfig (..)
  , defaultEngineConfig
  , EngineParams (..)
  , EngineEnv (..)
  , EngineM
  , runEngineM
  , askWorkerEnv
  , askDispatcher
  , flushEngineRedisSync
  , WorkerEngineHandle (..)
  , claimRawRedisBatch
  , ingestRawSidekiqBatch
  , fetchAndIngestRedisBatch
  , processFrameWithTimeout
  , handleRetryOrDLQ
  , startWorkerEngine
  , stopWorkerEngine
  , withWorkerEngine
  ) where

import WorkerQueue.Cron
import WorkerQueue.Database
import WorkerQueue.Frame
import WorkerQueue.Json
import WorkerQueue.Metrics
import WorkerQueue.Queue
import WorkerQueue.Redis
import WorkerQueue.Sidekiq
import WorkerQueue.TimeoutManager
import WorkerQueue.Worker
