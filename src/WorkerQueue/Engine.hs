{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}

module WorkerQueue.Engine
  ( -- * Engine Configuration & Worker Execution
    EngineConfig (..)
  , defaultEngineConfig
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

import Control.Concurrent (ThreadId, forkIO, killThread, threadDelay)
import Control.Concurrent.STM
  ( TBQueue
  , atomically
  , newTBQueueIO
  , readTBQueue
  , writeTBQueue
  )
import Control.Exception (SomeException, bracket, displayException, fromException, try)
import Control.Monad (replicateM, when)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Maybe (catMaybes)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock.POSIX (getPOSIXTime)
import GHC.Clock (getMonotonicTimeNSec)
import Numeric.Natural (Natural)
import WorkerQueue.Database (DbPools)
import WorkerQueue.Dispatcher
  ( Dispatcher (..)
  , dispatchZeroCopy
  , prewarmSlice
  )
import WorkerQueue.Frame
  ( JobHeader (..)
  , bumpFrameRetry
  , decodeHeaderZeroCopy
  )
import WorkerQueue.Metrics
  ( Logger
  , Metrics
  , incCompleted
  , incDeadLettered
  , incEnqueued
  , incHardKilled
  , incRetried
  , logErrorIO
  , logWarnIO
  , recordJobLatencyNs
  )
import WorkerQueue.Monad
  ( JobContext (..)
  , WorkerEnv (..)
  , runWorkerM
  )
import WorkerQueue.Queue
  ( dequeueWeighted
  , enqueuePriorityFramesBatch
  , newMultiPriorityQueue
  )
import WorkerQueue.Redis
  ( RedisStore
  , acknowledgeJobAsync
  , claimExecutableBatch
  , promoteDueZSetJobs
  , rshStore
  , scheduleRetryAsync
  , sendToDlqAsync
  , startRedisSyncEngine
  , stopRedisSyncEngine
  )
import WorkerQueue.Sidekiq
  ( computeSidekiqBackoffSecs
  , frameToSidekiqJson
  , sidekiqEnvelopeToFrameWithSlices
  )
import WorkerQueue.TimeoutManager
  ( JobLeaseExpired (..)
  , registerJob
  , startTimeoutManager
  , stopTimeoutManager
  , unregisterJob
  )

data EngineConfig = EngineConfig
  { cfgWorkerCount         :: !Int
  , cfgJsonWorkerCount     :: !Int     -- ^ Dedicated JSON processing & cache pre-warming worker threads
  , cfgQueueCapacity       :: !Natural
  , cfgLeaseTimeoutSecs    :: !Double
  , cfgGracePeriodSecs     :: !Double
  , cfgSweepIntervalMicros :: !Int
  , cfgFetchBatchSize      :: !Int
  , cfgPollIntervalMicros  :: !Int
  , cfgQueues              :: ![ByteString]
  } deriving (Show, Eq)

defaultEngineConfig :: EngineConfig
defaultEngineConfig = EngineConfig
  { cfgWorkerCount         = 16
  , cfgJsonWorkerCount     = 4
  , cfgQueueCapacity       = 65_536
  , cfgLeaseTimeoutSecs    = 15.0
  , cfgGracePeriodSecs     = 2.0
  , cfgSweepIntervalMicros = 100_000
  , cfgFetchBatchSize      = 128
  , cfgPollIntervalMicros  = 2_000
  , cfgQueues              = ["critical", "default", "low"]
  }

data WorkerEngineHandle ctx = WorkerEngineHandle
  { wehEnv           :: !(WorkerEnv ctx)
  , wehDispatcher    :: !(Dispatcher ctx)
  , wehWorkerThreads :: ![ThreadId]
  , wehJsonThreads   :: ![ThreadId]
  , wehFetcherThread :: !ThreadId
  }

-- | Claim a batch of executable raw Sidekiq JSON strings from Redis queues and ZSETs.
claimRawRedisBatch
  :: WorkerEnv ctx
  -> Dispatcher ctx
  -> [ByteString]
  -> Int
  -> IO [ByteString]
claimRawRedisBatch env disp queues batchSz = do
  let store   = rshStore (envRedisSync env)
      allowed = dispClassSet disp
      metrics = envMetrics env

  nowSec      <- realToFrac <$> getPOSIXTime
  promotedRaw <- promoteDueZSetJobs store allowed nowSec

  claimedPerQ <- mapM
    (\qName -> claimExecutableBatch store metrics allowed qName batchSz)
    queues

  pure (promotedRaw ++ concat claimedPerQ)

-- | Executed by the Dedicated JSON Processing Workers:
-- 1. Zero-copy scans each raw Sidekiq JSON envelope into a 64-byte Hybrid Frame.
-- 2. Pre-decodes/caches the JSON parameters in `Dispatcher`'s Two-Level `JsonParamCache`
--    and binds the ready handler closure in `dispReadyRing`.
-- 3. Enqueues the entire batch of frames into `MultiPriorityQueue` in a single STM transaction!
ingestRawSidekiqBatch
  :: WorkerEnv ctx
  -> Dispatcher ctx
  -> [ByteString]
  -> IO Int
ingestRawSidekiqBatch _   _    []          = pure 0
ingestRawSidekiqBatch env disp rawJsonList = do
  nowNs <- fromIntegral <$> getMonotonicTimeNSec
  let !batchLen = length rawJsonList
  baseJobId <- atomicModifyIORef' (envJobCounter env) (\n -> (n + fromIntegral batchLen, n + 1))

  mFrames <- mapM
    (\(offset, rawJson) -> do
      let !jId = baseJobId + offset
      case sidekiqEnvelopeToFrameWithSlices jId nowNs rawJson of
        Right (prio, clsSlice, argsSlice, frame) -> do
          prewarmSlice disp jId clsSlice argsSlice
          pure (Just (prio, frame))
        Left err -> do
          logWarnIO (envLogger env) ("Skipping malformed Sidekiq JSON: " <> T.pack err)
          pure Nothing
    )
    (zip [0 ..] rawJsonList)

  let validFrames = catMaybes mFrames
      !validCount = length validFrames
  when (validCount > 0) $ do
    enqueuePriorityFramesBatch (envQueue env) validFrames
    incEnqueued (envMetrics env) (fromIntegral validCount)
  pure validCount

-- | Direct synchronous claim + ingest helper.
fetchAndIngestRedisBatch
  :: WorkerEnv ctx
  -> Dispatcher ctx
  -> [ByteString]
  -> Int
  -> IO Int
fetchAndIngestRedisBatch env disp queues batchSz = do
  rawBatch <- claimRawRedisBatch env disp queues batchSz
  ingestRawSidekiqBatch env disp rawBatch

-- | Execute a single 64-byte job frame with `bracket` lease registration,
-- zero-cost `ReaderT` handler execution, and automatic Sidekiq Retry/DLQ recovery.
processFrameWithTimeout
  :: WorkerEnv ctx
  -> Dispatcher ctx
  -> ByteString
  -> IO ()
processFrameWithTimeout env disp rawFrame =
  case decodeHeaderZeroCopy rawFrame of
    Left err ->
      logErrorIO (envLogger env) ("Invalid binary frame in queue: " <> T.pack err)
    Right hdr -> do
      recoveredRef <- newIORef False
      let onLeaseExpired = do
            already <- atomicModifyIORef' recoveredRef (True,)
            when (not already) $ do
              incHardKilled (envMetrics env)
              handleRetryOrDLQ env rawFrame (Just "JobLeaseExpired (Stage-2 Hard Kill)")

      bracket
        (registerJob
          (envTimeoutMgr env)
          (hdrJobId hdr)
          (hdrSidekiqJid hdr)
          (hdrTagSlice hdr)
          onLeaseExpired)
        unregisterJob
        (\ticket -> do
          let jctx = JobContext
                { jcEnv    = env
                , jcTicket = ticket
                , jcHeader = hdr
                }
          res <- try (runWorkerM jctx (dispatchZeroCopy disp hdr (hdrPayloadSlice hdr)))
          case res of
            Right () -> do
              alreadyExpired <- readIORef recoveredRef
              when (not alreadyExpired) $ do
                nowNs <- fromIntegral <$> getMonotonicTimeNSec
                let durNs = nowNs - hdrCreatedAtNs hdr
                recordJobLatencyNs (envMetrics env) (hdrPriority hdr) durNs
                incCompleted (envMetrics env)
                acknowledgeJobAsync (envRedisSync env) (hdrSidekiqJid hdr)
            Left (ex :: SomeException) ->
              case fromException ex of
                Just (_ :: JobLeaseExpired) ->
                  -- Recovery already triggered by `onLeaseExpired` inside `triggerHardKill`
                  pure ()
                Nothing -> do
                  alreadyExpired <- atomicModifyIORef' recoveredRef (True,)
                  when (not alreadyExpired) $
                    handleRetryOrDLQ env rawFrame (Just (T.pack (displayException ex)))
        )
{-# INLINE processFrameWithTimeout #-}

-- | Increment `retryCount` and either schedule exponential backoff in Redis `retry` ZSET
-- or route the job to the Redis `dead` (DLQ) ZSET if `retryCount > maxRetries`.
handleRetryOrDLQ :: WorkerEnv ctx -> ByteString -> Maybe Text -> IO ()
handleRetryOrDLQ env rawFrame mErrMsg =
  case bumpFrameRetry rawFrame of
    Left err ->
      logErrorIO (envLogger env) ("handleRetryOrDLQ failed: " <> T.pack err)
    Right (nextRetry, maxRet, bumpedFrame) -> do
      nowSec <- realToFrac <$> getPOSIXTime
      let jid = case decodeHeaderZeroCopy bumpedFrame of
                  Right h -> hdrSidekiqJid h
                  Left _  -> "000000000000000000000000"
      case frameToSidekiqJson bumpedFrame mErrMsg of
        Left err ->
          logErrorIO (envLogger env) ("frameToSidekiqJson failed: " <> T.pack err)
        Right skJson ->
          if nextRetry <= maxRet
            then do
              backoffSec <- computeSidekiqBackoffSecs nextRetry
              let nextRunAt = nowSec + backoffSec
              incRetried (envMetrics env)
              scheduleRetryAsync (envRedisSync env) jid nextRunAt skJson
            else do
              incDeadLettered (envMetrics env)
              sendToDlqAsync (envRedisSync env) jid nowSec skJson

-- | Continuous execution worker loop running `WorkerM ctx` handlers.
workerLoop :: WorkerEnv ctx -> Dispatcher ctx -> IO ()
workerLoop env disp = go
  where
    go = do
      rawFrame <- dequeueWeighted (envQueue env)
      processFrameWithTimeout env disp rawFrame
      go

-- | Dedicated JSON Processing Worker Loop:
-- Pulls raw claimed Sidekiq JSON batches from `rawJsonQueue`, slices them into
-- 64-byte frames, pre-decodes/caches JSON parameters in `JsonParamCache`,
-- and batch-enqueues ready frames into `MultiPriorityQueue`.
jsonProcessorWorkerLoop
  :: WorkerEnv ctx
  -> Dispatcher ctx
  -> TBQueue [ByteString]
  -> IO ()
jsonProcessorWorkerLoop env disp rawJsonQueue = go
  where
    go = do
      rawBatch <- atomically (readTBQueue rawJsonQueue)
      _ <- ingestRawSidekiqBatch env disp rawBatch
      go

-- | Continuous background fetcher loop claiming executable raw batches from Redis/Valkey
-- and handing them off to the Dedicated JSON Processing Worker Pool.
fetcherLoop
  :: EngineConfig
  -> WorkerEnv ctx
  -> Dispatcher ctx
  -> TBQueue [ByteString]
  -> IO ()
fetcherLoop cfg env disp rawJsonQueue = go
  where
    go = do
      rawBatch <- claimRawRedisBatch env disp (cfgQueues cfg) (cfgFetchBatchSize cfg)
      if null rawBatch
        then threadDelay (cfgPollIntervalMicros cfg)
        else atomically (writeTBQueue rawJsonQueue rawBatch)
      go

startWorkerEngine
  :: EngineConfig
  -> RedisStore
  -> DbPools
  -> Logger
  -> Metrics
  -> ctx
  -> Dispatcher ctx
  -> IO (WorkerEngineHandle ctx)
startWorkerEngine cfg store dbPools logger metrics appCtx disp = do
  tm           <- startTimeoutManager (cfgLeaseTimeoutSecs cfg) (cfgGracePeriodSecs cfg) (cfgSweepIntervalMicros cfg)
  redisSync    <- startRedisSyncEngine store
  mpq          <- newMultiPriorityQueue (cfgQueueCapacity cfg)
  rawJsonQueue <- newTBQueueIO 1_024
  jCounter     <- newIORef 0
  let env = WorkerEnv
        { envLogger     = logger
        , envMetrics    = metrics
        , envDbPools    = dbPools
        , envTimeoutMgr = tm
        , envRedisSync  = redisSync
        , envQueue      = mpq
        , envJobCounter = jCounter
        , envAppContext = appCtx
        }
      nJsonWorkers = max 1 (cfgJsonWorkerCount cfg)
  fetcherTid <- forkIO (fetcherLoop cfg env disp rawJsonQueue)
  jsonTids   <- replicateM nJsonWorkers (forkIO (jsonProcessorWorkerLoop env disp rawJsonQueue))
  workerTids <- replicateM (cfgWorkerCount cfg) (forkIO (workerLoop env disp))
  pure WorkerEngineHandle
    { wehEnv           = env
    , wehDispatcher    = disp
    , wehWorkerThreads = workerTids
    , wehJsonThreads   = jsonTids
    , wehFetcherThread = fetcherTid
    }

stopWorkerEngine :: WorkerEngineHandle ctx -> IO ()
stopWorkerEngine hdl = do
  killThread (wehFetcherThread hdl)
  mapM_ killThread (wehJsonThreads hdl)
  mapM_ killThread (wehWorkerThreads hdl)
  stopTimeoutManager (envTimeoutMgr (wehEnv hdl))
  stopRedisSyncEngine (envRedisSync (wehEnv hdl))

withWorkerEngine
  :: EngineConfig
  -> RedisStore
  -> DbPools
  -> Logger
  -> Metrics
  -> ctx
  -> Dispatcher ctx
  -> (WorkerEngineHandle ctx -> IO a)
  -> IO a
withWorkerEngine cfg store dbPools logger metrics appCtx disp =
  bracket
    (startWorkerEngine cfg store dbPools logger metrics appCtx disp)
    stopWorkerEngine
