{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE RecordWildCards #-}

module WorkerQueue.Engine
  ( -- * Engine Configuration & `ReaderT` Monad (`EngineM ctx`)
    EngineConfig (..)
  , defaultEngineConfig
  , EngineParams (..)
  , EngineEnv (..)
  , EngineM
  , runEngineM
  , askWorkerEnv
  , askDispatcher
  , flushEngineRedisSync
  , WorkerEngineHandle (..)

    -- * Worker Execution & Batch Ingestion (`EngineM ctx`)
  , claimRawRedisBatch
  , ingestRawSidekiqBatch
  , fetchAndIngestRedisBatch
  , processFrameWithTimeout
  , handleRetryOrDLQ

    -- * Engine Lifecycle
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
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Reader (ReaderT (..), ask, asks)
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
  ( Header (..)
  , bumpRetry
  , decodeHeader
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
  ( ClaimBatchParams (..)
  , RedisStore
  , acknowledgeJobAsync
  , claimExecutableBatch
  , flushRedisSyncNow
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

-- | Parameters for starting and running the worker engine.
data EngineParams ctx = EngineParams
  { epConfig     :: !EngineConfig
  , epRedisStore :: !RedisStore
  , epDbPools    :: !DbPools
  , epLogger     :: !Logger
  , epMetrics    :: !Metrics
  , epAppContext :: !ctx
  , epDispatcher :: !(Dispatcher ctx)
  }

-- | Shared environment for `EngineM ctx` combining `WorkerEnv ctx` and `Dispatcher ctx`.
data EngineEnv ctx = EngineEnv
  { eeEnv        :: !(WorkerEnv ctx)
  , eeDispatcher :: !(Dispatcher ctx)
  }

type EngineM ctx = ReaderT (EngineEnv ctx) IO

runEngineM :: EngineEnv ctx -> EngineM ctx a -> IO a
runEngineM env action = runReaderT action env
{-# INLINE runEngineM #-}

askWorkerEnv :: EngineM ctx (WorkerEnv ctx)
askWorkerEnv = asks eeEnv
{-# INLINE askWorkerEnv #-}

askDispatcher :: EngineM ctx (Dispatcher ctx)
askDispatcher = asks eeDispatcher
{-# INLINE askDispatcher #-}

flushEngineRedisSync :: EngineM ctx ()
flushEngineRedisSync = do
  syncHdl <- asks (envRedisSync . eeEnv)
  liftIO (flushRedisSyncNow syncHdl)
{-# INLINE flushEngineRedisSync #-}

data WorkerEngineHandle ctx = WorkerEngineHandle
  { wehEnv           :: !(WorkerEnv ctx)
  , wehDispatcher    :: !(Dispatcher ctx)
  , wehWorkerThreads :: ![ThreadId]
  , wehJsonThreads   :: ![ThreadId]
  , wehFetcherThread :: !ThreadId
  }

-- | Claim a batch of executable raw Sidekiq JSON strings from Redis queues and ZSETs.
claimRawRedisBatch :: [ByteString] -> Int -> EngineM ctx [ByteString]
claimRawRedisBatch queues batchSz = do
  EngineEnv {..} <- ask
  liftIO $ do
    let store   = rshStore (envRedisSync eeEnv)
        allowed = dispClassSet eeDispatcher
        metrics = envMetrics eeEnv

    nowSec      <- realToFrac <$> getPOSIXTime
    promotedRaw <- promoteDueZSetJobs store allowed nowSec

    claimedPerQ <- mapM
      (\qName ->
        claimExecutableBatch ClaimBatchParams
          { cbpStore          = store
          , cbpMetrics        = metrics
          , cbpAllowedClasses = allowed
          , cbpQueueName      = qName
          , cbpMaxClaim       = batchSz
          }
      )
      queues

    pure (promotedRaw ++ concat claimedPerQ)

-- | Executed by the Dedicated JSON Processing Workers:
-- 1. Zero-copy scans each raw Sidekiq JSON envelope into a 64-byte Hybrid Frame.
-- 2. Pre-decodes/caches the JSON parameters in `Dispatcher`'s Two-Level `JsonParamCache`
--    and binds the ready handler closure in `dispReadyRing`.
-- 3. Enqueues the entire batch of frames into `MultiPriorityQueue` in a single STM transaction!
ingestRawSidekiqBatch :: [ByteString] -> EngineM ctx Int
ingestRawSidekiqBatch []          = pure 0
ingestRawSidekiqBatch rawJsonList = do
  EngineEnv {..} <- ask
  liftIO $ do
    nowNs <- fromIntegral <$> getMonotonicTimeNSec
    let !batchLen = length rawJsonList
    baseJobId <- atomicModifyIORef' (envJobCounter eeEnv) (\n -> (n + fromIntegral batchLen, n + 1))

    mFrames <- mapM
      (\(offset, rawJson) -> do
        let !jId = baseJobId + offset
        case sidekiqEnvelopeToFrameWithSlices jId nowNs rawJson of
          Right (prio, clsSlice, argsSlice, frame) -> do
            prewarmSlice eeDispatcher jId clsSlice argsSlice
            pure (Just (prio, frame))
          Left err -> do
            logWarnIO (envLogger eeEnv) ("Skipping malformed Sidekiq JSON: " <> T.pack err)
            pure Nothing
      )
      (zip [0 ..] rawJsonList)

    let validFrames = catMaybes mFrames
        !validCount = length validFrames
    when (validCount > 0) $ do
      enqueuePriorityFramesBatch (envQueue eeEnv) validFrames
      incEnqueued (envMetrics eeEnv) (fromIntegral validCount)
    pure validCount

-- | Direct synchronous claim + ingest helper in `EngineM ctx`.
fetchAndIngestRedisBatch :: [ByteString] -> Int -> EngineM ctx Int
fetchAndIngestRedisBatch queues batchSz = do
  rawBatch <- claimRawRedisBatch queues batchSz
  ingestRawSidekiqBatch rawBatch

-- | Execute a single 64-byte job frame with `bracket` lease registration,
-- zero-cost `ReaderT` handler execution, and automatic Sidekiq Retry/DLQ recovery.
processFrameWithTimeout :: ByteString -> EngineM ctx ()
processFrameWithTimeout rawFrame = do
  eenv@(EngineEnv {..}) <- ask
  liftIO $
    case decodeHeader rawFrame of
      Left err ->
        logErrorIO (envLogger eeEnv) ("Invalid binary frame in queue: " <> T.pack err)
      Right hdr -> do
        recoveredRef <- newIORef False
        let onLeaseExpired = do
              already <- atomicModifyIORef' recoveredRef (True,)
              when (not already) $ do
                incHardKilled (envMetrics eeEnv)
                runEngineM eenv (handleRetryOrDLQ rawFrame (Just "JobLeaseExpired (Stage-2 Hard Kill)"))

        bracket
          (registerJob
            (envTimeoutMgr eeEnv)
            (hdrJobId hdr)
            (hdrSidekiqJid hdr)
            (hdrTagSlice hdr)
            onLeaseExpired)
          unregisterJob
          (\ticket -> do
            let jctx = JobContext
                  { jcEnv    = eeEnv
                  , jcTicket = ticket
                  , jcHeader = hdr
                  }
            res <- try (runWorkerM jctx (dispatchZeroCopy eeDispatcher hdr (hdrPayloadSlice hdr)))
            case res of
              Right () -> do
                alreadyExpired <- readIORef recoveredRef
                when (not alreadyExpired) $ do
                  nowNs <- fromIntegral <$> getMonotonicTimeNSec
                  let durNs = nowNs - hdrCreatedAtNs hdr
                  recordJobLatencyNs (envMetrics eeEnv) (hdrPriority hdr) durNs
                  incCompleted (envMetrics eeEnv)
                  acknowledgeJobAsync (envRedisSync eeEnv) (hdrSidekiqJid hdr)
              Left (ex :: SomeException) ->
                case fromException ex of
                  Just (_ :: JobLeaseExpired) ->
                    -- Recovery already triggered by `onLeaseExpired` inside `triggerHardKill`
                    pure ()
                  Nothing -> do
                    alreadyExpired <- atomicModifyIORef' recoveredRef (True,)
                    when (not alreadyExpired) $
                      runEngineM eenv (handleRetryOrDLQ rawFrame (Just (T.pack (displayException ex))))
          )
{-# INLINE processFrameWithTimeout #-}

-- | Increment `retryCount` and either schedule exponential backoff in Redis `retry` ZSET
-- or route the job to the Redis `dead` (DLQ) ZSET if `retryCount > maxRetries`.
handleRetryOrDLQ :: ByteString -> Maybe Text -> EngineM ctx ()
handleRetryOrDLQ rawFrame mErrMsg = do
  env <- askWorkerEnv
  liftIO $
    case bumpRetry rawFrame of
      Left err ->
        logErrorIO (envLogger env) ("handleRetryOrDLQ failed: " <> T.pack err)
      Right (nextRetry, maxRet, bumpedFrame) -> do
        nowSec <- realToFrac <$> getPOSIXTime
        let jid = case decodeHeader bumpedFrame of
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
workerLoop :: EngineM ctx ()
workerLoop = do
  q <- asks (envQueue . eeEnv)
  go q
  where
    go q = do
      rawFrame <- liftIO (dequeueWeighted q)
      processFrameWithTimeout rawFrame
      go q

-- | Dedicated JSON Processing Worker Loop:
-- Pulls raw claimed Sidekiq JSON batches from `rawJsonQueue`, slices them into
-- 64-byte frames, pre-decodes/caches JSON parameters in `JsonParamCache`,
-- and batch-enqueues ready frames into `MultiPriorityQueue`.
jsonProcessorWorkerLoop :: TBQueue [ByteString] -> EngineM ctx ()
jsonProcessorWorkerLoop rawJsonQueue = go
  where
    go = do
      rawBatch <- liftIO $ atomically (readTBQueue rawJsonQueue)
      _ <- ingestRawSidekiqBatch rawBatch
      go

-- | Continuous background fetcher loop claiming executable raw batches from Redis/Valkey
-- and handing them off to the Dedicated JSON Processing Worker Pool.
fetcherLoop :: EngineConfig -> TBQueue [ByteString] -> EngineM ctx ()
fetcherLoop cfg rawJsonQueue = go
  where
    go = do
      rawBatch <- claimRawRedisBatch (cfgQueues cfg) (cfgFetchBatchSize cfg)
      if null rawBatch
        then liftIO $ threadDelay (cfgPollIntervalMicros cfg)
        else liftIO $ atomically (writeTBQueue rawJsonQueue rawBatch)
      go

startWorkerEngine :: EngineParams ctx -> IO (WorkerEngineHandle ctx)
startWorkerEngine EngineParams {..} = do
  tm           <- startTimeoutManager (cfgLeaseTimeoutSecs epConfig) (cfgGracePeriodSecs epConfig) (cfgSweepIntervalMicros epConfig)
  redisSync    <- startRedisSyncEngine epRedisStore
  mpq          <- newMultiPriorityQueue (cfgQueueCapacity epConfig)
  rawJsonQueue <- newTBQueueIO 1_024
  jCounter     <- newIORef 0
  let env = WorkerEnv
        { envLogger     = epLogger
        , envMetrics    = epMetrics
        , envDbPools    = epDbPools
        , envTimeoutMgr = tm
        , envRedisSync  = redisSync
        , envQueue      = mpq
        , envJobCounter = jCounter
        , envAppContext = epAppContext
        }
      eenv         = EngineEnv env epDispatcher
      nJsonWorkers = max 1 (cfgJsonWorkerCount epConfig)
  fetcherTid <- forkIO (runEngineM eenv (fetcherLoop epConfig rawJsonQueue))
  jsonTids   <- replicateM nJsonWorkers (forkIO (runEngineM eenv (jsonProcessorWorkerLoop rawJsonQueue)))
  workerTids <- replicateM (cfgWorkerCount epConfig) (forkIO (runEngineM eenv workerLoop))
  pure WorkerEngineHandle
    { wehEnv           = env
    , wehDispatcher    = epDispatcher
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

withWorkerEngine :: EngineParams ctx -> EngineM ctx a -> IO a
withWorkerEngine params action =
  bracket
    (startWorkerEngine params)
    stopWorkerEngine
    (\hdl -> runEngineM (EngineEnv (wehEnv hdl) (wehDispatcher hdl)) action)
