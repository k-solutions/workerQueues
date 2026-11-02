{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}

module Main (main) where

import Control.Concurrent (getNumCapabilities, threadDelay)
import Control.Monad (forM_, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON, object, (.=))
import Data.Aeson qualified as Aeson
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as B8
import Data.ByteString.Lazy qualified as BSL
import Data.Int (Int64)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Text (Text)
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (UTCTime (..), secondsToDiffTime)
import Data.Word (Word64)
import Database.SQLite.Simple qualified as SQLite
import GHC.Clock (getMonotonicTimeNSec)
import GHC.Generics (Generic)
import Text.Printf (printf)
import WorkerQueue
import WorkerQueue.Cron qualified as Cron
import WorkerQueue.Frame qualified as Frame
import WorkerQueue.Metrics

--------------------------------------------------------------------------------
-- Downstream Benchmark Application Context & Worker Payloads (`WorkerM BenchCtx`)
--------------------------------------------------------------------------------

data BenchCtx = BenchCtx
  { bctxSettledCounter   :: !(IORef Int)
  , bctxReconciledBatches:: !(IORef Int)
  , bctxCleanedSessions  :: !(IORef Int)
  }

-- 1. InvoiceSettlementWorker (Rails critical/default/low queue job)
data InvoiceSettlementArgs = InvoiceSettlementArgs
  { invoice_id   :: !Int64
  , amount_cents :: !Int64
  , currency     :: !Text
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

instance JobPayload InvoiceSettlementArgs where
  jobClassTag _ = "InvoiceSettlementWorker"

handleInvoiceSettlement :: InvoiceSettlementArgs -> WorkerM BenchCtx ()
handleInvoiceSettlement args = do
  bctx <- getAppContext
  -- Execute parameterized SQLite WAL query from pooled connection
  withSqlite $ \conn ->
    SQLite.execute
      conn
      "INSERT INTO settled_invoices (invoice_id, amount_cents, currency) VALUES (?, ?, ?)"
      (invoice_id args, amount_cents args, currency args)
  tickleLease
  liftIO $ atomicModifyIORef' (bctxSettledCounter bctx) (\n -> (n + 1, ()))

-- 2. LedgerReconciliationWorker (YAML Cron job with cooperative checkLease + tickleLease)
data LedgerReconciliationArgs = LedgerReconciliationArgs
  { batch_size  :: !Int
  , strict_mode :: !Bool
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

instance JobPayload LedgerReconciliationArgs where
  jobClassTag _ = "LedgerReconciliationWorker"

handleLedgerReconciliation :: LedgerReconciliationArgs -> WorkerM BenchCtx ()
handleLedgerReconciliation _args = do
  bctx <- getAppContext
  st   <- checkLease
  case st of
    Active _ -> do
      withSqlite $ \conn -> do
        [_] <- SQLite.query_ conn "SELECT COUNT(*) FROM settled_invoices" :: IO [SQLite.Only Int]
        pure ()
      tickleLease
      liftIO $ atomicModifyIORef' (bctxReconciledBatches bctx) (\n -> (n + 1, ()))
    _ -> pure ()

-- 3. StaleSessionCleanupWorker (YAML Cron job on `low` queue)
newtype StaleSessionCleanupArgs = StaleSessionCleanupArgs
  { max_age_minutes :: Int
  } deriving stock (Show, Eq, Generic)
    deriving anyclass (FromJSON, ToJSON)

instance JobPayload StaleSessionCleanupArgs where
  jobClassTag _ = "StaleSessionCleanupWorker"

handleStaleSessionCleanup :: StaleSessionCleanupArgs -> WorkerM BenchCtx ()
handleStaleSessionCleanup _args = do
  bctx <- getAppContext
  withSqlite $ \conn ->
    SQLite.execute_ conn "DELETE FROM settled_invoices WHERE invoice_id < 0"
  tickleLease
  liftIO $ atomicModifyIORef' (bctxCleanedSessions bctx) (\n -> (n + 1, ()))

-- 4. FaultInjectableWorker (Simulates hung DB / infinite loop when `should_stall = True`)
data FaultInjectableArgs = FaultInjectableArgs
  { task_id      :: !Int
  , should_stall :: !Bool
  } deriving (Show, Eq, Generic, FromJSON, ToJSON)

instance JobPayload FaultInjectableArgs where
  jobClassTag _ = "FaultInjectableWorker"

handleFaultInjectable :: FaultInjectableArgs -> WorkerM BenchCtx ()
handleFaultInjectable args =
  if should_stall args
    then liftIO $ foreverStall
    else do
      bctx <- getAppContext
      tickleLease
      liftIO $ atomicModifyIORef' (bctxSettledCounter bctx) (\n -> (n + 1, ()))
  where
    foreverStall = do
      threadDelay 10_000_000
      foreverStall

buildBenchDispatcher :: Dispatcher BenchCtx
buildBenchDispatcher = newDispatcher
  [ registerWorker handleInvoiceSettlement
  , registerWorker handleLedgerReconciliation
  , registerWorker handleStaleSessionCleanup
  , registerWorker handleFaultInjectable
  ]

--------------------------------------------------------------------------------
-- Benchmark Harness & Scenarios
--------------------------------------------------------------------------------

initBenchSqlitePool :: FilePath -> IO DbPools
initBenchSqlitePool dbPath = do
  pool <- newSqliteWalPool dbPath 60.0 16
  let pools = DbPools { pgPool = Nothing, sqlitePool = Just pool }
  withSqliteConn pools $ \conn -> do
    SQLite.execute_ conn "DROP TABLE IF EXISTS settled_invoices;"
    SQLite.execute_ conn
      "CREATE TABLE settled_invoices (\
      \  invoice_id INTEGER NOT NULL,\
      \  amount_cents INTEGER NOT NULL,\
      \  currency TEXT NOT NULL\
      \);"
  pure pools

formatJid :: Int -> ByteString
formatJid n =
  let s = B8.pack (printf "%024x" n)
   in B8.take 24 s

main :: IO ()
main = do
  caps <- getNumCapabilities
  putStrLn "================================================================================"
  putStrLn "  workerQueues — End-to-End Throughput, Latency & Fault-Recovery Benchmark"
  printf   "  GHC Runtime Capabilities (+RTS -N): %d\n" caps
  putStrLn "================================================================================\n"

  runScenarioA
  runScenarioB
  runScenarioC
  runScenarioD
  runScenarioE

  putStrLn "\nAll End-to-End Benchmark Scenarios completed and verified successfully."

--------------------------------------------------------------------------------
-- Scenario A: Peak End-to-End Pipeline Throughput + Capability-Aware Filtering
--             & Priority Tail-Latency Histograms (`p50`, `p95`, `p99`, `p99.9`)
--------------------------------------------------------------------------------

runScenarioA :: IO ()
runScenarioA = do
  putStrLn "[Scenario A] E2E Pipeline: Mixed Rails Sidekiq JSON + Capability Filter + SQLite WAL Pool"
  let totalHaskellJobs = 10_000 :: Int
      totalRubyJobs    = 1_500  :: Int

  store   <- newRedisStore "bench-node-1"
  pools   <- initBenchSqlitePool "/tmp/workerQueues_bench_a.sqlite3"
  metrics <- newMetrics
  settled <- newIORef 0
  reconc  <- newIORef 0
  cleaned <- newIORef 0
  let bctx = BenchCtx settled reconc cleaned
      disp = buildBenchDispatcher

  startPushNs <- getMonotonicTimeNSec

  -- 1. Enqueue 1,500 Ruby-only Sidekiq jobs across shared queues (must NOT be consumed by Haskell)
  forM_ [1 .. totalRubyJobs] $ \i -> do
    let qName = case i `rem` 3 of
                  0 -> "critical"
                  1 -> "default"
                  _ -> "low"
        rubyJson = BSL.toStrict $ Aeson.encode $ object
          [ "class" .= ("ActionMailer::MailDeliveryJob" :: Text)
          , "jid"   .= (printf "ruby%020x" i :: String)
          , "queue" .= (B8.unpack qName)
          , "args"  .= (["UserMailer", "welcome_email", "deliver_now"] :: [Text])
          ]
    pushSidekiqQueueJob store qName rubyJson

  -- 2. Enqueue 10,000 Rails Sidekiq & ActiveJob-wrapped InvoiceSettlementWorker jobs
  forM_ [1 .. totalHaskellJobs] $ \i -> do
    nowNs <- fromIntegral <$> getMonotonicTimeNSec
    let prio = case i `rem` 5 of
                 0 -> Critical -- 20% Critical
                 4 -> Low      -- 20% Low
                 _ -> Default  -- 60% Default
        qName = Frame.toQueueName prio
        args  = InvoiceSettlementArgs
                  { invoice_id   = fromIntegral i
                  , amount_cents = 125_00
                  , currency     = "EUR"
                  }
        rawJson =
          if i `rem` 2 == 0
            then encodeSidekiqJobJson prio (formatJid i) 25 nowNs args
            else
              -- Half enqueued in Rails ActiveJob JobWrapper format ("wrapped": "InvoiceSettlementWorker")
              BSL.toStrict $ Aeson.encode $ object
                [ "class"      .= ("ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper" :: Text)
                , "wrapped"    .= ("InvoiceSettlementWorker" :: Text)
                , "jid"        .= B8.unpack (formatJid i)
                , "queue"      .= B8.unpack qName
                , "args"       .= [object ["arguments" .= [args]]]
                , "retry"      .= (25 :: Int)
                , "created_at" .= nowNs
                ]
    pushSidekiqQueueJob store qName rawJson

  let cfg = defaultEngineConfig
        { cfgWorkerCount        = 32
        , cfgFetchBatchSize     = 256
        , cfgPollIntervalMicros = 500
        }
      params = EngineParams
        { epConfig     = cfg
        , epRedisStore = store
        , epDbPools    = pools
        , epLogger     = newSilentLogger
        , epMetrics    = metrics
        , epAppContext = bctx
        , epDispatcher = disp
        }

  startExecNs <- getMonotonicTimeNSec
  withWorkerEngine params $ do
    liftIO (waitUntilCounter settled totalHaskellJobs 10_000_000)
    flushEngineRedisSync
  endExecNs <- getMonotonicTimeNSec

  -- Verify Ruby-only jobs were completely untouched in Redis
  remCrit <- getQueueItems store "critical"
  remDef  <- getQueueItems store "default"
  remLow  <- getQueueItems store "low"
  let remainingRuby = length remCrit + length remDef + length remLow

  allSamples  <- readIORef (mAllLatenciesNs metrics)
  critSamples <- readIORef (mCritLatenciesNs metrics)
  cacheStats  <- getDispatcherCacheStats disp
  let allLat  = computeLatencySummary allSamples
      critLat = computeLatencySummary critSamples
      execSec = fromIntegral (endExecNs - startExecNs) / 1e9 :: Double
      totSec  = fromIntegral (endExecNs - startPushNs) / 1e9 :: Double
      tpsExec = fromIntegral totalHaskellJobs / execSec
      totCacheLookups = jcsExactHits cacheStats + jcsTemplateHits cacheStats + jcsMisses cacheStats
      cacheHitPct =
        if totCacheLookups == 0
          then 0.0
          else fromIntegral (jcsExactHits cacheStats + jcsTemplateHits cacheStats) * 100.0 / fromIntegral totCacheLookups :: Double

  printf "  - Haskell Jobs Executed (SQLite WAL): %d / %d\n" totalHaskellJobs totalHaskellJobs
  printf "  - Ruby-Only Jobs Untouched in Redis:  %d / %d (0 stolen!)\n" remainingRuby totalRubyJobs
  printf "  - JSON Param Cache (Exact/Tmpl/Miss): %d / %d / %d (%.2f%% cache hit rate!)\n"
    (jcsExactHits cacheStats) (jcsTemplateHits cacheStats) (jcsMisses cacheStats) cacheHitPct
  printf "  - Worker Execution Throughput:        %.0f jobs/sec (%.3f s)\n" tpsExec execSec
  printf "  - Full Producer+Worker Elapsed Time:  %.3f s\n" totSec
  printf "  - Critical Queue Latency (ms):        p50=%.2f  p95=%.2f  p99=%.2f  p99.9=%.2f\n"
    (latP50Ms critLat) (latP95Ms critLat) (latP99Ms critLat) (latP999Ms critLat)
  printf "  - Overall Queue Latency  (ms):        p50=%.2f  p95=%.2f  p99=%.2f  p99.9=%.2f\n\n"
    (latP50Ms allLat) (latP95Ms allLat) (latP99Ms allLat) (latP999Ms allLat)

  closeDbPools pools
  when (remainingRuby /= totalRubyJobs) $
    error ("Expected " <> show totalRubyJobs <> " untouched Ruby jobs, got " <> show remainingRuby)

--------------------------------------------------------------------------------
-- Scenario B: Built-In YAML Cron Scheduler (`config/schedule.yml`) &
--             Multi-Node Redis `SET NX EX` Deduplication
--------------------------------------------------------------------------------

sampleScheduleYaml :: ByteString
sampleScheduleYaml =
  "jobs:\n\
  \  daily_ledger_reconciliation:\n\
  \    cron: \"0 2 * * *\"\n\
  \    class: \"LedgerReconciliationWorker\"\n\
  \    queue: \"critical\"\n\
  \    retry: 10\n\
  \    args:\n\
  \      batch_size: 5000\n\
  \      strict_mode: true\n\
  \    description: \"Reconcile ledger entries every night at 02:00 UTC\"\n\
  \  stale_session_cleanup:\n\
  \    cron: \"*/15 * * * *\"\n\
  \    class: \"StaleSessionCleanupWorker\"\n\
  \    queue: \"low\"\n\
  \    retry: 3\n\
  \    args:\n\
  \      max_age_minutes: 120\n\
  \  unregistered_ruby_cron:\n\
  \    cron: \"* * * * *\"\n\
  \    class: \"RubyDigestMailerWorker\"\n\
  \    queue: \"default\"\n"

runScenarioB :: IO ()
runScenarioB = do
  putStrLn "[Scenario B] Built-In YAML Cron Scheduler (`WorkerQueue.Cron`) Throughput & 4-Node NX Lock"
  schedFile <- case Cron.parseScheduleYaml sampleScheduleYaml of
    Right s  -> pure s
    Left err -> error ("Failed to parse sampleScheduleYaml: " <> err)

  let disp = buildBenchDispatcher
      (execSpecs, skippedNames) = Cron.validateCapabilities (dispClassSet disp) schedFile

  -- 1. Micro-benchmark `Cron.matchesTime` across 100,000 timestamps
  let matchIterations = 100_000 :: Int
      baseDay         = fromGregorian 2026 11 1
  startMatchNs <- getMonotonicTimeNSec
  let !matchedCount = foldl'
        (\ !acc i ->
          let utc = UTCTime baseDay (secondsToDiffTime (fromIntegral ((i * 60) `rem` 86_400)))
              m   = length (filter (\s -> Cron.matchesTime (Cron.jsExpr s) utc) execSpecs)
           in acc + m)
        0
        [1 .. matchIterations]
  endMatchNs <- getMonotonicTimeNSec
  let matchSec     = fromIntegral (endMatchNs - startMatchNs) / 1e9 :: Double
      matchEvalTps = fromIntegral (matchIterations * length execSpecs) / matchSec

  -- 2. End-to-End Multi-Node `Cron.executeTick` (`ReaderT Env IO`) Benchmark across 500 15-minute ticks
  let totalCronTicks = 500 :: Int
      nodesPerTick   = 4   :: Int
  store   <- newRedisStore "cluster-shared"
  pools   <- initBenchSqlitePool "/tmp/workerQueues_bench_b.sqlite3"
  metrics <- newMetrics
  settled <- newIORef 0
  reconc  <- newIORef 0
  cleaned <- newIORef 0
  let bctx   = BenchCtx settled reconc cleaned
      params = EngineParams
        { epConfig     = defaultEngineConfig
        , epRedisStore = store
        , epDbPools    = pools
        , epLogger     = newSilentLogger
        , epMetrics    = metrics
        , epAppContext = bctx
        , epDispatcher = disp
        }

  startCronNs <- getMonotonicTimeNSec
  withWorkerEngine params $ do
    env <- askWorkerEnv
    let cronEnv = Cron.fromWorkerEnv execSpecs env
    liftIO $ do
      -- First verify 4-node deduplication on a single 02:00 UTC tick (matches both specs -> 2 jobs)
      let tick0200 = UTCTime baseDay (secondsToDiffTime (2 * 3600))
      firstTickCounts <- mapM
        (\_nodeIdx -> Cron.runCronM cronEnv (Cron.executeTick tick0200))
        ([1 .. nodesPerTick] :: [Int])

      -- Now evaluate `totalCronTicks - 1` additional distinct 15-minute intervals across all 4 nodes
      restCounts <- mapM
        (\tickIdx -> do
          let dayOffset = (tickIdx * 15) `div` 1440
              minOfDay  = (tickIdx * 15) `rem` 1440
              tickUtc   = UTCTime
                            (fromGregorian 2026 11 (1 + fromIntegral dayOffset))
                            (secondsToDiffTime (fromIntegral (minOfDay * 60)))
          sum <$> mapM (\_ -> Cron.runCronM cronEnv (Cron.executeTick tickUtc)) ([1 .. nodesPerTick] :: [Int])
        )
        [1 .. totalCronTicks - 1]

      let firstTickEnqueued        = sum firstTickCounts
          totalEnqueuedAcrossNodes = firstTickEnqueued + sum restCounts

      -- Wait until all enqueued cron jobs have been executed by `WorkerM`
      let waitCronDone !waited = do
            r <- readIORef reconc
            c <- readIORef cleaned
            if r + c >= totalEnqueuedAcrossNodes
              then pure (r, c)
              else if waited >= 10_000_000
                then error ("Timeout waiting for cron jobs: " <> show (r + c, totalEnqueuedAcrossNodes))
                else threadDelay 2_000 >> waitCronDone (waited + 2_000)

      (rCount, cCount) <- waitCronDone (0 :: Int)
      flushRedisSyncNow (envRedisSync env)
      endCronNs <- getMonotonicTimeNSec

      cronSamples <- readIORef (mAllLatenciesNs metrics)
      let cronLat   = computeLatencySummary cronSamples
          cronSec   = fromIntegral (endCronNs - startCronNs) / 1e9 :: Double
          totalEvals = totalCronTicks * nodesPerTick
          tickTps   = fromIntegral totalEvals / cronSec
          jobTps    = fromIntegral totalEnqueuedAcrossNodes / cronSec

      printf "  - Executable Cron Specs Validated:    %d (Skipped Ruby Cron: %s)\n"
        (length execSpecs) (show skippedNames)
      printf "  - Cron.matchesTime Evaluation Speed:  %.0f expr-checks/sec (%d matches in %.3f ms)\n"
        matchEvalTps matchedCount (matchSec * 1e3)
      printf "  - 4-Node First Tick Lock Result:      %s (Enqueued = %d, Deduplicated!)\n"
        (show firstTickCounts) firstTickEnqueued
      printf "  - Multi-Node Cron.executeTick Speed:  %.0f node-ticks/sec (%.0f jobs/sec, %.3f s)\n"
        tickTps jobTps cronSec
      printf "  - Cron Workers Executed in WorkerM:   Total=%d (LedgerReconciliation=%d, StaleSessionCleanup=%d)\n"
        (rCount + cCount) rCount cCount
      printf "  - Cron Job Execution Latency (ms):    p50=%.2f  p95=%.2f  p99=%.2f  p99.9=%.2f\n\n"
        (latP50Ms cronLat) (latP95Ms cronLat) (latP99Ms cronLat) (latP999Ms cronLat)

      when (firstTickEnqueued /= 2) $
        error ("Expected exactly 2 deduplicated cron jobs on 02:00 tick, got " <> show firstTickEnqueued)

  closeDbPools pools

--------------------------------------------------------------------------------
-- Scenario C: Two-Stage Warp-Style Timeout Manager (`SoftExpired` -> `HardExpired`
--             via `throwTo JobLeaseExpired` + Sidekiq Retry/DLQ Routing)
--------------------------------------------------------------------------------

runScenarioC :: IO ()
runScenarioC = do
  putStrLn "[Scenario C] Two-Stage TimeoutManager: Stalled Worker Hard-Kill (`throwTo`) & DLQ Recovery"
  store   <- newRedisStore "bench-node-c"
  pools   <- initBenchSqlitePool "/tmp/workerQueues_bench_c.sqlite3"
  metrics <- newMetrics
  settled <- newIORef 0
  reconc  <- newIORef 0
  cleaned <- newIORef 0
  let bctx = BenchCtx settled reconc cleaned
      disp = buildBenchDispatcher
      healthyCount = 50 :: Int
      stalledCount = 4  :: Int

  -- Enqueue 4 stalled jobs (maxRetries = 0 so they route straight to Sidekiq DLQ on hard-kill)
  -- mixed with 50 healthy jobs
  forM_ [1 .. stalledCount] $ \i -> do
    nowNs <- fromIntegral <$> getMonotonicTimeNSec
    let args = FaultInjectableArgs { task_id = i, should_stall = True }
        raw  = encodeSidekiqJobJson Critical (formatJid (9000 + i)) 0 nowNs args
    pushSidekiqQueueJob store "critical" raw

  forM_ [1 .. healthyCount] $ \i -> do
    nowNs <- fromIntegral <$> getMonotonicTimeNSec
    let args = FaultInjectableArgs { task_id = i, should_stall = False }
        raw  = encodeSidekiqJobJson Default (formatJid (1000 + i)) 3 nowNs args
    pushSidekiqQueueJob store "default" raw

  -- Short lease timeout (40ms) + 20ms Stage-1 grace period + 15ms sweep interval
  let fastTimeoutCfg = defaultEngineConfig
        { cfgWorkerCount         = 8
        , cfgLeaseTimeoutSecs    = 0.040
        , cfgGracePeriodSecs     = 0.020
        , cfgSweepIntervalMicros = 15_000
        , cfgPollIntervalMicros  = 1_000
        }
      params = EngineParams
        { epConfig     = fastTimeoutCfg
        , epRedisStore = store
        , epDbPools    = pools
        , epLogger     = newSilentLogger
        , epMetrics    = metrics
        , epAppContext = bctx
        , epDispatcher = disp
        }

  startNs <- getMonotonicTimeNSec
  withWorkerEngine params $ do
    liftIO $ do
      waitUntilCounter settled healthyCount 5_000_000
      waitUntilCounterWord64 (mHardKilled metrics) (fromIntegral stalledCount) 5_000_000
    flushEngineRedisSync
  endNs <- getMonotonicTimeNSec

  snap    <- snapshotMetrics metrics
  dlqJobs <- getDeadZSet store
  let elapsedMs = fromIntegral (endNs - startNs) / 1e6 :: Double

  printf "  - Healthy Jobs Completed:             %d / %d\n" (snapCompleted snap) healthyCount
  printf "  - Stalled Workers Hard-Killed:        %d / %d (via Stage-2 throwTo JobLeaseExpired)\n"
    (snapHardKilled snap) stalledCount
  printf "  - Stalled Jobs Routed to Sidekiq DLQ: %d / %d\n" (length dlqJobs) stalledCount
  printf "  - Total Fault-Recovery Elapsed Time:  %.1f ms\n\n" elapsedMs

  closeDbPools pools
  when (length dlqJobs /= stalledCount) $
    error ("Expected " <> show stalledCount <> " jobs in DLQ, got " <> show (length dlqJobs))

--------------------------------------------------------------------------------
-- Scenario D: Explicit `WorkerQueue.Engine` (`EngineM ctx`) Benchmark
--             1. `ingestRawSidekiqBatch` (Zero-Copy Scan + Frame + Cache Prewarm)
--             2. `processFrameWithTimeout` (Direct `EngineM` Dispatch + Lease)
--             3. Multi-Threaded `withWorkerEngine` Pure Engine Throughput (No Disk I/O)
--------------------------------------------------------------------------------

runScenarioD :: IO ()
runScenarioD = do
  putStrLn "[Scenario D] Explicit `WorkerQueue.Engine` (`EngineM ctx`) Core Throughput & Dispatch"
  let directCount = 10_000 :: Int
      engineCount = 20_000 :: Int

  -- Part 1 & 2: Direct `ingestRawSidekiqBatch` and `processFrameWithTimeout` in `EngineM`
  store1   <- newRedisStore "bench-engine-direct"
  metrics1 <- newMetrics
  settled1 <- newIORef 0
  reconc1  <- newIORef 0
  cleaned1 <- newIORef 0
  let bctx1 = BenchCtx settled1 reconc1 cleaned1
      disp1 = buildBenchDispatcher

  tm1        <- startTimeoutManager 15.0 2.0 100_000
  redisSync1 <- startRedisSyncEngine store1
  mpq1       <- newMultiPriorityQueue 65_536
  jCnt1      <- newIORef 0
  let env1 = WorkerEnv
        { envLogger     = newSilentLogger
        , envMetrics    = metrics1
        , envDbPools    = emptyDbPools
        , envTimeoutMgr = tm1
        , envRedisSync  = redisSync1
        , envQueue      = mpq1
        , envJobCounter = jCnt1
        , envAppContext = bctx1
        }
      eenv1 = EngineEnv env1 disp1

  nowBaseNs <- fromIntegral <$> getMonotonicTimeNSec
  let rawBatch =
        [ encodeSidekiqJobJson
            Default
            (formatJid i)
            25
            nowBaseNs
            (FaultInjectableArgs { task_id = i, should_stall = False })
        | i <- [1 .. directCount]
        ]

  startIngestNs <- getMonotonicTimeNSec
  ingested <- runEngineM eenv1 (ingestRawSidekiqBatch rawBatch)
  endIngestNs <- getMonotonicTimeNSec

  startProcNs <- getMonotonicTimeNSec
  runEngineM eenv1 $ do
    forM_ [1 .. ingested] $ \_ -> do
      frame <- liftIO (dequeueWeighted mpq1)
      processFrameWithTimeout frame
    flushEngineRedisSync
  endProcNs <- getMonotonicTimeNSec

  stopTimeoutManager tm1
  stopRedisSyncEngine redisSync1

  let ingestSec = fromIntegral (endIngestNs - startIngestNs) / 1e9 :: Double
      ingestTps = fromIntegral ingested / ingestSec
      procSec   = fromIntegral (endProcNs - startProcNs) / 1e9 :: Double
      procTps   = fromIntegral ingested / procSec

  -- Part 3: Full Multi-Threaded `withWorkerEngine` Pure In-Memory Throughput (20,000 jobs)
  store2   <- newRedisStore "bench-engine-mt"
  metrics2 <- newMetrics
  settled2 <- newIORef 0
  reconc2  <- newIORef 0
  cleaned2 <- newIORef 0
  let bctx2 = BenchCtx settled2 reconc2 cleaned2
      disp2 = buildBenchDispatcher

  forM_ [1 .. engineCount] $ \i -> do
    nowNs <- fromIntegral <$> getMonotonicTimeNSec
    let prio  = if i `rem` 4 == 0 then Critical else Default
        qName = Frame.toQueueName prio
        raw   = encodeSidekiqJobJson
                  prio
                  (formatJid i)
                  25
                  nowNs
                  (FaultInjectableArgs { task_id = i, should_stall = False })
    pushSidekiqQueueJob store2 qName raw

  let engineCfg = defaultEngineConfig
        { cfgWorkerCount        = 32
        , cfgJsonWorkerCount    = 4
        , cfgFetchBatchSize     = 512
        , cfgPollIntervalMicros = 250
        }
      params2 = EngineParams
        { epConfig     = engineCfg
        , epRedisStore = store2
        , epDbPools    = emptyDbPools
        , epLogger     = newSilentLogger
        , epMetrics    = metrics2
        , epAppContext = bctx2
        , epDispatcher = disp2
        }

  startMtNs <- getMonotonicTimeNSec
  withWorkerEngine params2 $ do
    liftIO (waitUntilCounter settled2 engineCount 10_000_000)
    flushEngineRedisSync
  endMtNs <- getMonotonicTimeNSec

  mtSamples <- readIORef (mAllLatenciesNs metrics2)
  let mtLat = computeLatencySummary mtSamples
      mtSec = fromIntegral (endMtNs - startMtNs) / 1e9 :: Double
      mtTps = fromIntegral engineCount / mtSec

  printf "  - EngineM ingestRawSidekiqBatch:      %.0f jobs/sec (%d jobs in %.3f ms)\n"
    ingestTps ingested (ingestSec * 1e3)
  printf "  - EngineM processFrameWithTimeout:    %.0f jobs/sec (%d jobs in %.3f ms)\n"
    procTps ingested (procSec * 1e3)
  printf "  - Multi-Threaded withWorkerEngine:    %.0f jobs/sec (%d jobs in %.3f s)\n"
    mtTps engineCount mtSec
  printf "  - Pure Engine Queue Latency (ms):     p50=%.2f  p95=%.2f  p99=%.2f  p99.9=%.2f\n\n"
    (latP50Ms mtLat) (latP95Ms mtLat) (latP99Ms mtLat) (latP999Ms mtLat)

--------------------------------------------------------------------------------
-- Scenario E: Explicit `WorkerQueue.Metrics` Telemetry, Histograms & Logger Benchmark
--             1. Atomic Counter Updates (`incEnqueued`, `incCompleted`, etc.) & `snapshotMetrics`
--             2. Latency Sample Recording (`recordJobLatencyNs`) & `computeLatencySummary`
--             3. Structured Logger Level Filtering (`newStderrLogger` + `logInfoIO` / `logWarnIO`)
--------------------------------------------------------------------------------

runScenarioE :: IO ()
runScenarioE = do
  putStrLn "[Scenario E] Explicit `WorkerQueue.Metrics` Atomic Counters, Histograms & Logger"
  let counterIters = 100_000 :: Int
      totalOps     = counterIters * 5
      latencyCount = 50_000  :: Int
      loggerCalls  = 500_000 :: Int

  metrics <- newMetrics

  -- 1. Benchmark atomic counter updates + snapshots
  startCntNs <- getMonotonicTimeNSec
  forM_ [1 .. counterIters] $ \_ -> do
    incEnqueued metrics 1
    incCompleted metrics
    incRetried metrics
    incRubySkipped metrics 1
    incCronTriggered metrics
  snap <- snapshotMetrics metrics
  endCntNs <- getMonotonicTimeNSec

  -- 2. Benchmark `recordJobLatencyNs` + `computeLatencySummary`
  startLatNs <- getMonotonicTimeNSec
  forM_ [1 .. latencyCount] $ \i -> do
    let prio = if i `rem` 4 == 0 then Critical else Default
        durNs = fromIntegral ((i `rem` 2_000) * 1_000) :: Int64
    recordJobLatencyNs metrics prio durNs
  allSamples  <- readIORef (mAllLatenciesNs metrics)
  critSamples <- readIORef (mCritLatenciesNs metrics)
  let !allSummary  = computeLatencySummary allSamples
      !critSummary = computeLatencySummary critSamples
  endLatNs <- getMonotonicTimeNSec

  -- 3. Benchmark `newStderrLogger` level filtering (`when (lvl >= minLevel)`)
  errLogger <- newStderrLogger LevelError
  startLogNs <- getMonotonicTimeNSec
  forM_ [1 .. loggerCalls] $ \i ->
    if even i
      then logInfoIO errLogger "worker heartbeat ok"
      else logWarnIO errLogger "queue depth threshold check"
  endLogNs <- getMonotonicTimeNSec

  let cntSec = fromIntegral (endCntNs - startCntNs) / 1e9 :: Double
      cntOps = fromIntegral totalOps / cntSec
      latSec = fromIntegral (endLatNs - startLatNs) / 1e9 :: Double
      latOps = fromIntegral latencyCount / latSec
      logSec = fromIntegral (endLogNs - startLogNs) / 1e9 :: Double
      logOps = fromIntegral loggerCalls / logSec

  printf "  - Atomic Counter Updates (500k ops):  %.0f ops/sec (%.3f ms, completed=%d)\n"
    cntOps (cntSec * 1e3) (snapCompleted snap)
  printf "  - Latency Record + Histogram (50k):   %.0f samples/sec (%.3f ms, crit=%d, p99=%.2f ms)\n"
    latOps (latSec * 1e3) (latCount critSummary) (latP99Ms allSummary)
  printf "  - Filtered Logger (`newStderrLogger`): %.0f calls/sec (%d calls in %.3f ms)\n"
    logOps loggerCalls (logSec * 1e3)

--------------------------------------------------------------------------------
-- Polling Helpers for Benchmark Synchronization
--------------------------------------------------------------------------------

waitUntilCounter :: IORef Int -> Int -> Int -> IO ()
waitUntilCounter ref target maxWaitMicros = go 0
  where
    step = 2_000
    go !waited = do
      cur <- readIORef ref
      if cur >= target
        then pure ()
        else if waited >= maxWaitMicros
          then error ("Timeout waiting for counter: have " <> show cur <> ", target " <> show target)
          else do
            threadDelay step
            go (waited + step)

waitUntilCounterWord64 :: IORef Word64 -> Word64 -> Int -> IO ()
waitUntilCounterWord64 ref target maxWaitMicros = go 0
  where
    step = 2_000
    go !waited = do
      cur <- readIORef ref
      if cur >= target
        then pure ()
        else if waited >= maxWaitMicros
          then error ("Timeout waiting for Word64 counter: have " <> show cur <> ", target " <> show target)
          else do
            threadDelay step
            go (waited + step)
