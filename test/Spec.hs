{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}

module Main (main) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Exception (catch, throwIO)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON, object, (.=))
import Data.Aeson qualified as Aeson
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.HashSet qualified as HS
import Data.Int (Int64)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Text (Text)
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (UTCTime (..), secondsToDiffTime)
import Data.Word (Word8, Word16, Word64)
import Database.SQLite.Simple qualified as SQLite
import GHC.Generics (Generic)
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.HUnit (assertBool, assertEqual, testCase, (@?=))
import Test.Tasty.QuickCheck as QC
import WorkerQueue
import WorkerQueue.Cron qualified as Cron
import WorkerQueue.Frame qualified as Frame

--------------------------------------------------------------------------------
-- Sample Domain Payload Types for Testing
--------------------------------------------------------------------------------

data SampleInvoice = SampleInvoice
  { invId       :: !Int64
  , invCurrency :: !Text
  } deriving stock (Show, Eq, Generic)
    deriving anyclass (FromJSON, ToJSON)

instance JobPayload SampleInvoice where
  jobClassTag _ = "InvoiceSettlementWorker"

newtype SampleCleanup = SampleCleanup
  { cleanupDays :: Int
  } deriving stock (Show, Eq, Generic)
    deriving anyclass (FromJSON, ToJSON)

instance JobPayload SampleCleanup where
  jobClassTag _ = "StaleSessionCleanupWorker"

data TestCtx = TestCtx
  { tctxTag     :: !Text
  , tctxCounter :: !(IORef Int)
  }

--------------------------------------------------------------------------------
-- Test Suite Entrypoint
--------------------------------------------------------------------------------

main :: IO ()
main = defaultMain $ testGroup "workerQueues Library Tasty Specifications"
  [ frameSpecs
  , jsonScannerAndCacheSpecs
  , sidekiqSpecs
  , redisCapabilitySpecs
  , cronSpecs
  , timeoutManagerSpecs
  , workerMonadAndEngineSpecs
  ]

--------------------------------------------------------------------------------
-- 1. 64-Byte Zero-Copy Frame & `JobPayload` Codec Specs
--------------------------------------------------------------------------------

frameSpecs :: TestTree
frameSpecs = testGroup "WorkerQueue.Frame (64-Byte Cache-Line Header & Zero-Copy Slices)"
  [ testCase "encodes and decodes 64-byte header + zero-copy slices accurately" $ do
      let jid   = "b4a8c9d012e3f4567890abcd"
          qName = "critical"
          tag   = "InvoiceSettlementWorker"
          body  = "{\"invId\":98124,\"invCurrency\":\"EUR\"}"
          raw   = Frame.encode Frame.Params
            { fpPriority     = Critical
            , fpSource       = SourceRails
            , fpRetryCount   = 2
            , fpMaxRetries   = 25
            , fpJobId        = 777
            , fpSidekiqJid   = jid
            , fpCreatedAtNs  = 1_700_000_000_000
            , fpQueueSlice   = qName
            , fpTagSlice     = tag
            , fpPayloadSlice = body
            }

      BS.length raw @?= 64 + BS.length qName + BS.length tag + BS.length body

      case Frame.decodeHeader raw of
        Left err  -> fail ("Frame.decodeHeader failed: " <> err)
        Right hdr -> do
          hdrMagic hdr        @?= 0x5751
          hdrVersion hdr      @?= 1
          hdrPriority hdr     @?= Critical
          hdrSource hdr       @?= SourceRails
          hdrRetryCount hdr   @?= 2
          hdrMaxRetries hdr   @?= 25
          hdrJobId hdr        @?= 777
          hdrSidekiqJid hdr   @?= jid
          hdrCreatedAtNs hdr  @?= 1_700_000_000_000
          hdrQueueSlice hdr   @?= qName
          hdrTagSlice hdr     @?= tag
          hdrPayloadSlice hdr @?= body
          decodePayload (hdrPayloadSlice hdr) @?= Right (SampleInvoice 98124 "EUR")

  , testCase "bumpRetry increments retryCount at offset 4..5 without altering slices" $ do
      let jid  = "111122223333444455556666"
          raw0 = Frame.encodeJob Default SourceHaskell 10 42 jid 999_000 (SampleInvoice 55 "USD")
      case Frame.bumpRetry raw0 of
        Left err -> fail err
        Right (r1, maxR, raw1) -> do
          r1   @?= 1
          maxR @?= 10
          case Frame.decodeHeader raw1 of
            Left err  -> fail err
            Right hdr -> do
              hdrRetryCount hdr @?= 1
              hdrMaxRetries hdr @?= 10
              hdrJobId hdr      @?= 42
              hdrSidekiqJid hdr @?= jid
              decodePayload (hdrPayloadSlice hdr) @?= Right (SampleInvoice 55 "USD")

  , testCase "rejects frames shorter than 64 bytes or with invalid magic" $ do
      assertBool "short frame rejected" $
        case Frame.decodeHeader "short" of
          Left _  -> True
          Right _ -> False
      assertBool "bad magic rejected" $
        case Frame.decodeHeader (BS.replicate 64 0xFF) of
          Left _  -> True
          Right _ -> False

  , QC.testProperty "QuickCheck round-trip property for arbitrary 64-byte frames" $
      \(prioIdx :: Word8, srcIdx :: Word8, rCnt :: Word16, maxR :: Word16, jId :: Word64, ts :: Int64, qBytes :: [Word8], tBytes :: [Word8], pBytes :: [Word8]) ->
        let prio   = toEnum (fromIntegral prioIdx `rem` 3)
            src    = toEnum (fromIntegral srcIdx  `rem` 3)
            qSlice = BS.pack qBytes
            tSlice = BS.pack tBytes
            pSlice = BS.pack pBytes
            jid    = "abcdef0123456789abcdef01"
            frame  = Frame.encode Frame.Params
              { fpPriority     = prio
              , fpSource       = src
              , fpRetryCount   = rCnt
              , fpMaxRetries   = maxR
              , fpJobId        = jId
              , fpSidekiqJid   = jid
              , fpCreatedAtNs  = ts
              , fpQueueSlice   = qSlice
              , fpTagSlice     = tSlice
              , fpPayloadSlice = pSlice
              }
         in case Frame.decodeHeader frame of
              Left _    -> False
              Right hdr ->
                hdrPriority hdr     == prio
                  && hdrSource hdr       == src
                  && hdrRetryCount hdr   == rCnt
                  && hdrMaxRetries hdr   == maxR
                  && hdrJobId hdr        == jId
                  && hdrSidekiqJid hdr   == jid
                  && hdrCreatedAtNs hdr  == ts
                  && hdrQueueSlice hdr   == qSlice
                  && hdrTagSlice hdr     == tSlice
                  && hdrPayloadSlice hdr == pSlice
  ]

--------------------------------------------------------------------------------
-- 2. Zero-Copy JSON Scanner & Two-Level Parameter Cache Specs (`WorkerQueue.Json`)
--------------------------------------------------------------------------------

jsonScannerAndCacheSpecs :: TestTree
jsonScannerAndCacheSpecs = testGroup "WorkerQueue.Json (Zero-Copy Scanner & Two-Level Parameter Cache)"
  [ testCase "scans Sidekiq and ActiveJob JSON fields zero-copy without Aeson DOM allocation" $ do
      let raw = "{\"class\":\"ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper\",\"wrapped\":\"InvoiceSettlementWorker\",\"jid\":\"1234567890abcdef12345678\",\"queue\":\"critical\",\"args\":[{\"arguments\":[{\"invId\":500,\"invCurrency\":\"EUR\"}]}],\"retry\":10,\"retry_count\":2}"
      scanSidekiqClassFast raw @?= Just "InvoiceSettlementWorker"
      scanSidekiqJidFast raw   @?= Just "1234567890abcdef12345678"
      scanSidekiqArgsSlice raw @?= Just "{\"invId\":500,\"invCurrency\":\"EUR\"}"

  , testCase "hits Level-1 Exact Cache on identical JSON and Level-2 Template Cache when most params are unchanged" $ do
      cache <- newJsonParamCache 256
      -- 1st call: Cache miss (populates Level-1 and Level-2 template cache)
      r1 <- decodeWithParamCache cache "{\"invId\":101,\"invCurrency\":\"EUR\"}"
      r1 @?= Right (SampleInvoice 101 "EUR")

      -- 2nd call: Identical JSON -> hits Level-1 Exact Cache!
      r2 <- decodeWithParamCache cache "{\"invId\":101,\"invCurrency\":\"EUR\"}"
      r2 @?= Right (SampleInvoice 101 "EUR")

      -- 3rd call: Varying `invId` (102), unchanged `invCurrency: "EUR"` -> hits Level-2 Template Cache!
      r3 <- decodeWithParamCache cache "{\"invId\":102,\"invCurrency\":\"EUR\"}"
      r3 @?= Right (SampleInvoice 102 "EUR")

      stats <- getJsonCacheStats cache
      jcsMisses stats       @?= 1
      jcsExactHits stats    @?= 1
      jcsTemplateHits stats @?= 1
  ]

--------------------------------------------------------------------------------
-- 3. Sidekiq & Rails `ActiveJob` JSON Envelope Interoperability Specs
--------------------------------------------------------------------------------

sidekiqSpecs :: TestTree
sidekiqSpecs = testGroup "WorkerQueue.Sidekiq (Rails Sidekiq::Client & ActiveJob Wire Format)"
  [ testCase "parses direct Sidekiq::Client JSON and transcodes to 64-byte frame" $ do
      let rawJson = BSL.toStrict $ Aeson.encode $ object
            [ "class"       .= ("InvoiceSettlementWorker" :: Text)
            , "jid"         .= ("a1b2c3d4e5f60123456789ab" :: Text)
            , "queue"       .= ("critical" :: Text)
            , "args"        .= [SampleInvoice 701 "GBP"]
            , "retry"       .= (15 :: Int)
            , "retry_count" .= (3 :: Int)
            ]
      extractSidekiqClass rawJson @?= Just "InvoiceSettlementWorker"
      case sidekiqEnvelopeToFrame 101 500_000 rawJson of
        Left err    -> fail err
        Right frame ->
          case Frame.decodeHeader frame of
            Left err  -> fail err
            Right hdr -> do
              hdrPriority hdr   @?= Critical
              hdrTagSlice hdr   @?= "InvoiceSettlementWorker"
              hdrSidekiqJid hdr @?= "a1b2c3d4e5f60123456789ab"
              hdrMaxRetries hdr @?= 15
              hdrRetryCount hdr @?= 3
              decodePayload (hdrPayloadSlice hdr) @?= Right (SampleInvoice 701 "GBP")

  , testCase "unwraps Rails ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper (`wrapped` field)" $ do
      let activeJobJson = BSL.toStrict $ Aeson.encode $ object
            [ "class"   .= ("ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper" :: Text)
            , "wrapped" .= ("InvoiceSettlementWorker" :: Text)
            , "jid"     .= ("99887766554433221100aabb" :: Text)
            , "queue"   .= ("default" :: Text)
            , "args"    .= [object ["job_class" .= ("InvoiceSettlementWorker" :: Text), "arguments" .= [SampleInvoice 888 "JPY"]]]
            ]
      extractSidekiqClass activeJobJson @?= Just "InvoiceSettlementWorker"
      case sidekiqEnvelopeToFrame 102 600_000 activeJobJson of
        Left err    -> fail err
        Right frame ->
          case Frame.decodeHeader frame of
            Left err  -> fail err
            Right hdr -> do
              hdrTagSlice hdr @?= "InvoiceSettlementWorker"
              decodePayload (hdrPayloadSlice hdr) @?= Right (SampleInvoice 888 "JPY")

  , testCase "computeSidekiqBackoffSecs satisfies Sidekiq exponential backoff bounds" $ do
      let retryCnt = 3 :: Word16
      delay <- computeSidekiqBackoffSecs retryCnt
      -- Base = 3^4 + 15 = 96; max jitter = 30 * (3 + 1) = 120 => [96.0 .. 216.0]
      assertBool ("backoff within [96, 216], got " <> show delay) (delay >= 96.0 && delay <= 216.0)
  ]

--------------------------------------------------------------------------------
-- 3. Capability-Aware Redis Filtering & Async Sync Specs
--------------------------------------------------------------------------------

redisCapabilitySpecs :: TestTree
redisCapabilitySpecs = testGroup "WorkerQueue.Redis (Capability-Aware Filtering & Async Sync)"
  [ testCase "claims only registered Haskell classes and leaves Ruby Sidekiq jobs untouched in FIFO order" $ do
      store   <- newRedisStore "test-node-1"
      metrics <- newMetrics
      let allowed = HS.fromList ["InvoiceSettlementWorker"]
          rubyJob1 = BSL.toStrict $ Aeson.encode $ object
            [ "class" .= ("RubyActionMailerJob" :: Text)
            , "jid"   .= ("ruby11111111111111111111" :: Text)
            , "queue" .= ("default" :: Text)
            , "args"  .= ([1, 2, 3] :: [Int])
            ]
          hsJob1 = encodeSidekiqJobJson Default "hs1111111111111111111111" 25 1000 (SampleInvoice 10 "EUR")
          rubyJob2 = BSL.toStrict $ Aeson.encode $ object
            [ "class" .= ("RubyActiveStorageJob" :: Text)
            , "jid"   .= ("ruby22222222222222222222" :: Text)
            , "queue" .= ("default" :: Text)
            , "args"  .= (["blob"] :: [Text])
            ]
          hsJob2 = encodeSidekiqJobJson Default "hs2222222222222222222222" 25 2000 (SampleInvoice 20 "USD")

      pushSidekiqQueueJob store "default" rubyJob1
      pushSidekiqQueueJob store "default" hsJob1
      pushSidekiqQueueJob store "default" rubyJob2
      pushSidekiqQueueJob store "default" hsJob2

      claimed <- claimExecutableBatch ClaimBatchParams
        { cbpStore          = store
        , cbpMetrics        = metrics
        , cbpAllowedClasses = allowed
        , cbpQueueName      = "default"
        , cbpMaxClaim       = 10
        }
      claimed @?= [hsJob1, hsJob2]

      -- Verify both Ruby jobs are still untouched in the queue in exact FIFO order!
      remaining <- getQueueItems store "default"
      remaining @?= [rubyJob1, rubyJob2]

      -- Verify working leases recorded for claimed Haskell jobs
      leases <- getWorkingLeases store
      length leases @?= 2

  , testCase "promotes only due and registered jobs from schedule/retry ZSETs" $ do
      store <- newRedisStore "test-node-zset"
      let allowed    = HS.fromList ["InvoiceSettlementWorker"]
          dueHsJob   = encodeSidekiqJobJson Critical "due_hs_00000000000000000" 25 100 (SampleInvoice 1 "EUR")
          futHsJob   = encodeSidekiqJobJson Critical "fut_hs_00000000000000000" 25 100 (SampleInvoice 2 "EUR")
          dueRubyJob = BSL.toStrict $ Aeson.encode $ object
            [ "class" .= ("RubyOnlyScheduledWorker" :: Text)
            , "jid"   .= ("due_rb_00000000000000000" :: Text)
            ]

      pushSidekiqScheduledJob store 1000.0 dueHsJob
      pushSidekiqScheduledJob store 2000.0 futHsJob
      pushSidekiqScheduledJob store 1000.0 dueRubyJob

      promoted <- promoteDueZSetJobs store allowed 1500.0
      promoted @?= [dueHsJob]
  ]

--------------------------------------------------------------------------------
-- 4. Built-In YAML Cron Scheduler & Multi-Node Lock Specs
--------------------------------------------------------------------------------

cronSpecs :: TestTree
cronSpecs = testGroup "WorkerQueue.Cron (YAML Schedule Parser, Capability Filter & NX Lock)"
  [ testCase "evaluates 5-field cron expressions accurately" $ do
      Right exprNightly <- pure (Cron.parseExpr "0 2 * * *")
      Right exprEvery15 <- pure (Cron.parseExpr "*/15 * * * *")

      let t0200 = UTCTime (fromGregorian 2026 11 1) (secondsToDiffTime (2 * 3600))
          t0215 = UTCTime (fromGregorian 2026 11 1) (secondsToDiffTime (2 * 3600 + 15 * 60))

      Cron.matchesTime exprNightly t0200 @?= True
      Cron.matchesTime exprNightly t0215 @?= False
      Cron.matchesTime exprEvery15 t0200 @?= True
      Cron.matchesTime exprEvery15 t0215 @?= True

  , testCase "validates YAML cron capabilities and deduplicates across worker nodes via Redis NX lock" $ do
      let yamlBs =
            "jobs:\n\
            \  cleanup_job:\n\
            \    cron: \"*/15 * * * *\"\n\
            \    class: \"StaleSessionCleanupWorker\"\n\
            \    queue: \"low\"\n\
            \    retry: 5\n\
            \    args:\n\
            \      cleanupDays: 30\n\
            \  ruby_job:\n\
            \    cron: \"* * * * *\"\n\
            \    class: \"UnregisteredRubyWorker\"\n\
            \    queue: \"default\"\n"
      Right sched <- pure (Cron.parseScheduleYaml yamlBs)
      let allowed = HS.fromList ["StaleSessionCleanupWorker"]
          (execSpecs, skipped) = Cron.validateCapabilities allowed sched

      length execSpecs @?= 1
      skipped          @?= ["ruby_job"]

      store   <- newRedisStore "cron-node"
      syncHdl <- startRedisSyncEngine store
      mpq     <- newMultiPriorityQueue 128
      metrics <- newMetrics
      jCnt    <- newIORef 0
      let tickTime = UTCTime (fromGregorian 2026 11 1) (secondsToDiffTime (2 * 3600 + 15 * 60))
          cronEnv  = Cron.Env execSpecs syncHdl mpq metrics jCnt

      c1 <- Cron.runCronM cronEnv (Cron.executeTick tickTime)
      c2 <- Cron.runCronM cronEnv (Cron.executeTick tickTime)
      stopRedisSyncEngine syncHdl

      c1 @?= 1
      c2 @?= 0 -- Second node/call in same minute is deduplicated by Redis NX lock!
  ]

--------------------------------------------------------------------------------
-- 5. Two-Stage Warp-Style TimeoutManager Specs
--------------------------------------------------------------------------------

timeoutManagerSpecs :: TestTree
timeoutManagerSpecs = testGroup "WorkerQueue.TimeoutManager (Two-Stage Soft/Hard Lease Expiration)"
  [ testCase "transitions through Stage-1 SoftExpired and Stage-2 HardExpired (throwTo JobLeaseExpired)" $ do
      -- Lease timeout = 30ms, Grace period = 30ms, background interval = 10s (we trigger sweepAndSwap manually)
      mgr <- startTimeoutManager 0.030 0.030 10_000_000
      expiredCallbackFired <- newIORef False
      caughtLeaseException <- newIORef False
      ticketSlot           <- newIORef Nothing

      _workerTid <- forkIO $ do
        let onExpire = writeIORef expiredCallbackFired True
        t <- registerJob mgr 99 "jid_stalled_000000000000" "InvoiceSettlementWorker" onExpire
        writeIORef ticketSlot (Just t)
        (threadDelay 5_000_000) `catch` \(_ :: JobLeaseExpired) ->
          writeIORef caughtLeaseException True

      threadDelay 10_000
      Just ticket <- readIORef ticketSlot

      -- Initial state is Active
      st0 <- checkLeaseState ticket
      case st0 of
        Active _ -> pure ()
        other    -> fail ("Expected Active, got " <> show other)

      -- Wait 40ms (> 30ms leaseTimeoutSeconds) and run first sweep -> Stage 1 SoftExpired
      threadDelay 40_000
      sweepAndSwap mgr
      st1 <- checkLeaseState ticket
      case st1 of
        SoftExpired _ -> pure ()
        other         -> fail ("Expected Stage-1 SoftExpired, got " <> show other)

      -- Wait another 40ms (> 30ms gracePeriodSeconds) and run second sweep -> Stage 2 HardExpired + throwTo
      threadDelay 40_000
      sweepAndSwap mgr
      threadDelay 10_000

      st2     <- checkLeaseState ticket
      cbFired <- readIORef expiredCallbackFired
      caught  <- readIORef caughtLeaseException
      stopTimeoutManager mgr

      st2     @?= HardExpired
      cbFired @?= True
      caught  @?= True
  ]

--------------------------------------------------------------------------------
-- 6. `WorkerM ctx` (`ReaderT`) & End-to-End Retry / DLQ Lifecycle Specs
--------------------------------------------------------------------------------

workerMonadAndEngineSpecs :: TestTree
workerMonadAndEngineSpecs = testGroup "WorkerQueue.Worker (`WorkerM ctx` ReaderT API & Retry/DLQ Lifecycle)"
  [ testCase "executes typed handler inside WorkerM ctx with SQLite WAL pool and acknowledges lease" $ do
      store   <- newRedisStore "worker-spec-node"
      pool    <- newSqliteWalPool "/tmp/workerQueues_tasty_spec.sqlite3" 60.0 2
      let pools = DbPools { pgPool = Nothing, sqlitePool = Just pool }
      withSqliteConn pools $ \conn -> do
        SQLite.execute_ conn "DROP TABLE IF EXISTS test_invoices;"
        SQLite.execute_ conn "CREATE TABLE test_invoices (id INTEGER PRIMARY KEY, currency TEXT NOT NULL);"

      counter <- newIORef 0
      let ctx = TestCtx "spec-ctx" counter
          handler :: SampleInvoice -> WorkerM TestCtx ()
          handler inv = do
            appCtx <- getAppContext
            liftIO $ assertEqual "ReaderT context tag matches" "spec-ctx" (tctxTag appCtx)
            withSqlite $ \conn ->
              SQLite.execute conn "INSERT INTO test_invoices (id, currency) VALUES (?, ?)" (invId inv, invCurrency inv)
            tickleLease
            liftIO $ atomicModifyIORef' (tctxCounter appCtx) (\n -> (n + 1, ()))

          disp = newDispatcher [registerWorker handler]

      metrics <- newMetrics
      let rawJson = encodeSidekiqJobJson Critical "spec_jid_000000000000001" 3 1000 (SampleInvoice 42 "CHF")
      pushSidekiqQueueJob store "critical" rawJson

      let cfg    = defaultEngineConfig { cfgWorkerCount = 2, cfgPollIntervalMicros = 1_000 }
          params = EngineParams
            { epConfig     = cfg
            , epRedisStore = store
            , epDbPools    = pools
            , epLogger     = newSilentLogger
            , epMetrics    = metrics
            , epAppContext = ctx
            , epDispatcher = disp
            }
      withWorkerEngine params $ do
        liftIO (threadDelay 30_000)
        flushEngineRedisSync

      finalCount <- readIORef counter
      leases     <- getWorkingLeases store
      closeDbPools pools

      finalCount    @?= 1
      length leases @?= 0

  , testCase "routes failing handler to Sidekiq retry ZSET when retryCount <= maxRetries and to DLQ when exceeded" $ do
      store   <- newRedisStore "retry-dlq-node"
      metrics <- newMetrics
      counter <- newIORef 0
      let ctx = TestCtx "fail-ctx" counter
          failingHandler :: SampleCleanup -> WorkerM TestCtx ()
          failingHandler _ = liftIO $ throwIO (userError "simulated DB constraint failure")
          disp = newDispatcher [registerWorker failingHandler]

      -- Job 1: maxRetries = 2 (should go to `retry` ZSET on first failure)
      let frameRetry = Frame.encodeJob Default SourceRails 2 1 "retry_jid_00000000000001" 1000 (SampleCleanup 7)
      -- Job 2: maxRetries = 0 (should go directly to `dead` DLQ ZSET on first failure)
      let frameDlq   = Frame.encodeJob Default SourceRails 0 2 "dlq_jid_0000000000000001" 1000 (SampleCleanup 7)

      tm        <- startTimeoutManager 5.0 1.0 100_000
      redisSync <- startRedisSyncEngine store
      mpq       <- newMultiPriorityQueue 64
      jCnt      <- newIORef 0
      let env = WorkerEnv
            { envLogger     = newSilentLogger
            , envMetrics    = metrics
            , envDbPools    = emptyDbPools
            , envTimeoutMgr = tm
            , envRedisSync  = redisSync
            , envQueue      = mpq
            , envJobCounter = jCnt
            , envAppContext = ctx
            }

      runEngineM (EngineEnv env disp) $ do
        processFrameWithTimeout frameRetry
        processFrameWithTimeout frameDlq
        flushEngineRedisSync
      stopTimeoutManager tm
      stopRedisSyncEngine redisSync

      retries <- getRetryZSet store
      deads   <- getDeadZSet store
      snap    <- snapshotMetrics metrics

      length retries       @?= 1
      length deads         @?= 1
      snapRetried snap     @?= 1
      snapDeadLettered snap @?= 1
  ]
