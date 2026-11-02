{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}

module WorkerQueue.Redis
  ( -- * Lua Scripts for Redis/Valkey Capability-Aware Filtering
    claimExecutableBatchLua
  , promoteExecutableZSetLua

    -- * Redis / Valkey Store & State Inspection
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

    -- * Asynchronous Pipelined Sync Engine
  , RedisOp (..)
  , RedisSyncHandle (..)
  , startRedisSyncEngine
  , stopRedisSyncEngine
  , flushRedisSyncNow
  , acknowledgeJobAsync
  , scheduleRetryAsync
  , sendToDlqAsync
  , recordWorkingLeaseAsync
  ) where

import Control.Concurrent (ThreadId, forkIO, killThread, threadDelay)
import Control.Concurrent.STM
  ( STM
  , TBQueue
  , TVar
  , atomically
  , flushTBQueue
  , modifyTVar'
  , newTBQueueIO
  , newTVarIO
  , readTVar
  , readTVarIO
  , writeTBQueue
  , writeTVar
  )
import Control.Monad (when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Foldable (for_)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HM
import Data.HashSet (HashSet)
import Data.HashSet qualified as HS
import Data.IORef (IORef, atomicModifyIORef', newIORef, writeIORef)
import Data.List (partition)
import Data.Sequence (Seq (..), (|>))
import Data.Sequence qualified as Seq
import WorkerQueue.Json (scanSidekiqJidFast)
import WorkerQueue.Metrics (Metrics, incRubySkipped)
import WorkerQueue.Sidekiq (extractSidekiqClass)

-- | Exact Redis/Valkey Lua script for capability-aware batch claiming from `queue:<name>`.
-- Only claims jobs whose `wrapped` (ActiveJob) or `class` is in ARGV[2..N] (`registeredClasses`).
claimExecutableBatchLua :: ByteString
claimExecutableBatchLua =
  "local allowed = {}\n\
  \for i = 2, #ARGV do allowed[ARGV[i]] = true end\n\
  \local batch = {}\n\
  \local max_claim = tonumber(ARGV[1])\n\
  \local items = redis.call('LRANGE', KEYS[1], -max_claim * 2, -1)\n\
  \for i = #items, 1, -1 do\n\
  \  if #batch >= max_claim then break end\n\
  \  local raw = items[i]\n\
  \  local ok, job = pcall(cjson.decode, raw)\n\
  \  if ok and type(job) == 'table' then\n\
  \    local cls = job['wrapped'] or job['class']\n\
  \    if cls and allowed[cls] and job['jid'] then\n\
  \      redis.call('LREM', KEYS[1], -1, raw)\n\
  \      redis.call('HSET', KEYS[2], job['jid'], raw)\n\
  \      table.insert(batch, raw)\n\
  \    end\n\
  \  end\n\
  \end\n\
  \return batch"

-- | Exact Redis/Valkey Lua script for capability-aware ZSET promotion (`schedule` and `retry`).
promoteExecutableZSetLua :: ByteString
promoteExecutableZSetLua =
  "local allowed = {}\n\
  \for i = 3, #ARGV do allowed[ARGV[i]] = true end\n\
  \local now_score = tonumber(ARGV[1])\n\
  \local max_batch = tonumber(ARGV[2])\n\
  \local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now_score, 'LIMIT', 0, max_batch * 2)\n\
  \local promoted = {}\n\
  \for i = 1, #due do\n\
  \  if #promoted >= max_batch then break end\n\
  \  local raw = due[i]\n\
  \  local ok, job = pcall(cjson.decode, raw)\n\
  \  if ok and type(job) == 'table' then\n\
  \    local cls = job['wrapped'] or job['class']\n\
  \    if cls and allowed[cls] then\n\
  \      redis.call('ZREM', KEYS[1], raw)\n\
  \      table.insert(promoted, raw)\n\
  \    end\n\
  \  end\n\
  \end\n\
  \return promoted"

data ZSetEntry = ZSetEntry
  { zsetScore  :: !Double
  , zsetMember :: !ByteString
  } deriving (Show, Eq)

data QueueState = QueueState !(HashSet ByteString) !Int !(Seq ByteString)

emptyQueueState :: QueueState
emptyQueueState = QueueState HS.empty 0 Seq.empty
{-# INLINE emptyQueueState #-}

-- | Atomic Sidekiq-compatible Redis/Valkey state store supporting
-- capability-aware Lua-equivalent operations, working leases, ZSETs, and NX locks.
data RedisStore = RedisStore
  { rsNodeId     :: !ByteString
  , rsQueues     :: !(TVar (HashMap ByteString QueueState))
  , rsWorking    :: !(TVar (HashMap ByteString ByteString))
  , rsScheduleZ  :: !(TVar [ZSetEntry])
  , rsRetryZ     :: !(TVar [ZSetEntry])
  , rsDeadZ      :: !(TVar [ZSetEntry])
  , rsCronLocks  :: !(TVar (HashMap ByteString (ByteString, Double)))
  }

newRedisStore :: ByteString -> IO RedisStore
newRedisStore nodeId =
  RedisStore nodeId
    <$> newTVarIO HM.empty
    <*> newTVarIO HM.empty
    <*> newTVarIO []
    <*> newTVarIO []
    <*> newTVarIO []
    <*> newTVarIO HM.empty

-- | Simulate Rails `LPUSH queue:<name> <sidekiq_json>` (pushes to head; oldest is at tail).
pushSidekiqQueueJob :: RedisStore -> ByteString -> ByteString -> IO ()
pushSidekiqQueueJob store queueName rawJson =
  atomically $ modifyTVar' (rsQueues store) $ \qm ->
    let QueueState allowed skipCnt cur = HM.lookupDefault emptyQueueState queueName qm
     in HM.insert queueName (QueueState allowed skipCnt (cur |> rawJson)) qm
{-# INLINE pushSidekiqQueueJob #-}

-- | Simulate Rails `ZADD schedule <run_at_epoch_sec> <sidekiq_json>`.
pushSidekiqScheduledJob :: RedisStore -> Double -> ByteString -> IO ()
pushSidekiqScheduledJob store !runAtSec rawJson =
  atomically $ modifyTVar' (rsScheduleZ store) $ \zs ->
    ZSetEntry runAtSec rawJson : zs
{-# INLINE pushSidekiqScheduledJob #-}

-- | Parameters for `claimExecutableBatch`.
data ClaimBatchParams = ClaimBatchParams
  { cbpStore          :: !RedisStore
  , cbpMetrics        :: !Metrics
  , cbpAllowedClasses :: !(HashSet ByteString) -- ^ `registeredClasses` from `Dispatcher`
  , cbpQueueName      :: !ByteString           -- ^ Queue name (e.g. "critical", "default", "low")
  , cbpMaxClaim       :: !Int                  -- ^ Max batch size to claim
  }

-- | Execute capability-aware atomic batch claim on `queue:<name>`.
-- Only pops jobs whose `wrapped` or `class` is in `registeredClasses`.
-- Non-matching (e.g. Ruby Sidekiq) jobs remain untouched in FIFO order!
claimExecutableBatch :: ClaimBatchParams -> IO [ByteString]
claimExecutableBatch !params = do
  let store          = cbpStore params
      metrics        = cbpMetrics params
      allowedClasses = cbpAllowedClasses params
      queueName      = cbpQueueName params
      !maxClaim      = cbpMaxClaim params
  (claimed, skippedCount) <- atomically $ do
    qm <- readTVar (rsQueues store)
    let QueueState prevAllowed prevSkip qSeq = HM.lookupDefault emptyQueueState queueName qm
        !startSkip = if prevAllowed == allowedClasses then prevSkip else 0
        (alreadySkippedSeq, toScanSeq) = Seq.splitAt startSkip qSeq
        (matched, keptSuffix, newlySkipped) = scanQueueFIFO allowedClasses maxClaim toScanSeq
        !totalSkippedPrefix = startSkip + newlySkipped
        !remaining = alreadySkippedSeq <> keptSuffix
        commitQueueState =
          writeTVar (rsQueues store) $!
            HM.insert queueName (QueueState allowedClasses totalSkippedPrefix remaining) qm
        commitWorkingLeases =
          modifyTVar' (rsWorking store) $ \wm ->
            foldl' (\acc raw -> HM.insert (extractJidKey raw) raw acc) wm matched
        applyClaim
          | null matched && newlySkipped == 0 && prevAllowed == allowedClasses =
              pure ([], 0)
          | null matched = do
              commitQueueState
              pure ([], newlySkipped)
          | otherwise = do
              commitQueueState
              commitWorkingLeases
              pure (matched, newlySkipped)
    applyClaim
  when (skippedCount > 0) $
    incRubySkipped metrics (fromIntegral skippedCount)
  pure claimed
{-# INLINE claimExecutableBatch #-}

scanQueueFIFO
  :: HashSet ByteString
  -> Int
  -> Seq ByteString
  -> ([ByteString], Seq ByteString, Int)
scanQueueFIFO allowed !maxClaim = go [] 0 Seq.empty 0
  where
    go claimedAcc !_ keptAcc !skipped Seq.Empty =
      (reverse claimedAcc, keptAcc, skipped)
    go claimedAcc !claimedCnt keptAcc !skipped (x :<| xs)
      | claimedCnt >= maxClaim =
          (reverse claimedAcc, (keptAcc |> x) <> xs, skipped)
      | otherwise =
          case extractSidekiqClass x of
            Just cls | HS.member cls allowed ->
              go (x : claimedAcc) (claimedCnt + 1) keptAcc skipped xs
            _ ->
              -- Unregistered / Ruby-only job: leave untouched in queue!
              go claimedAcc claimedCnt (keptAcc |> x) (skipped + 1) xs
{-# INLINE scanQueueFIFO #-}

extractJidKey :: ByteString -> ByteString
extractJidKey raw =
  case scanSidekiqJidFast raw of
    Just jid
      | BS.length jid == 24 -> jid
      | BS.length jid > 24  -> BS.take 24 jid
      | otherwise           -> jid <> BS.replicate (24 - BS.length jid) 0x30
    Nothing -> raw
{-# INLINE extractJidKey #-}

-- | Atomically promote due jobs (`score <= nowEpochSec`) from `schedule` and `retry` ZSETs
-- whose `wrapped` or `class` is in `registeredClasses`, leaving Ruby-only jobs untouched.
promoteDueZSetJobs
  :: RedisStore
  -> HashSet ByteString
  -> Double -- ^ Current POSIX timestamp (seconds)
  -> IO [ByteString]
promoteDueZSetJobs store allowedClasses nowSec = atomically $ do
  schedPromoted <- promoteFromTVar (rsScheduleZ store)
  retryPromoted <- promoteFromTVar (rsRetryZ store)
  let allPromoted = schedPromoted ++ retryPromoted
  modifyTVar' (rsWorking store) $ \wm ->
    foldl' (\acc raw -> HM.insert (extractJidKey raw) raw acc) wm allPromoted
  pure allPromoted
  where
    promoteFromTVar :: TVar [ZSetEntry] -> STM [ByteString]
    promoteFromTVar tvar = do
      entries <- readTVar tvar
      let isExecutableDue (ZSetEntry score raw) =
            score <= nowSec &&
            case extractSidekiqClass raw of
              Just cls -> HS.member cls allowedClasses
              Nothing  -> False
          (dueExec, keep) = partition isExecutableDue entries
      writeTVar tvar keep
      pure (map zsetMember dueExec)

-- | Atomic distributed lock (`SET wq:cron:lock:<job>:<minute> <node_id> NX EX 90`).
acquireCronLock
  :: RedisStore
  -> ByteString -- ^ Lock key
  -> Double     -- ^ Current POSIX timestamp (seconds)
  -> Double     -- ^ TTL (seconds)
  -> IO Bool
acquireCronLock store lockKey nowSec ttlSec = atomically $ do
  locks <- readTVar (rsCronLocks store)
  case HM.lookup lockKey locks of
    Just (_, expiresAt) | expiresAt > nowSec ->
      pure False
    _ -> do
      writeTVar (rsCronLocks store) (HM.insert lockKey (rsNodeId store, nowSec + ttlSec) locks)
      pure True

getQueueItems :: RedisStore -> ByteString -> IO [ByteString]
getQueueItems store qName = do
  qm <- readTVarIO (rsQueues store)
  let QueueState _ _ seqItems = HM.lookupDefault emptyQueueState qName qm
  pure $ foldr (:) [] seqItems

getWorkingLeases :: RedisStore -> IO (HashMap ByteString ByteString)
getWorkingLeases store = readTVarIO (rsWorking store)

getRetryZSet :: RedisStore -> IO [ZSetEntry]
getRetryZSet store = readTVarIO (rsRetryZ store)

getDeadZSet :: RedisStore -> IO [ZSetEntry]
getDeadZSet store = readTVarIO (rsDeadZ store)

--------------------------------------------------------------------------------
-- Asynchronous Pipelined Redis/Valkey Write-Behind Sync Engine
--------------------------------------------------------------------------------

data RedisOp
  = OpAckLease !ByteString
  | OpScheduleRetry !ByteString !Double !ByteString
  | OpSendToDlq !ByteString !Double !ByteString
  | OpRecordLease !ByteString !ByteString
  deriving (Show, Eq)

data RedisSyncHandle = RedisSyncHandle
  { rshStore    :: !RedisStore
  , rshOpQueue  :: !(TBQueue RedisOp)
  , rshThreadId :: !(IORef (Maybe ThreadId))
  }

startRedisSyncEngine :: RedisStore -> IO RedisSyncHandle
startRedisSyncEngine store = do
  opQ    <- newTBQueueIO 65_536
  tidRef <- newIORef Nothing
  let hdl = RedisSyncHandle
        { rshStore    = store
        , rshOpQueue  = opQ
        , rshThreadId = tidRef
        }
  tid <- forkIO (redisSyncLoop hdl)
  writeIORef tidRef (Just tid)
  pure hdl

stopRedisSyncEngine :: RedisSyncHandle -> IO ()
stopRedisSyncEngine hdl = do
  flushRedisSyncNow hdl
  mTid <- atomicModifyIORef' (rshThreadId hdl) (Nothing,)
  maybe (pure ()) killThread mTid

redisSyncLoop :: RedisSyncHandle -> IO ()
redisSyncLoop hdl = go
  where
    go = do
      threadDelay 1_000 -- 1ms pipelined batch window
      flushRedisSyncNow hdl
      go

-- | Drain and apply all pending Redis operations in a single batched STM transaction.
flushRedisSyncNow :: RedisSyncHandle -> IO ()
flushRedisSyncNow hdl = atomically $ do
  ops <- flushTBQueue (rshOpQueue hdl)
  for_ ops (applyRedisOpSTM (rshStore hdl))

applyRedisOpSTM :: RedisStore -> RedisOp -> STM ()
applyRedisOpSTM store op =
  case op of
    OpAckLease jid ->
      modifyTVar' (rsWorking store) (HM.delete jid)

    OpScheduleRetry jid nextRunSec sidekiqJson -> do
      modifyTVar' (rsWorking store) (HM.delete jid)
      modifyTVar' (rsRetryZ store) (ZSetEntry nextRunSec sidekiqJson :)

    OpSendToDlq jid failedAtSec sidekiqJson -> do
      modifyTVar' (rsWorking store) (HM.delete jid)
      modifyTVar' (rsDeadZ store) (ZSetEntry failedAtSec sidekiqJson :)

    OpRecordLease jid rawJson ->
      modifyTVar' (rsWorking store) (HM.insert jid rawJson)
{-# INLINE applyRedisOpSTM #-}

acknowledgeJobAsync :: RedisSyncHandle -> ByteString -> IO ()
acknowledgeJobAsync hdl jid =
  atomically $ writeTBQueue (rshOpQueue hdl) (OpAckLease jid)
{-# INLINE acknowledgeJobAsync #-}

scheduleRetryAsync :: RedisSyncHandle -> ByteString -> Double -> ByteString -> IO ()
scheduleRetryAsync hdl jid !nextRunSec sidekiqJson =
  atomically $ writeTBQueue (rshOpQueue hdl) (OpScheduleRetry jid nextRunSec sidekiqJson)
{-# INLINE scheduleRetryAsync #-}

sendToDlqAsync :: RedisSyncHandle -> ByteString -> Double -> ByteString -> IO ()
sendToDlqAsync hdl jid !failedAtSec sidekiqJson =
  atomically $ writeTBQueue (rshOpQueue hdl) (OpSendToDlq jid failedAtSec sidekiqJson)
{-# INLINE sendToDlqAsync #-}

recordWorkingLeaseAsync :: RedisSyncHandle -> ByteString -> ByteString -> IO ()
recordWorkingLeaseAsync hdl jid rawJson =
  atomically $ writeTBQueue (rshOpQueue hdl) (OpRecordLease jid rawJson)
{-# INLINE recordWorkingLeaseAsync #-}
