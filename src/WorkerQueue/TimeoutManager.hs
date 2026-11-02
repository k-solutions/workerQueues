{-# LANGUAGE GHC2024       #-}
{-# LANGUAGE LambdaCase    #-}
{-# LANGUAGE TupleSections #-}

module WorkerQueue.TimeoutManager
  ( TimeoutManager (..)
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
  ) where

import           Control.Concurrent (ThreadId, forkIO, killThread, myThreadId,
                                     threadDelay, throwTo)
import           Control.Exception  (Exception, mask_)
import           Control.Monad
import           Data.ByteString    (ByteString)
import           Data.Foldable
import           Data.IORef         (IORef, atomicModifyIORef', newIORef,
                                     readIORef, writeIORef)
import           Data.Word          (Word64)
import           GHC.Clock          (getMonotonicTime)

-- | Asynchronous exception thrown to hard-kill a worker thread whose lease expired.
data JobLeaseExpired = JobLeaseExpired
  { expiredJobId      :: !Word64
  , expiredSidekiqJid :: !ByteString
  , expiredClassTag   :: !ByteString
  } deriving (Show, Eq)

instance Exception JobLeaseExpired

-- | Two-stage lifecycle state of a worker job lease.
data JobState
  = Active !Double       -- ^ Job is running; stores last heartbeat POSIX timestamp (seconds)
  | SoftExpired !Double  -- ^ Stage 1: Lease expired at timestamp; awaiting cooperative exit or hard-kill
  | HardExpired          -- ^ Stage 2: Worker thread sent JobLeaseExpired via throwTo & recovery triggered
  | Completed            -- ^ Job finished and unregistered cleanly
  deriving (Show, Eq)

-- | Handle for an active job held by a worker thread.
data JobTicket = JobTicket
  { ticketJobId      :: !Word64
  , ticketSidekiqJid :: !ByteString
  , ticketClassTag   :: !ByteString
  , ticketThreadId   :: !ThreadId
  , ticketState      :: !(IORef JobState)
  , ticketOnExpire   :: !(IO ()) -- ^ Recovery action (increment retry backoff or route to Sidekiq DLQ)
  }

newtype TicketNode = TicketNode
  { nodeTicket :: JobTicket
  }

-- | Centralized Two-Stage Timeout Manager using Warp's double-IORef strategy.
data TimeoutManager = TimeoutManager
  { refActive           :: !(IORef [TicketNode])
  , refOld              :: !(IORef [TicketNode])
  , leaseTimeoutSeconds :: !Double -- ^ Allowed time between heartbeats before SoftExpired
  , gracePeriodSeconds  :: !Double -- ^ Grace window after SoftExpired before throwTo hard-kill
  , sweepIntervalMicros :: !Int    -- ^ Background sweep interval (e.g., 250_000 for 250ms)
  , mgrLoopThreadId     :: !(IORef (Maybe ThreadId))
  , mgrHardKills        :: !(IORef Word64)
  }

-- | Initialize and start the background Timeout Manager thread.
startTimeoutManager :: Double -> Double -> Int -> IO TimeoutManager
startTimeoutManager leaseSec graceSec sweepMicros = do
  activeRef <- newIORef []
  oldRef    <- newIORef []
  tidRef    <- newIORef Nothing
  killsRef  <- newIORef 0
  let mgr = TimeoutManager
        { refActive           = activeRef
        , refOld              = oldRef
        , leaseTimeoutSeconds = leaseSec
        , gracePeriodSeconds  = graceSec
        , sweepIntervalMicros = sweepMicros
        , mgrLoopThreadId     = tidRef
        , mgrHardKills        = killsRef
        }
  tid <- forkIO (timeoutManagerLoop mgr)
  writeIORef tidRef (Just tid)
  pure mgr

-- | Stop the background Timeout Manager thread cleanly.
stopTimeoutManager :: TimeoutManager -> IO ()
stopTimeoutManager mgr = do
  mTid <- atomicModifyIORef' (mgrLoopThreadId mgr) (Nothing,)
  for_ mTid killThread

timeoutManagerLoop :: TimeoutManager -> IO ()
timeoutManagerLoop mgr = go
  where
    go = do
      threadDelay (sweepIntervalMicros mgr)
      sweepAndSwap mgr
      go

-- | Parameters for a single two-stage timeout sweep pass.
data SweepContext = SweepContext
  { scMgr        :: !TimeoutManager
  , scNowSec     :: !Double
  , scSoftCutoff :: !Double
  , scHardCutoff :: !Double
  , scZeroGrace  :: !Bool
  }

-- | Swap-and-Merge Core Algorithm (Upgraded with Two-Stage Soft/Hard Expiration)
sweepAndSwap :: TimeoutManager -> IO ()
sweepAndSwap mgr = do
  !nowSec <- getMonotonicTime
  let !ctx = SweepContext
        { scMgr        = mgr
        , scNowSec     = nowSec
        , scSoftCutoff = nowSec - leaseTimeoutSeconds mgr
        , scHardCutoff = nowSec - gracePeriodSeconds mgr
        , scZeroGrace  = gracePeriodSeconds mgr <= 0
        }

  -- Step 1: Inspect tickets in refOld
  oldTickets  <- readIORef (refOld mgr)
  survivedOld <- processTickets ctx oldTickets

  -- Step 2: Atomically drain refActive
  newActive   <- atomicModifyIORef' (refActive mgr) ([],)

  -- Step 3: Process newActive and merge survivors into refOld
  survivedNew <- processTickets ctx newActive
  writeIORef (refOld mgr) (survivedNew ++ survivedOld)

processTickets :: SweepContext -> [TicketNode] -> IO [TicketNode]
processTickets !ctx nodes = go nodes []
  where
    go [] acc = pure acc
    go (node:rest) acc = do
      acc' <- processTicket ctx node acc
      go rest acc'

processTicket :: SweepContext -> TicketNode -> [TicketNode] -> IO [TicketNode]
processTicket !ctx node acc = do
  let t = nodeTicket node
  readIORef (ticketState t) >>= \case
    Completed ->
      pure acc

    HardExpired ->
      pure acc

    Active lastHeartbeat
      | lastHeartbeat < scSoftCutoff ctx && scZeroGrace ctx -> do
          triggerHardKill (scMgr ctx) t
          pure acc
      | lastHeartbeat < scSoftCutoff ctx -> do
          -- Stage 1: Transition to SoftExpired and retain for grace period
          writeIORef (ticketState t) (SoftExpired (scNowSec ctx))
          pure (node : acc)
      | otherwise ->
          pure (node : acc)

    SoftExpired softExpAt
      | softExpAt <= scHardCutoff ctx -> do
          -- Stage 2: Grace period elapsed! Hard-kill worker thread & trigger retry/DLQ
          triggerHardKill (scMgr ctx) t
          pure acc
      | otherwise ->
          pure (node : acc)
{-# INLINE processTicket #-}

triggerHardKill :: TimeoutManager -> JobTicket -> IO ()
triggerHardKill mgr t = mask_ $ do
  -- Atomically claim expiration so a concurrent completion does not double-fire
  claimed <- atomicModifyIORef' (ticketState t) $ \case
      Completed   -> (Completed, False)
      HardExpired -> (HardExpired, False)
      _           -> (HardExpired, True)
  when claimed $ do
      atomicModifyIORef' (mgrHardKills mgr) (\n -> (n + 1, ()))
      throwTo
        (ticketThreadId t)
        (JobLeaseExpired (ticketJobId t) (ticketSidekiqJid t) (ticketClassTag t))
      ticketOnExpire t

-- | Register a job lease on the current worker thread.
registerJob :: TimeoutManager -> Word64 -> ByteString -> ByteString -> IO () -> IO JobTicket
registerJob mgr !jId !jid !classTag onExpire = do
  tid     <- myThreadId
  !nowSec <- getMonotonicTime
  stRef   <- newIORef (Active nowSec)
  let !ticket = JobTicket
        { ticketJobId      = jId
        , ticketSidekiqJid = jid
        , ticketClassTag   = classTag
        , ticketThreadId   = tid
        , ticketState      = stRef
        , ticketOnExpire   = onExpire
        }
      !node = TicketNode ticket
  atomicModifyIORef' (refActive mgr) $ \active -> (node : active, ())
  pure ticket
{-# INLINE registerJob #-}

-- | Extend the job lease heartbeat if still Active.
tickleJob :: JobTicket -> IO ()
tickleJob ticket = do
  !nowSec <- getMonotonicTime
  atomicModifyIORef' (ticketState ticket) $ \case
      Active _ -> (Active nowSec, ())
      other    -> (other, ())
{-# INLINE tickleJob #-}

-- | Cooperative check allowing long-running handlers to abort cleanly during Stage 1 SoftExpired.
checkLeaseState :: JobTicket -> IO JobState
checkLeaseState ticket = readIORef (ticketState ticket)
{-# INLINE checkLeaseState #-}

-- | Mark the job ticket as Completed upon clean handler exit.
unregisterJob :: JobTicket -> IO ()
unregisterJob ticket =
  atomicModifyIORef' (ticketState ticket) $ \case
      HardExpired -> (HardExpired, ())
      _           -> (Completed, ())
{-# INLINE unregisterJob #-}
