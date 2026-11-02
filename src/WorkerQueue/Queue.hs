{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}

module WorkerQueue.Queue
  ( MultiPriorityQueue (..)
  , QueueDepths (..)
  , newMultiPriorityQueue
  , enqueueFrame
  , enqueueFramesBatch
  , enqueuePriorityFramesBatch
  , tryEnqueueFrame
  , dequeueWeighted
  , tryDequeueWeighted
  , getQueueDepths
  , isQueueEmpty
  ) where

import Control.Applicative ((<|>))
import Control.Concurrent.STM
  ( STM
  , TBQueue
  , atomically
  , isEmptyTBQueue
  , isFullTBQueue
  , lengthTBQueue
  , newTBQueueIO
  , readTBQueue
  , writeTBQueue
  )
import Data.ByteString (ByteString)
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.Word (Word64)
import Numeric.Natural (Natural)
import WorkerQueue.Frame (Priority (..), decodeHeaderZeroCopy, hdrPriority)

-- | Multi-priority in-memory bounded STM queues for 64-byte hybrid frames.
data MultiPriorityQueue = MultiPriorityQueue
  { qCritical   :: !(TBQueue ByteString)
  , qDefault    :: !(TBQueue ByteString)
  , qLow        :: !(TBQueue ByteString)
  , qSlotTicket :: !(IORef Word64)
  }

data QueueDepths = QueueDepths
  { depthCritical :: !Int
  , depthDefault  :: !Int
  , depthLow      :: !Int
  } deriving (Show, Eq)

-- | Create a new `MultiPriorityQueue` with the given per-queue capacity.
newMultiPriorityQueue :: Natural -> IO MultiPriorityQueue
newMultiPriorityQueue capacity = do
  qc   <- newTBQueueIO capacity
  qd   <- newTBQueueIO capacity
  ql   <- newTBQueueIO capacity
  slot <- newIORef 0
  pure MultiPriorityQueue
    { qCritical   = qc
    , qDefault    = qd
    , qLow        = ql
    , qSlotTicket = slot
    }

selectQueue :: MultiPriorityQueue -> Priority -> TBQueue ByteString
selectQueue mpq Critical = qCritical mpq
selectQueue mpq Default  = qDefault mpq
selectQueue mpq Low      = qLow mpq
{-# INLINE selectQueue #-}

-- | Enqueue a 64-byte binary frame into its priority TBQueue (blocks if full).
enqueueFrame :: MultiPriorityQueue -> ByteString -> IO ()
enqueueFrame mpq rawFrame = do
  let !prio = case decodeHeaderZeroCopy rawFrame of
                Right hdr -> hdrPriority hdr
                Left _    -> Default
  atomically $ writeTBQueue (selectQueue mpq prio) rawFrame
{-# INLINE enqueueFrame #-}

-- | Enqueue a batch of `(Priority, ByteString)` frames inside a single STM transaction
-- without re-decoding the 64-byte header.
enqueuePriorityFramesBatch :: MultiPriorityQueue -> [(Priority, ByteString)] -> IO ()
enqueuePriorityFramesBatch _   []     = pure ()
enqueuePriorityFramesBatch mpq tagged =
  atomically $ mapM_ (\(prio, f) -> writeTBQueue (selectQueue mpq prio) f) tagged
{-# INLINE enqueuePriorityFramesBatch #-}

-- | Enqueue a batch of 64-byte binary frames inside a single STM transaction,
-- avoiding per-item STM wakeup storms across waiting worker threads.
enqueueFramesBatch :: MultiPriorityQueue -> [ByteString] -> IO ()
enqueueFramesBatch _   []     = pure ()
enqueueFramesBatch mpq frames = do
  let tagged =
        [ ( case decodeHeaderZeroCopy f of
              Right hdr -> hdrPriority hdr
              Left _    -> Default
          , f
          )
        | f <- frames
        ]
  enqueuePriorityFramesBatch mpq tagged
{-# INLINE enqueueFramesBatch #-}

-- | Non-blocking enqueue; returns False if the target priority TBQueue is full.
tryEnqueueFrame :: MultiPriorityQueue -> ByteString -> IO Bool
tryEnqueueFrame mpq rawFrame = do
  let !prio = case decodeHeaderZeroCopy rawFrame of
                Right hdr -> hdrPriority hdr
                Left _    -> Default
      !q    = selectQueue mpq prio
  atomically $ do
    full <- isFullTBQueue q
    if full
      then pure False
      else do
        writeTBQueue q rawFrame
        pure True
{-# INLINE tryEnqueueFrame #-}

-- | Sidekiq-style weighted priority dequeue (70% Critical-first, 20% Default-first, 10% Low-first)
-- falling back across all three queues inside a single blocking STM transaction.
dequeueWeighted :: MultiPriorityQueue -> IO ByteString
dequeueWeighted mpq = do
  !slot <- atomicModifyIORef' (qSlotTicket mpq) (\n -> (n + 1, n `rem` 10))
  atomically (dequeueCascadeSTM mpq slot)
{-# INLINE dequeueWeighted #-}

-- | Non-blocking weighted dequeue returning `Nothing` when all queues are empty.
tryDequeueWeighted :: MultiPriorityQueue -> IO (Maybe ByteString)
tryDequeueWeighted mpq = do
  !slot <- atomicModifyIORef' (qSlotTicket mpq) (\n -> (n + 1, n `rem` 10))
  atomically $
    (Just <$> dequeueCascadeSTM mpq slot) <|> pure Nothing
{-# INLINE tryDequeueWeighted #-}

dequeueCascadeSTM :: MultiPriorityQueue -> Word64 -> STM ByteString
dequeueCascadeSTM mpq !slot
  -- Slots 0..6 (70%): Critical -> Default -> Low
  | slot < 7 =
      readTBQueue (qCritical mpq)
        <|> readTBQueue (qDefault mpq)
        <|> readTBQueue (qLow mpq)
  -- Slots 7..8 (20%): Default -> Critical -> Low
  | slot < 9 =
      readTBQueue (qDefault mpq)
        <|> readTBQueue (qCritical mpq)
        <|> readTBQueue (qLow mpq)
  -- Slot 9 (10%): Low -> Critical -> Default (prevents Low queue starvation)
  | otherwise =
      readTBQueue (qLow mpq)
        <|> readTBQueue (qCritical mpq)
        <|> readTBQueue (qDefault mpq)
{-# INLINE dequeueCascadeSTM #-}

getQueueDepths :: MultiPriorityQueue -> IO QueueDepths
getQueueDepths mpq = atomically $ do
  c <- fromIntegral <$> lengthTBQueue (qCritical mpq)
  d <- fromIntegral <$> lengthTBQueue (qDefault mpq)
  l <- fromIntegral <$> lengthTBQueue (qLow mpq)
  pure QueueDepths
    { depthCritical = c
    , depthDefault  = d
    , depthLow      = l
    }

isQueueEmpty :: MultiPriorityQueue -> IO Bool
isQueueEmpty mpq = atomically $ do
  ec <- isEmptyTBQueue (qCritical mpq)
  ed <- isEmptyTBQueue (qDefault mpq)
  el <- isEmptyTBQueue (qLow mpq)
  pure (ec && ed && el)
