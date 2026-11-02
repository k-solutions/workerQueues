{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}

module WorkerQueue.Metrics
  ( -- * Structured Logger
    Logger (..)
  , LogLevel (..)
  , newStderrLogger
  , newSilentLogger
  , logInfoIO
  , logWarnIO
  , logErrorIO

    -- * Atomic Telemetry & Latency Histograms
  , Metrics (..)
  , MetricsSnapshot (..)
  , LatencySummary (..)
  , newMetrics
  , incEnqueued
  , incCompleted
  , incRetried
  , incDeadLettered
  , incHardKilled
  , incRubySkipped
  , incCronTriggered
  , recordJobLatencyNs
  , snapshotMetrics
  , computeLatencySummary
  ) where

import Control.Concurrent.MVar (newMVar, withMVar)
import Control.Monad (when)
import Data.Int (Int64)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.List (sort)
import Data.Text (Text)
import Data.Text.IO qualified as TIO
import Data.Word (Word64)
import System.IO (stderr)
import WorkerQueue.Frame (Priority (..))

data LogLevel
  = LevelInfo
  | LevelWarn
  | LevelError
  deriving (Show, Eq, Ord)

newtype Logger = Logger
  { runLogger :: LogLevel -> Text -> IO ()
  }

logLevelPrefix :: LogLevel -> Text
logLevelPrefix LevelInfo  = "[INFO]  "
logLevelPrefix LevelWarn  = "[WARN]  "
logLevelPrefix LevelError = "[ERROR] "
{-# INLINE logLevelPrefix #-}

newStderrLogger :: LogLevel -> IO Logger
newStderrLogger minLevel = do
  lock <- newMVar ()
  pure $ Logger $ \lvl msg ->
    when (lvl >= minLevel) $
      withMVar lock $ \() ->
        TIO.hPutStrLn stderr (logLevelPrefix lvl <> msg)

newSilentLogger :: Logger
newSilentLogger = Logger $ \_ _ -> pure ()
{-# INLINE newSilentLogger #-}

logInfoIO :: Logger -> Text -> IO ()
logInfoIO l = runLogger l LevelInfo
{-# INLINE logInfoIO #-}

logWarnIO :: Logger -> Text -> IO ()
logWarnIO l = runLogger l LevelWarn
{-# INLINE logWarnIO #-}

logErrorIO :: Logger -> Text -> IO ()
logErrorIO l = runLogger l LevelError
{-# INLINE logErrorIO #-}

-- | Lock-free atomic metrics and latency sample collectors.
data Metrics = Metrics
  { mEnqueued          :: !(IORef Word64)
  , mCompleted         :: !(IORef Word64)
  , mRetried           :: !(IORef Word64)
  , mDeadLettered      :: !(IORef Word64)
  , mHardKilled        :: !(IORef Word64)
  , mRubySkipped       :: !(IORef Word64)
  , mCronTriggered     :: !(IORef Word64)
  , mAllLatenciesNs    :: !(IORef [Int64])
  , mCritLatenciesNs   :: !(IORef [Int64])
  }

data MetricsSnapshot = MetricsSnapshot
  { snapEnqueued      :: !Word64
  , snapCompleted     :: !Word64
  , snapRetried       :: !Word64
  , snapDeadLettered  :: !Word64
  , snapHardKilled    :: !Word64
  , snapRubySkipped   :: !Word64
  , snapCronTriggered :: !Word64
  } deriving (Show, Eq)

data LatencySummary = LatencySummary
  { latCount :: !Int
  , latP50Ms :: !Double
  , latP95Ms :: !Double
  , latP99Ms :: !Double
  , latP999Ms:: !Double
  , latMeanMs:: !Double
  , latMaxMs :: !Double
  } deriving (Show, Eq)

newMetrics :: IO Metrics
newMetrics =
  Metrics
    <$> newIORef 0
    <*> newIORef 0
    <*> newIORef 0
    <*> newIORef 0
    <*> newIORef 0
    <*> newIORef 0
    <*> newIORef 0
    <*> newIORef []
    <*> newIORef []

incCounter :: IORef Word64 -> Word64 -> IO ()
incCounter ref !delta = atomicModifyIORef' ref (\n -> (n + delta, ()))
{-# INLINE incCounter #-}

incEnqueued :: Metrics -> Word64 -> IO ()
incEnqueued m = incCounter (mEnqueued m)
{-# INLINE incEnqueued #-}

incCompleted :: Metrics -> IO ()
incCompleted m = incCounter (mCompleted m) 1
{-# INLINE incCompleted #-}

incRetried :: Metrics -> IO ()
incRetried m = incCounter (mRetried m) 1
{-# INLINE incRetried #-}

incDeadLettered :: Metrics -> IO ()
incDeadLettered m = incCounter (mDeadLettered m) 1
{-# INLINE incDeadLettered #-}

incHardKilled :: Metrics -> IO ()
incHardKilled m = incCounter (mHardKilled m) 1
{-# INLINE incHardKilled #-}

incRubySkipped :: Metrics -> Word64 -> IO ()
incRubySkipped m = incCounter (mRubySkipped m)
{-# INLINE incRubySkipped #-}

incCronTriggered :: Metrics -> IO ()
incCronTriggered m = incCounter (mCronTriggered m) 1
{-# INLINE incCronTriggered #-}

recordJobLatencyNs :: Metrics -> Priority -> Int64 -> IO ()
recordJobLatencyNs m prio !durNs = do
  let !clamped = max 0 durNs
  atomicModifyIORef' (mAllLatenciesNs m) (\xs -> (clamped : xs, ()))
  case prio of
    Critical -> atomicModifyIORef' (mCritLatenciesNs m) (\xs -> (clamped : xs, ()))
    _        -> pure ()
{-# INLINE recordJobLatencyNs #-}

snapshotMetrics :: Metrics -> IO MetricsSnapshot
snapshotMetrics m =
  MetricsSnapshot
    <$> readIORef (mEnqueued m)
    <*> readIORef (mCompleted m)
    <*> readIORef (mRetried m)
    <*> readIORef (mDeadLettered m)
    <*> readIORef (mHardKilled m)
    <*> readIORef (mRubySkipped m)
    <*> readIORef (mCronTriggered m)

computeLatencySummary :: [Int64] -> LatencySummary
computeLatencySummary [] = LatencySummary 0 0 0 0 0 0 0
computeLatencySummary samples =
  let sorted = sort samples
      n      = length sorted
      toMs x = fromIntegral x / 1_000_000.0
      pct p  =
        let idx = min (n - 1) (max 0 (floor (fromIntegral n * p :: Double)))
         in toMs (sorted !! idx)
      totalNs = sum (map fromIntegral sorted :: [Double])
   in LatencySummary
        { latCount  = n
        , latP50Ms  = pct 0.50
        , latP95Ms  = pct 0.95
        , latP99Ms  = pct 0.99
        , latP999Ms = pct 0.999
        , latMeanMs = (totalNs / fromIntegral n) / 1_000_000.0
        , latMaxMs  = toMs (last sorted)
        }
