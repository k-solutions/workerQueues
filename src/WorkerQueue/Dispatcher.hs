{-# LANGUAGE GHC2024            #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings  #-}

module WorkerQueue.Dispatcher
  ( -- * Capability-Aware Dispatcher & JSON Parameter Cache
    ReadySlot (..)
  , HandlerRegistration (..)
  , Dispatcher (..)
  , readyRingSize
  , readyRingMask
  , registerWorker
  , newDispatcher
  , registeredClassTags
  , getDispatcherCacheStats
  , prewarmSlice
  , dispatchZeroCopy
  ) where

import           Control.Exception      (throwIO)
import           Control.Monad.IO.Class (liftIO)
import           Data.Bits              ((.&.))
import           Data.ByteString        (ByteString)
import           Data.HashMap.Strict    (HashMap)
import qualified Data.HashMap.Strict    as HM
import           Data.HashSet           (HashSet)
import qualified Data.HashSet           as HS
import           Data.IORef             (IORef, atomicModifyIORef', newIORef,
                                         writeIORef)
import           Data.Proxy             (Proxy (..))
import qualified Data.Text              as T
import qualified Data.Text.Encoding     as TE
import           Data.Vector            (Vector)
import qualified Data.Vector            as V
import           Data.Word              (Word64)
import           System.IO.Unsafe       (unsafePerformIO)
import           WorkerQueue.Frame      (Header (..), JobPayload (..))
import           WorkerQueue.Json       (JsonCacheStats (..), JsonParamCache,
                                         decodeWithParamCache,
                                         getJsonCacheStats,
                                         lookupPrewarmedOrDecode,
                                         newJsonParamCache)
import           WorkerQueue.Monad      (JobHandler, WorkerM)

readyRingSize :: Int
readyRingSize = 16_384
{-# INLINE readyRingSize #-}

readyRingMask :: Int
readyRingMask = readyRingSize - 1
{-# INLINE readyRingMask #-}

data ReadySlot ctx = ReadySlot !Word64 (WorkerM ctx ())

data HandlerRegistration ctx = HandlerRegistration
  { hrClassTag :: !ByteString
  , hrBuild    :: !(IO (ByteString -> WorkerM ctx (), ByteString -> IO (Maybe (WorkerM ctx ())), IO JsonCacheStats))
  }

-- | Immutable handler lookup table, derived capability whitelist (`dispClassSet`),
-- per-class `JsonParamCache` prewarmers, and O(1) `dispReadyRing` for pre-decoded jobs.
data Dispatcher ctx = Dispatcher
  { dispHandlers   :: !(HashMap ByteString (ByteString -> WorkerM ctx ()))
  , dispPrewarmers :: !(HashMap ByteString (ByteString -> IO (Maybe (WorkerM ctx ()))))
  , dispReadyRing  :: !(Vector (IORef (ReadySlot ctx)))
  , dispClassSet   :: !(HashSet ByteString)
  , dispStatsFns   :: ![IO JsonCacheStats]
  }

-- | Register a typed `JobHandler ctx a` for a `JobPayload a` domain type,
-- attaching a dedicated 16-way striped Two-Level `JsonParamCache a`.
registerWorker :: forall a ctx. JobPayload a => JobHandler ctx a -> HandlerRegistration ctx
registerWorker handler =
  let !tag = jobClassTag (Proxy @a)
  in HandlerRegistration
        { hrClassTag = tag
        , hrBuild    = do
            cache <- newJsonParamCache 16_384 :: IO (JsonParamCache a)
            let invoke payloadSlice = do
                  res <- liftIO (lookupPrewarmedOrDecode cache payloadSlice)
                  case res of
                    Right val -> handler val
                    Left err  ->
                      liftIO $ throwIO $ userError $
                        "Failed to decode payload for class "
                          <> T.unpack (TE.decodeUtf8Lenient tag)
                          <> ": "
                          <> err
                prewarm payloadSlice = do
                  res <- decodeWithParamCache cache payloadSlice
                  case res of
                    Right val -> pure (Just (handler val))
                    Left _    -> pure Nothing
            pure (invoke, prewarm, getJsonCacheStats cache)
        }
{-# INLINE registerWorker #-}

-- | Build an immutable `Dispatcher ctx` and compute its `registeredClasses` whitelist.
newDispatcher :: [HandlerRegistration ctx] -> Dispatcher ctx
newDispatcher regs = unsafePerformIO $ do
  built <- mapM (\r -> (hrClassTag r,) <$> hrBuild r) regs
  ring  <- V.replicateM readyRingSize (newIORef (ReadySlot 0 (pure ())))
  let hMap = HM.fromList [(tag, inv)     | (tag, (inv, _, _))     <- built]
      pMap = HM.fromList [(tag, prewarm) | (tag, (_, prewarm, _)) <- built]
      sFns = [statsFn                    | (_, (_, _, statsFn))   <- built]
      cSet = HS.fromList (HM.keys hMap)
  pure Dispatcher
    { dispHandlers   = hMap
    , dispPrewarmers = pMap
    , dispReadyRing  = ring
    , dispClassSet   = cSet
    , dispStatsFns   = sFns
    }
{-# NOINLINE newDispatcher #-}

registeredClassTags :: Dispatcher ctx -> [ByteString]
registeredClassTags = HS.toList . dispClassSet

-- | Aggregate `JsonCacheStats` (`jcsExactHits`, `jcsTemplateHits`, `jcsMisses`)
-- across all registered handlers in `Dispatcher ctx`.
getDispatcherCacheStats :: Dispatcher ctx -> IO JsonCacheStats
getDispatcherCacheStats disp = do
  allStats <- sequence (dispStatsFns disp)
  pure $ foldl'
    (\(JsonCacheStats eh th ms) (JsonCacheStats e t m) ->
      JsonCacheStats (eh + e) (th + t) (ms + m))
    (JsonCacheStats 0 0 0)
    allStats

-- | Pre-decode and cache a job's JSON payload slice on a Dedicated JSON Processing Worker,
-- binding the ready `WorkerM ctx ()` closure into `dispReadyRing` at `jId .&. 16383`
-- so the Execution Worker dispatches in O(1) with zero hashing or HashMap lookups.
prewarmSlice :: Dispatcher ctx -> Word64 -> ByteString -> ByteString -> IO ()
prewarmSlice disp !jId clsSlice argsSlice =
  case HM.lookup clsSlice (dispPrewarmers disp) of
    Just prewarm -> do
      mAction <- prewarm argsSlice
      case mAction of
        Just action | jId > 0 -> do
          let !slotIdx = fromIntegral jId .&. readyRingMask
              !slotRef = V.unsafeIndex (dispReadyRing disp) slotIdx
          writeIORef slotRef (ReadySlot jId action)
        _ -> pure ()
    Nothing -> pure ()
{-# INLINE prewarmSlice #-}

-- | Route a decoded `Header` and its zero-copy `payloadSlice` to the matching `WorkerM ctx` handler.
-- First checks and consumes the O(1) `dispReadyRing` slot populated by the Dedicated JSON Processing Worker pool!
dispatchZeroCopy :: Dispatcher ctx -> Header -> ByteString -> WorkerM ctx ()
dispatchZeroCopy disp hdr payloadSlice = do
  let !jId = hdrJobId hdr
  if jId > 0
    then do
      let !slotIdx = fromIntegral jId .&. readyRingMask
          !slotRef = V.unsafeIndex (dispReadyRing disp) slotIdx
      ReadySlot slotJobId readyAction <- liftIO $
        atomicModifyIORef' slotRef (\s@(ReadySlot sid _) ->
          if sid == jId
            then (ReadySlot 0 (pure ()), s)
            else (s, ReadySlot 0 (pure ())))
      if slotJobId == jId
        then readyAction
        else fallbackInvoke
    else fallbackInvoke
  where
    fallbackInvoke =
      case HM.lookup (hdrTagSlice hdr) (dispHandlers disp) of
        Just invoke -> invoke payloadSlice
        Nothing     ->
          liftIO $ throwIO $ userError $
            "Unregistered job class tag: " <> T.unpack (TE.decodeUtf8Lenient (hdrTagSlice hdr))
{-# INLINE dispatchZeroCopy #-}
