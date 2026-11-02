{-# LANGUAGE GHC2024            #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings  #-}

module WorkerQueue.Sidekiq
  ( SidekiqEnvelope (..)
  , extractSidekiqClass
  , parseSidekiqEnvelope
  , sidekiqEnvelopeToFrame
  , sidekiqEnvelopeToFrameWithSlices
  , frameToSidekiqJson
  , encodeSidekiqJobJson
  , computeSidekiqBackoffSecs
  ) where

import           Data.Aeson           (FromJSON (..), ToJSON (..), Value (..),
                                       object, withObject, (.!=), (.:), (.:?),
                                       (.=))
import qualified Data.Aeson           as Aeson
import qualified Data.Aeson.KeyMap    as KM
import           Data.Aeson.Types     (Parser)
import           Data.ByteString      (ByteString)
import qualified Data.ByteString.Lazy as BSL
import           Data.Int             (Int64)
import           Data.Proxy           (Proxy (..))
import           Data.Text            (Text)
import qualified Data.Text.Encoding   as TE
import qualified Data.Vector          as V
import           Data.Word            (Word16, Word64)
import           System.Random        (randomRIO)
import           WorkerQueue.Frame    (Header (..), JobPayload (..),
                                       Params (..), Priority, Source (..),
                                       decodeHeader, encode, parsePriority,
                                       toQueueName)
import           WorkerQueue.Json     (FastSidekiqScan (..),
                                       scanSidekiqClassFast,
                                       scanSidekiqEnvelopeFast)

-- | Parsed representation of a Ruby on Rails Sidekiq / ActiveJob JSON job envelope.
data SidekiqEnvelope = SidekiqEnvelope
  { skClass      :: !ByteString -- ^ Effective worker class (`wrapped` for ActiveJob, else `class`)
  , skJid        :: !ByteString -- ^ 24-character hex Sidekiq JID
  , skQueue      :: !ByteString -- ^ Queue name (e.g. "critical", "default", "low")
  , skArgsBytes  :: !ByteString -- ^ Extracted JSON bytes for the typed worker payload
  , skMaxRetries :: !Word16     -- ^ Sidekiq `retry` configuration (default 25, or 0 if `retry: false`)
  , skRetryCount :: !Word16     -- ^ Current `retry_count`
  , skCreatedNs  :: !Int64      -- ^ Creation timestamp in nanoseconds (if present in JSON)
  } deriving (Show, Eq)

instance FromJSON SidekiqEnvelope where
  parseJSON = withObject "SidekiqEnvelope" $ \o -> do
    rawCls   <- o .: "class" :: Parser Text
    mWrapped <- o .:? "wrapped" :: Parser (Maybe Text)
    let effectiveCls = TE.encodeUtf8 $ maybe rawCls id mWrapped

    jidTxt   <- o .:? "jid" .!= "000000000000000000000000"
    queueTxt <- o .:? "queue" .!= "default"
    argsVal  <- o .:? "args" .!= Array V.empty

    retryVal <- o .:? "retry" :: Parser (Maybe Value)
    let maxRet = case retryVal of
          Just (Bool False) -> 0
          Just (Bool True)  -> 25
          Just (Number n)   -> max 0 ( round n )
          _                 -> 25

    retryCnt <- o .:? "retry_count" .!= (0 :: Word16)
    mCreated <- o .:? "created_at" :: Parser (Maybe Double)
    let createdNs = case mCreated of
          Just sec | sec > 1_000_000_000_000 -> round sec -- Already ns
          Just sec                           -> round (sec * 1_000_000_000)
          Nothing                            -> 0

    let payloadVal = unwrapSidekiqArgs mWrapped argsVal
        argsBytes  = BSL.toStrict (Aeson.encode payloadVal)

    pure SidekiqEnvelope
      { skClass      = effectiveCls
      , skJid        = TE.encodeUtf8 jidTxt
      , skQueue      = TE.encodeUtf8 queueTxt
      , skArgsBytes  = argsBytes
      , skMaxRetries = maxRet
      , skRetryCount = retryCnt
      , skCreatedNs  = createdNs
      }

-- | Unwrap Sidekiq `args: [obj]` or ActiveJob `args: [{"arguments": [obj]}]`
-- into the direct JSON value expected by the typed Haskell `JobPayload` decoder.
unwrapSidekiqArgs :: Maybe Text -> Value -> Value
unwrapSidekiqArgs mWrapped val =
  case val of
    Array vec | V.length vec == 1 ->
      let firstElem = V.unsafeHead vec
       in case (mWrapped, firstElem) of
            (Just _, Object km)
              | Just (Array ajArgs) <- KM.lookup "arguments" km
              , V.length ajArgs == 1 ->
                  V.unsafeHead ajArgs
            _ -> firstElem
    _ -> val

-- | Inspect a raw Sidekiq JSON ByteString and extract the effective class tag
-- (`wrapped` if present, otherwise `class`) using the zero-copy ByteString scanner.
extractSidekiqClass :: ByteString -> Maybe ByteString
extractSidekiqClass = scanSidekiqClassFast
{-# INLINE extractSidekiqClass #-}

parseSidekiqEnvelope :: ByteString -> Either String SidekiqEnvelope
parseSidekiqEnvelope = Aeson.eitherDecodeStrict'
{-# INLINE parseSidekiqEnvelope #-}

-- | Convert a raw Sidekiq JSON envelope into our 64-byte Hybrid Zero-Copy Binary Frame
-- returning `(priority, classSlice, argsSlice, rawFrame)` so callers can pre-warm
-- `JsonParamCache` and batch-enqueue without re-decoding the 64-byte header.
sidekiqEnvelopeToFrameWithSlices
  :: Word64     -- ^ Monotonic internal ticket/job ID
  -> Int64      -- ^ Fallback current monotonic/POSIX timestamp (ns)
  -> ByteString -- ^ Raw Sidekiq JSON envelope from Redis
  -> Either String (Priority, ByteString, ByteString, ByteString)
sidekiqEnvelopeToFrameWithSlices !jId !fallbackNowNs rawJson =
  case scanSidekiqEnvelopeFast rawJson of
    Just fss ->
      let !prio      = parsePriority (fssQueue fss)
          !createdNs = if fssCreatedNs fss > 0 then fssCreatedNs fss else fallbackNowNs
          !clsSlice  = fssClass fss
          !argsSlice = fssArgsSlice fss
          !frame     = encode Params
            { fpPriority     = prio
            , fpSource       = SourceRails
            , fpRetryCount   = fssRetryCount fss
            , fpMaxRetries   = fssMaxRetries fss
            , fpJobId        = jId
            , fpSidekiqJid   = fssJid fss
            , fpCreatedAtNs  = createdNs
            , fpQueueSlice   = fssQueue fss
            , fpTagSlice     = clsSlice
            , fpPayloadSlice = argsSlice
            }
       in Right (prio, clsSlice, argsSlice, frame)
    Nothing -> do
      env <- parseSidekiqEnvelope rawJson
      let !prio      = parsePriority (skQueue env)
          !createdNs = if skCreatedNs env > 0 then skCreatedNs env else fallbackNowNs
          !clsSlice  = skClass env
          !argsSlice = skArgsBytes env
          !frame     = encode Params
            { fpPriority     = prio
            , fpSource       = SourceRails
            , fpRetryCount   = skRetryCount env
            , fpMaxRetries   = skMaxRetries env
            , fpJobId        = jId
            , fpSidekiqJid   = skJid env
            , fpCreatedAtNs  = createdNs
            , fpQueueSlice   = skQueue env
            , fpTagSlice     = clsSlice
            , fpPayloadSlice = argsSlice
            }
      Right (prio, clsSlice, argsSlice, frame)
{-# INLINE sidekiqEnvelopeToFrameWithSlices #-}

-- | Convert a raw Sidekiq JSON envelope into our 64-byte Hybrid Zero-Copy Binary Frame
-- using the zero-allocation ByteString scanner (`scanSidekiqEnvelopeFast`), falling back
-- to Aeson only if unusual formatting is encountered.
sidekiqEnvelopeToFrame
  :: Word64     -- ^ Monotonic internal ticket/job ID
  -> Int64      -- ^ Fallback current monotonic/POSIX timestamp (ns)
  -> ByteString -- ^ Raw Sidekiq JSON envelope from Redis
  -> Either String ByteString
sidekiqEnvelopeToFrame !jId !fallbackNowNs rawJson =
  case sidekiqEnvelopeToFrameWithSlices jId fallbackNowNs rawJson of
    Right (_, _, _, frame) -> Right frame
    Left err               -> Left err
{-# INLINE sidekiqEnvelopeToFrame #-}

-- | Reconstruct a Sidekiq-compatible JSON envelope from a 64-byte binary frame
-- for storing in Redis `retry` or `dead` (DLQ) sorted sets.
frameToSidekiqJson :: ByteString -> Maybe Text -> Either String ByteString
frameToSidekiqJson rawFrame mErrMsg = do
  hdr <- decodeHeader rawFrame
  let argsVal = case Aeson.decodeStrict' (hdrPayloadSlice hdr) of
                  Just v  -> Array (V.singleton v)
                  Nothing -> Array V.empty
      basePairs =
        [ "class"       .= TE.decodeUtf8Lenient (hdrTagSlice hdr)
        , "jid"         .= TE.decodeUtf8Lenient (hdrSidekiqJid hdr)
        , "queue"       .= TE.decodeUtf8Lenient (hdrQueueSlice hdr)
        , "args"        .= argsVal
        , "retry"       .= hdrMaxRetries hdr
        , "retry_count" .= hdrRetryCount hdr
        , "created_at"  .= (fromIntegral (hdrCreatedAtNs hdr) / 1e9 :: Double)
        ]
      errPairs = case mErrMsg of
        Just msg -> ["error_message" .= msg, "error_class" .= ("HaskellWorkerException" :: Text)]
        Nothing  -> []
  Right $ BSL.toStrict $ Aeson.encode (object (basePairs ++ errPairs))

-- | Format a typed Haskell `JobPayload` as a standard Sidekiq JSON string
-- (used by Rails-simulation benchmarks and Redis queue producers).
encodeSidekiqJobJson
  :: forall a. JobPayload a
  => Priority
  -> ByteString -- ^ 24-char Sidekiq JID
  -> Word16     -- ^ maxRetries
  -> Int64      -- ^ createdAtNs
  -> a
  -> ByteString
encodeSidekiqJobJson prio jid maxRet createdNs payload =
  let clsName = TE.decodeUtf8Lenient (jobClassTag (Proxy @a))
      qName   = TE.decodeUtf8Lenient (toQueueName prio)
  in BSL.toStrict $ Aeson.encode $ object
        [ "class"      .= clsName
        , "jid"        .= TE.decodeUtf8Lenient jid
        , "queue"      .= qName
        , "args"       .= [toJSON payload]
        , "retry"      .= maxRet
        , "retry_count".= (0 :: Int)
        , "created_at" .= statusCreatedAt createdNs
        ]
  where
    statusCreatedAt ns = fromIntegral ns :: Int64

-- | Sidekiq exponential retry backoff formula with random jitter:
--   delay_secs = (retryCount ^ 4) + 15 + jitter(0, 30) * (retryCount + 1)
computeSidekiqBackoffSecs :: Word16 -> IO Double
computeSidekiqBackoffSecs retryCnt = do
  jitter <- randomRIO (0.0, 30.0 :: Double)
  let r = fromIntegral retryCnt :: Double
  pure $ (r ** 4) + 15.0 + jitter * (r + 1.0)
