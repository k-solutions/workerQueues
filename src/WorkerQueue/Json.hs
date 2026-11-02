{-# LANGUAGE GHC2024            #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings  #-}

module WorkerQueue.Json
  ( -- * Zero-Copy ByteString JSON Scanner (Zero DOM Allocation)
    FastSidekiqScan (..)
  , scanJsonStringField
  , scanJsonIntField
  , scanSidekiqClassFast
  , scanSidekiqJidFast
  , scanSidekiqArgsSlice
  , scanSidekiqEnvelopeFast

    -- * Two-Level Striped JSON Parameter & Template Cache
  , JsonParamCache (..)
  , JsonCacheStats (..)
  , newJsonParamCache
  , decodeWithParamCache
  , lookupPrewarmedOrDecode
  , getJsonCacheStats
  ) where

import           Control.Applicative      ((<|>))
import           Control.Monad            (guard)
import           Data.Aeson               (Result (..), Value (..), fromJSON)
import qualified Data.Aeson               as Aeson
import qualified Data.Aeson.Key           as Key
import           Data.Aeson.KeyMap        (KeyMap)
import qualified Data.Aeson.KeyMap        as KM
import           Data.Bits                ((.&.))
import           Data.ByteString          (ByteString)
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Internal as BSI
import           Data.Hashable            (hash)
import           Data.HashMap.Strict      (HashMap)
import qualified Data.HashMap.Strict      as HM
import           Data.Int                 (Int64)
import           Data.IORef               (IORef, atomicModifyIORef', newIORef,
                                           readIORef)
import           Data.Maybe               (fromMaybe)
import qualified Data.Text.Encoding       as TE
import           Data.Vector              (Vector)
import qualified Data.Vector              as V
import           Data.Word                (Word16, Word64, Word8)
import           WorkerQueue.Frame        (JobPayload (..))

--------------------------------------------------------------------------------
-- 1. Zero-Copy ByteString JSON Field & Slice Scanner
--------------------------------------------------------------------------------

wDoubleQuote, wColon, wComma, wOpenBrace, wCloseBrace, wOpenBracket, wCloseBracket, wBackslash :: Word8
wDoubleQuote  = 0x22 -- '"'
wColon        = 0x3A -- ':'
wComma        = 0x2C -- ','
wOpenBrace    = 0x7B -- '{'
wCloseBrace   = 0x7D -- '}'
wOpenBracket  = 0x5B -- '['
wCloseBracket = 0x5D -- ']'
wBackslash    = 0x5C -- '\\'

isWhitespace :: Word8 -> Bool
isWhitespace w = w == 0x20 || w == 0x09 || w == 0x0A || w == 0x0D
{-# INLINE isWhitespace #-}

isDigitByte :: Word8 -> Bool
isDigitByte w = w >= 0x30 && w <= 0x39
{-# INLINE isDigitByte #-}

skipSpaces :: ByteString -> ByteString
skipSpaces = BS.dropWhile isWhitespace
{-# INLINE skipSpaces #-}

-- | Fast zero-copy extraction of a string field (e.g. `"class":"..."`)
-- from a JSON ByteString without allocating an Aeson DOM tree.
scanJsonStringField :: ByteString -> ByteString -> Maybe ByteString
scanJsonStringField needleKey raw = do
  let (_, afterKey) = BS.breakSubstring needleKey raw
  guard (not (BS.null afterKey))
  let afterNeedle = BS.drop (BS.length needleKey) afterKey
  (firstByte, rest) <- BS.uncons (skipSpaces (dropPastColon afterNeedle))
  guard (firstByte == wDoubleQuote)
  extractQuotedSlice rest
{-# INLINE scanJsonStringField #-}

dropPastColon :: ByteString -> ByteString
dropPastColon bs =
  case BS.uncons (skipSpaces bs) of
    Just (b, rest) | b == wColon -> rest
    _                            -> BS.empty
{-# INLINE dropPastColon #-}

-- | Return the zero-copy slice up to the closing unescaped `"`.
-- Uses C `memchr` (`BS.elemIndex`) on the unescaped fast path.
extractQuotedSlice :: ByteString -> Maybe ByteString
extractQuotedSlice bs =
  case BS.elemIndex wDoubleQuote bs of
    Nothing -> Nothing
    Just i
      | i == 0 || BS.index bs (i - 1) /= wBackslash -> Just (BS.take i bs)
      | otherwise                                   -> go 0 False
  where
    len = BS.length bs
    go !i !escaped
      | i >= len  = Nothing
      | escaped   = go (i + 1) False
      | otherwise =
          case BS.index bs i of
            b | b == wBackslash   -> go (i + 1) True
              | b == wDoubleQuote -> Just (BS.take i bs)
              | otherwise         -> go (i + 1) False
{-# INLINE extractQuotedSlice #-}

-- | Fast zero-copy extraction of an integer/numeric field (e.g. `"retry":25`, `"created_at":...`).
scanJsonIntField :: ByteString -> ByteString -> Maybe Int64
scanJsonIntField needleKey raw = do
  let (_, afterKey) = BS.breakSubstring needleKey raw
  guard (not (BS.null afterKey))
  let afterNeedle = BS.drop (BS.length needleKey) afterKey
  parseLeadingInt64 (skipSpaces (dropPastColon afterNeedle))
{-# INLINE scanJsonIntField #-}

parseLeadingInt64 :: ByteString -> Maybe Int64
parseLeadingInt64 bs = do
  (firstByte, tailBs) <- BS.uncons bs
  let (neg, rest) = if firstByte == 0x2D then (True, tailBs) else (False, bs)
      digits      = BS.takeWhile isDigitByte rest
  guard (not (BS.null digits))
  let !n = BS.foldl' (\acc w -> acc * 10 + fromIntegral (w - 0x30)) (0 :: Int64) digits
  pure (if neg then negate n else n)
{-# INLINE parseLeadingInt64 #-}

-- | Extract the effective Sidekiq worker class (`"wrapped"` for ActiveJob, else `"class"`)
-- in a single zero-copy ByteString scan.
scanSidekiqClassFast :: ByteString -> Maybe ByteString
scanSidekiqClassFast raw =
  scanJsonStringField "\"wrapped\"" raw <|> scanJsonStringField "\"class\"" raw
{-# INLINE scanSidekiqClassFast #-}

-- | Extract the 24-byte Sidekiq `"jid"` slice in a single zero-copy ByteString scan.
scanSidekiqJidFast :: ByteString -> Maybe ByteString
scanSidekiqJidFast = scanJsonStringField "\"jid\""
{-# INLINE scanSidekiqJidFast #-}

-- | Extract a zero-copy ByteString slice of the inner JSON payload inside
-- ActiveJob `"arguments":[<payload>]` or Sidekiq `"args":[<payload>]` / `"args":<payload>`.
scanSidekiqArgsSlice :: ByteString -> Maybe ByteString
scanSidekiqArgsSlice raw =
  extractArgsPayload "\"arguments\"" raw <|> extractArgsPayload "\"args\"" raw
{-# INLINE scanSidekiqArgsSlice #-}

extractArgsPayload :: ByteString -> ByteString -> Maybe ByteString
extractArgsPayload key raw = do
  let (_, afterKey) = BS.breakSubstring key raw
  guard (not (BS.null afterKey))
  let afterNeedle = BS.drop (BS.length key) afterKey
      valStart    = skipSpaces (dropPastColon afterNeedle)
  (firstByte, rest) <- BS.uncons valStart
  case firstByte of
    b | b == wOpenBracket ->
          unwrapSingleObjectArray rest <|> takeBalancedSlice wOpenBracket wCloseBracket valStart
      | b == wOpenBrace ->
          takeBalancedSlice wOpenBrace wCloseBrace valStart
      | otherwise ->
          Nothing
{-# INLINE extractArgsPayload #-}

-- | Unwrap a single-element object array `[{...}]` into its inner `{...}` slice.
unwrapSingleObjectArray :: ByteString -> Maybe ByteString
unwrapSingleObjectArray afterOpenBracket = do
  let inner = skipSpaces afterOpenBracket
  (firstInner, _) <- BS.uncons inner
  guard (firstInner == wOpenBrace)
  objSlice <- takeBalancedSlice wOpenBrace wCloseBrace inner
  let afterObj = skipSpaces (BS.drop (BS.length objSlice) inner)
  (closeByte, _) <- BS.uncons afterObj
  guard (closeByte == wCloseBracket)
  pure objSlice
{-# INLINE unwrapSingleObjectArray #-}

-- | Return a zero-copy ByteString slice spanning from `openByte` to its balanced `closeByte`,
-- respecting quoted strings and escape sequences.
takeBalancedSlice :: Word8 -> Word8 -> ByteString -> Maybe ByteString
takeBalancedSlice !openByte !closeByte (BSI.BS fp len)
  | len <= 0  = Nothing
  | otherwise = go (BSI.BS fp len) (0 :: Int) (0 :: Int) False False
  where
    go !bs !i !depth !inStr !escaped
      | i >= len  = Nothing
      | escaped   = go bs (i + 1) depth inStr False
      | inStr     = stepInString bs i depth (BS.index bs i)
      | otherwise = stepOutsideString bs i depth (BS.index bs i)

    stepInString !bs !i !depth !b
      | b == wBackslash   = go bs (i + 1) depth True True
      | b == wDoubleQuote = go bs (i + 1) depth False False
      | otherwise         = go bs (i + 1) depth True False

    stepOutsideString !bs !i !depth !b
      | b == wDoubleQuote            = go bs (i + 1) depth True False
      | b == openByte                = go bs (i + 1) (depth + 1) False False
      | b == closeByte && depth <= 1 = Just (BS.take (i + 1) bs)
      | b == closeByte               = go bs (i + 1) (depth - 1) False False
      | otherwise                    = go bs (i + 1) depth False False
{-# INLINE takeBalancedSlice #-}

-- | Result of zero-copy scanning a Sidekiq JSON envelope without Aeson DOM allocation.
data FastSidekiqScan = FastSidekiqScan
  { fssClass      :: !ByteString
  , fssJid        :: !ByteString
  , fssQueue      :: !ByteString
  , fssArgsSlice  :: !ByteString
  , fssMaxRetries :: !Word16
  , fssRetryCount :: !Word16
  , fssCreatedNs  :: !Int64
  } deriving (Show, Eq)

-- | Scan a Sidekiq / ActiveJob JSON ByteString into zero-copy slices in a single pass
-- without building an Aeson Value tree or re-serializing `args`.
scanSidekiqEnvelopeFast :: ByteString -> Maybe FastSidekiqScan
scanSidekiqEnvelopeFast raw = do
  let mWrapped = scanJsonStringField "\"wrapped\"" raw
  cls <- case mWrapped of
           Just w  -> Just w
           Nothing -> scanJsonStringField "\"class\"" raw
  let jid    = fromMaybe  "000000000000000000000000" $ scanSidekiqJidFast raw
      q      = fromMaybe "default" $  scanJsonStringField "\"queue\"" raw
      mbArgs = fromMaybe "[]" $  extractArgsPayload "\"args\"" raw

      argsSlice = case mWrapped of
                    Just _ -> fromMaybe mbArgs $ extractArgsPayload "\"arguments\"" raw
                    Nothing -> mbArgs

      maxRet = case scanJsonIntField "\"retry\"" raw of
                 Just n | n >= 0 -> fromIntegral n
                 _ ->
                   let (_, afterRetry) = BS.breakSubstring "\"retry\"" raw
                       valS = skipSpaces (dropPastColon (BS.drop 7 afterRetry))
                   in if "false" `BS.isPrefixOf` valS then 0 else 25
      retCnt = case scanJsonIntField "\"retry_count\"" raw of
                 Just n | n >= 0 -> fromIntegral n
                 _               -> 0
      createdNs = case scanJsonIntField "\"created_at\"" raw of
                    Just n | n > 1_000_000_000_000 -> n
                    Just n | n > 0                 -> n * 1_000_000_000
                    _                              -> 0
  Just FastSidekiqScan
    { fssClass      = cls
    , fssJid        = jid
    , fssQueue      = q
    , fssArgsSlice  = argsSlice
    , fssMaxRetries = maxRet
    , fssRetryCount = retCnt
    , fssCreatedNs  = createdNs
    }
{-# INLINE scanSidekiqEnvelopeFast #-}

--------------------------------------------------------------------------------
-- 2. Two-Level Striped Lock-Free JSON Parameter & Template Cache
--
-- Level 1 (Exact Cache):   Caches `payloadSlice -> a` when 100% of JSON params match.
-- Level 2 (Template Cache): When jobs are reposted with a varying leading or trailing field
--                           (e.g. `{"invoice_id":1001,"amount_cents":12500,"currency":"EUR"}`)
--                           while the remaining parameter fields are unchanged, caches
--                           the pre-parsed `(Key, KeyMap Value)` template keyed by the unchanged
--                           substring and only parses the single varying delta field!
--------------------------------------------------------------------------------

numCacheStripes :: Int
numCacheStripes = 16
{-# INLINE numCacheStripes #-}

stripeMask :: Int
stripeMask = numCacheStripes - 1
{-# INLINE stripeMask #-}

data StripeBucket v = StripeBucket !Int !(HashMap ByteString v)

data TemplateEntry = TemplateEntry !Key.Key !(KeyMap Value)

emptyStripeBucket :: StripeBucket v
emptyStripeBucket = StripeBucket 0 HM.empty
{-# INLINE emptyStripeBucket #-}

insertStripeBucket :: Int -> ByteString -> v -> StripeBucket v -> (StripeBucket v, ())
insertStripeBucket !maxSz !k !v (StripeBucket !cnt !m)
  | cnt >= maxSz = (StripeBucket 1 (HM.singleton k v), ())
  | otherwise    = (StripeBucket (cnt + 1) (HM.insert k v m), ())
{-# INLINE insertStripeBucket #-}

data JsonParamCache a = JsonParamCache
  { jpcExactStripes    :: !(Vector (IORef (StripeBucket a)))
  , jpcTemplateStripes :: !(Vector (IORef (StripeBucket TemplateEntry)))
  , jpcMaxPerStripe    :: !Int
  , jpcExactHits       :: !(IORef Word64)
  , jpcTemplateHits    :: !(IORef Word64)
  , jpcMisses          :: !(IORef Word64)
  }

data JsonCacheStats = JsonCacheStats
  { jcsExactHits    :: !Word64
  , jcsTemplateHits :: !Word64
  , jcsMisses       :: !Word64
  } deriving (Show, Eq)

newJsonParamCache :: Int -> IO (JsonParamCache a)
newJsonParamCache maxTotalEntries = do
  let perStripe = max 16 (maxTotalEntries `div` numCacheStripes)
  exactStripes <- V.replicateM numCacheStripes (newIORef emptyStripeBucket)
  tmplStripes  <- V.replicateM numCacheStripes (newIORef emptyStripeBucket)
  eHits        <- newIORef 0
  tHits        <- newIORef 0
  misses       <- newIORef 0
  pure JsonParamCache
    { jpcExactStripes    = exactStripes
    , jpcTemplateStripes = tmplStripes
    , jpcMaxPerStripe    = perStripe
    , jpcExactHits       = eHits
    , jpcTemplateHits    = tHits
    , jpcMisses          = misses
    }

-- | Decode `payloadSlice` using the Two-Level JSON Parameter & Template Cache:
-- 1. Checks Level-1 Exact Match Cache (`payloadSlice -> a`).
-- 2. Checks Level-2 Unchanged-Parameters Template Cache (matching either an unchanged
--    prefix before the last comma OR an unchanged suffix after the first comma),
--    splicing in the single changed field without re-tokenizing the JSON string.
-- 3. On a miss, parses via Aeson and populates both Level-1 and Level-2 caches.
decodeWithParamCache
  :: JobPayload a
  => JsonParamCache a
  -> ByteString
  -> IO (Either String a)
decodeWithParamCache cache payloadSlice = do
  -- Level 1: Exact payloadSlice lookup
  let !h        = hash payloadSlice
      !exactIdx = h .&. stripeMask
      !exactRef = V.unsafeIndex (jpcExactStripes cache) exactIdx
  StripeBucket _ exactMap <- readIORef exactRef
  case HM.lookup payloadSlice exactMap of
    Just val -> do
      atomicModifyIORef' (jpcExactHits cache) (\n -> (n + 1, ()))
      pure (Right val)
    Nothing -> do
      let mPrefixDelta = splitLastJsonField payloadSlice
      mHit <- tryTemplateLookup mPrefixDelta
      case mHit of
        Just val -> pure (Right val)
        Nothing -> do
          let mSuffixDelta = splitFirstJsonField payloadSlice
          mHit2 <- tryTemplateLookup mSuffixDelta
          case mHit2 of
            Just val -> pure (Right val)
            Nothing  -> fullDecodeAndCache exactRef mPrefixDelta mSuffixDelta
  where
    tryTemplateLookup Nothing = pure Nothing
    tryTemplateLookup (Just (_, deltaVal, tmplSlice)) = do
      let !tmplIdx = hash tmplSlice .&. stripeMask
          !tmplRef = V.unsafeIndex (jpcTemplateStripes cache) tmplIdx
      StripeBucket _ tmplMap <- readIORef tmplRef
      case HM.lookup tmplSlice tmplMap of
        Just (TemplateEntry cachedKey baseKm) -> do
          let !mergedKm = KM.insert cachedKey deltaVal baseKm
          case fromJSON (Object mergedKm) of
            Success val -> do
              atomicModifyIORef' (jpcTemplateHits cache) (\n -> (n + 1, ()))
              pure (Just val)
            Error _ -> pure Nothing
        Nothing -> pure Nothing

    fullDecodeAndCache exactRef mPrefixDelta mSuffixDelta = do
      atomicModifyIORef' (jpcMisses cache) (\n -> (n + 1, ()))
      let !maxSz = jpcMaxPerStripe cache
      case Aeson.decodeStrict' payloadSlice of
        Just (Object km) -> do
          insertTemplateSlice maxSz km mPrefixDelta
          insertTemplateSlice maxSz km mSuffixDelta
          case fromJSON (Object km) of
            Success val -> do
              let !ownedKey = BS.copy payloadSlice
              atomicModifyIORef' exactRef (insertStripeBucket maxSz ownedKey val)
              pure (Right val)
            Error err -> pure (Left err)
        _ ->
          case decodePayload payloadSlice of
            Left err  -> pure (Left err)
            Right val -> do
              let !ownedKey = BS.copy payloadSlice
              atomicModifyIORef' exactRef (insertStripeBucket maxSz ownedKey val)
              pure (Right val)

    insertTemplateSlice _     _  Nothing = pure ()
    insertTemplateSlice maxSz km (Just (deltaKey, _, tmplSlice)) = do
      let !ownedTmpl = BS.copy tmplSlice
          !cachedKey = Key.fromText (TE.decodeUtf8Lenient deltaKey)
          !entry     = TemplateEntry cachedKey km
          !tmplIdx   = hash ownedTmpl .&. stripeMask
          !tmplRef   = V.unsafeIndex (jpcTemplateStripes cache) tmplIdx
      atomicModifyIORef' tmplRef (insertStripeBucket maxSz ownedTmpl entry)
{-# INLINABLE decodeWithParamCache #-}

-- | Split a JSON object `{"k1":v1,"k2":v2,"kLast":<primitive_val>}` at the LAST comma
-- into `(kLastSlice, parsedLastVal, unchangedPrefixSlice)`.
splitLastJsonField :: ByteString -> Maybe (ByteString, Value, ByteString)
splitLastJsonField raw = do
  lastCommaIdx <- findLastTopLevelComma raw
  let prefixSlice = BS.take lastCommaIdx raw
      afterComma  = skipSpaces (BS.drop (lastCommaIdx + 1) raw)
  (kSlice, val, afterVal) <- parseKeyValuePair afterComma
  (closeByte, _) <- BS.uncons (skipSpaces afterVal)
  guard (closeByte == wCloseBrace)
  pure (kSlice, val, prefixSlice)
{-# INLINE splitLastJsonField #-}

-- | Scan backwards from the end of a flat/nested JSON object to locate the last top-level comma
-- in O(tail_field_len) (~15 bytes) rather than scanning from the start of the ByteString.
findLastTopLevelComma :: ByteString -> Maybe Int
findLastTopLevelComma bs
  | len < 2   = Nothing
  | otherwise = go (len - 1) (0 :: Int) False
  where
    len = BS.length bs
    go !i !depth !inStr
      | i <= 0    = Nothing
      | inStr     = stepInString i depth (BS.index bs i)
      | otherwise = stepOutsideString i depth (BS.index bs i)

    stepInString !i !depth !b
      | b == wDoubleQuote && not (isEscapedAt i) = go (i - 1) depth False
      | otherwise                                = go (i - 1) depth True

    stepOutsideString !i !depth !b
      | b == wDoubleQuote                      = go (i - 1) depth True
      | b == wCloseBrace || b == wCloseBracket = go (i - 1) (depth + 1) False
      | b == wOpenBrace  || b == wOpenBracket  = go (i - 1) (depth - 1) False
      | b == wComma && depth == 1              = Just i
      | otherwise                              = go (i - 1) depth False

    isEscapedAt !i = i > 0 && BS.index bs (i - 1) == wBackslash
{-# INLINE findLastTopLevelComma #-}

-- | Fast O(1) lookup for execution workers when the Dedicated JSON Processing Worker
-- has already pre-decoded and cached `payloadSlice` in `jpcExactStripes`.
-- Falls back to `decodeWithParamCache` if the frame bypassed the JSON ingestor pool.
lookupPrewarmedOrDecode
  :: JobPayload a
  => JsonParamCache a
  -> ByteString
  -> IO (Either String a)
lookupPrewarmedOrDecode cache payloadSlice = do
  let !exactIdx = hash payloadSlice .&. stripeMask
      !exactRef = V.unsafeIndex (jpcExactStripes cache) exactIdx
  StripeBucket _ exactMap <- readIORef exactRef
  case HM.lookup payloadSlice exactMap of
    Just val -> pure (Right val)
    Nothing  -> decodeWithParamCache cache payloadSlice
{-# INLINABLE lookupPrewarmedOrDecode #-}

-- | Split a JSON object `{"key1":<primitive_val>,"key2":...}` at the first comma
-- into `(key1Slice, parsedValue1, unchangedTailSlice)` with zero heap DOM allocation.
splitFirstJsonField :: ByteString -> Maybe (ByteString, Value, ByteString)
splitFirstJsonField raw = do
  (openByte, afterOpen) <- BS.uncons (skipSpaces raw)
  guard (openByte == wOpenBrace)
  (kSlice, val, afterVal) <- parseKeyValuePair (skipSpaces afterOpen)
  let sComma = skipSpaces afterVal
  (commaByte, _) <- BS.uncons sComma
  guard (commaByte == wComma)
  pure (kSlice, val, sComma)
{-# INLINE splitFirstJsonField #-}

-- | Parse a `"key":<primitive_value>` pair starting at the opening quote of the key.
parseKeyValuePair :: ByteString -> Maybe (ByteString, Value, ByteString)
parseKeyValuePair bs = do
  (quoteByte, afterQuote) <- BS.uncons bs
  guard (quoteByte == wDoubleQuote)
  kSlice <- extractQuotedSlice afterQuote
  let afterKey = skipSpaces (dropPastColon (BS.drop (BS.length kSlice + 1) afterQuote))
  (val, afterVal) <- parseSimpleHeadValue afterKey
  pure (kSlice, val, afterVal)
{-# INLINE parseKeyValuePair #-}

parseSimpleHeadValue :: ByteString -> Maybe (Value, ByteString)
parseSimpleHeadValue bs = do
  (firstByte, rest0) <- BS.uncons bs
  case firstByte of
    b | b == wDoubleQuote -> do
          strSlice <- extractQuotedSlice rest0
          let rest = BS.drop (BS.length strSlice + 1) rest0
          pure (String (TE.decodeUtf8Lenient strSlice), rest)
      | "true"  `BS.isPrefixOf` bs -> Just (Bool True, BS.drop 4 bs)
      | "false" `BS.isPrefixOf` bs -> Just (Bool False, BS.drop 5 bs)
      | otherwise                  -> parseSimpleNumber firstByte rest0 bs
{-# INLINE parseSimpleHeadValue #-}

parseSimpleNumber :: Word8 -> ByteString -> ByteString -> Maybe (Value, ByteString)
parseSimpleNumber firstByte tailBs fullBs = do
  let (neg, r0) = if firstByte == 0x2D then (True, tailBs) else (False, fullBs)
      digits    = BS.takeWhile isDigitByte r0
  guard (not (BS.null digits))
  let restAfterInt = BS.drop (BS.length digits) r0
  case BS.uncons restAfterInt of
    Just (dot, afterDot) | dot == 0x2E ->
      pure (parseFractional neg digits afterDot)
    _ ->
      let !n64 = BS.foldl' (\acc w -> acc * 10 + fromIntegral (w - 0x30)) (0 :: Int64) digits
          !s64 = if neg then negate n64 else n64
       in Just (Number (fromIntegral s64), restAfterInt)
{-# INLINE parseSimpleNumber #-}

parseFractional :: Bool -> ByteString -> ByteString -> (Value, ByteString)
parseFractional neg digits afterDot =
  let fracDigits = BS.takeWhile isDigitByte afterDot
      intPart    = BS.foldl' (\acc w -> acc * 10 + fromIntegral (w - 0x30)) (0 :: Double) digits
      fracPart   = BS.foldl' (\acc w -> acc * 10 + fromIntegral (w - 0x30)) (0 :: Double) fracDigits
      scale      = 10.0 ** fromIntegral (BS.length fracDigits)
      num        = intPart + (fracPart / scale)
      signed     = if neg then negate num else num
      rest       = BS.drop (BS.length fracDigits) afterDot
   in (Number (realToFrac signed), rest)
{-# INLINE parseFractional #-}

getJsonCacheStats :: JsonParamCache a -> IO JsonCacheStats
getJsonCacheStats cache =
  JsonCacheStats
    <$> readIORef (jpcExactHits cache)
    <*> readIORef (jpcTemplateHits cache)
    <*> readIORef (jpcMisses cache)
