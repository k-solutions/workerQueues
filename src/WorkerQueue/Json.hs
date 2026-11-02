{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}

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

import Data.Aeson (Result (..), Value (..), fromJSON)
import Data.Aeson qualified as Aeson
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap (KeyMap)
import Data.Aeson.KeyMap qualified as KM
import Data.Bits ((.&.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Internal qualified as BSI
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HM
import Data.Hashable (hash)
import Data.Int (Int64)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Text.Encoding qualified as TE
import Data.Vector (Vector)
import Data.Vector qualified as V
import Data.Word (Word8, Word16, Word64)
import WorkerQueue.Frame (JobPayload (..))

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

skipSpaces :: ByteString -> ByteString
skipSpaces = BS.dropWhile isWhitespace
{-# INLINE skipSpaces #-}

-- | Fast zero-copy extraction of a string field (e.g. `"class":"..."`)
-- from a JSON ByteString without allocating an Aeson DOM tree.
scanJsonStringField :: ByteString -> ByteString -> Maybe ByteString
scanJsonStringField needleKey raw =
  let (_, afterKey) = BS.breakSubstring needleKey raw
   in if BS.null afterKey
        then Nothing
        else
          let afterNeedle = BS.drop (BS.length needleKey) afterKey
              afterColon  = skipSpaces (dropPastColon afterNeedle)
           in if BS.null afterColon || BS.head afterColon /= wDoubleQuote
                then Nothing
                else extractQuotedSlice (BS.drop 1 afterColon)
{-# INLINE scanJsonStringField #-}

dropPastColon :: ByteString -> ByteString
dropPastColon bs =
  let s = skipSpaces bs
   in if not (BS.null s) && BS.head s == wColon
        then BS.drop 1 s
        else BS.empty
{-# INLINE dropPastColon #-}

-- | Return the zero-copy slice up to the closing unescaped `"`.
-- Uses C `memchr` (`BS.elemIndex`) on the unescaped fast path.
extractQuotedSlice :: ByteString -> Maybe ByteString
extractQuotedSlice bs =
  case BS.elemIndex wDoubleQuote bs of
    Nothing -> Nothing
    Just i
      | i == 0 || BS.index bs (i - 1) /= wBackslash ->
          Just (BS.take i bs)
      | otherwise ->
          go 0 False
  where
    len = BS.length bs
    go !i !escaped
      | i >= len = Nothing
      | escaped  = go (i + 1) False
      | otherwise =
          let b = BS.index bs i
           in if b == wBackslash
                then go (i + 1) True
                else if b == wDoubleQuote
                  then Just (BS.take i bs)
                  else go (i + 1) False
{-# INLINE extractQuotedSlice #-}

-- | Fast zero-copy extraction of an integer/numeric field (e.g. `"retry":25`, `"created_at":...`).
scanJsonIntField :: ByteString -> ByteString -> Maybe Int64
scanJsonIntField needleKey raw =
  let (_, afterKey) = BS.breakSubstring needleKey raw
   in if BS.null afterKey
        then Nothing
        else
          let afterNeedle = BS.drop (BS.length needleKey) afterKey
              valStart    = skipSpaces (dropPastColon afterNeedle)
           in parseLeadingInt64 valStart
{-# INLINE scanJsonIntField #-}

parseLeadingInt64 :: ByteString -> Maybe Int64
parseLeadingInt64 bs
  | BS.null bs = Nothing
  | otherwise  =
      let (neg, rest) =
            if BS.head bs == 0x2D -- '-'
              then (True, BS.drop 1 bs)
              else (False, bs)
          digits = BS.takeWhile (\w -> w >= 0x30 && w <= 0x39) rest
       in if BS.null digits
            then Nothing
            else
              let !n = BS.foldl' (\acc w -> acc * 10 + fromIntegral (w - 0x30)) (0 :: Int64) digits
               in Just (if neg then negate n else n)
{-# INLINE parseLeadingInt64 #-}

-- | Extract the effective Sidekiq worker class (`"wrapped"` for ActiveJob, else `"class"`)
-- in a single zero-copy ByteString scan.
scanSidekiqClassFast :: ByteString -> Maybe ByteString
scanSidekiqClassFast raw =
  case scanJsonStringField "\"wrapped\"" raw of
    Just w  -> Just w
    Nothing -> scanJsonStringField "\"class\"" raw
{-# INLINE scanSidekiqClassFast #-}

-- | Extract the 24-byte Sidekiq `"jid"` slice in a single zero-copy ByteString scan.
scanSidekiqJidFast :: ByteString -> Maybe ByteString
scanSidekiqJidFast = scanJsonStringField "\"jid\""
{-# INLINE scanSidekiqJidFast #-}

-- | Extract a zero-copy ByteString slice of the inner JSON payload inside
-- ActiveJob `"arguments":[<payload>]` or Sidekiq `"args":[<payload>]` / `"args":<payload>`.
scanSidekiqArgsSlice :: ByteString -> Maybe ByteString
scanSidekiqArgsSlice raw =
  case extractArgsPayload "\"arguments\"" raw of
    Just slice -> Just slice
    Nothing    -> extractArgsPayload "\"args\"" raw
{-# INLINE scanSidekiqArgsSlice #-}

extractArgsPayload :: ByteString -> ByteString -> Maybe ByteString
extractArgsPayload key raw =
  let (_, afterKey) = BS.breakSubstring key raw
   in if BS.null afterKey
        then Nothing
        else
          let afterNeedle = BS.drop (BS.length key) afterKey
              valStart    = skipSpaces (dropPastColon afterNeedle)
           in if BS.null valStart
                then Nothing
                else
                  let firstByte = BS.head valStart
                   in if firstByte == wOpenBracket
                        then
                          -- Unwrap single-element array `[{...}]` if first element is an object
                          let inner = skipSpaces (BS.drop 1 valStart)
                           in if not (BS.null inner) && BS.head inner == wOpenBrace
                                then do
                                  objSlice <- takeBalancedSlice wOpenBrace wCloseBrace inner
                                  let afterObj = skipSpaces (BS.drop (BS.length objSlice) inner)
                                  if not (BS.null afterObj) && BS.head afterObj == wCloseBracket
                                    then Just objSlice
                                    else takeBalancedSlice wOpenBracket wCloseBracket valStart
                                else takeBalancedSlice wOpenBracket wCloseBracket valStart
                        else if firstByte == wOpenBrace
                          then takeBalancedSlice wOpenBrace wCloseBrace valStart
                          else Nothing
{-# INLINE extractArgsPayload #-}

-- | Return a zero-copy ByteString slice spanning from `openByte` to its balanced `closeByte`,
-- respecting quoted strings and escape sequences.
takeBalancedSlice :: Word8 -> Word8 -> ByteString -> Maybe ByteString
takeBalancedSlice !openByte !closeByte (BSI.BS fp len)
  | len <= 0 = Nothing
  | otherwise =
      let bs = BSI.BS fp len
       in go bs (0 :: Int) (0 :: Int) False False
  where
    go !bs !i !depth !inStr !escaped
      | i >= len = Nothing
      | escaped  = go bs (i + 1) depth inStr False
      | otherwise =
          let b = BS.index bs i
           in if inStr
                then
                  if b == wBackslash
                    then go bs (i + 1) depth True True
                    else if b == wDoubleQuote
                      then go bs (i + 1) depth False False
                      else go bs (i + 1) depth True False
                else
                  if b == wDoubleQuote
                    then go bs (i + 1) depth True False
                    else if b == openByte
                      then go bs (i + 1) (depth + 1) False False
                      else if b == closeByte
                        then
                          if depth <= 1
                            then Just (BS.take (i + 1) bs)
                            else go bs (i + 1) (depth - 1) False False
                        else go bs (i + 1) depth False False
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
  let jid = case scanSidekiqJidFast raw of
              Just j  -> j
              Nothing -> "000000000000000000000000"
      q   = case scanJsonStringField "\"queue\"" raw of
              Just qn -> qn
              Nothing -> "default"
      argsSlice = case mWrapped of
                    Just _ ->
                      case extractArgsPayload "\"arguments\"" raw of
                        Just s  -> s
                        Nothing ->
                          case extractArgsPayload "\"args\"" raw of
                            Just s  -> s
                            Nothing -> "[]"
                    Nothing ->
                      case extractArgsPayload "\"args\"" raw of
                        Just s  -> s
                        Nothing -> "[]"
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
  if BS.null afterComma || BS.head afterComma /= wDoubleQuote
    then Nothing
    else do
      kSlice <- extractQuotedSlice (BS.drop 1 afterComma)
      let afterK = skipSpaces (dropPastColon (BS.drop (1 + BS.length kSlice + 1) afterComma))
      (val, afterVal) <- parseSimpleHeadValue afterK
      let sClose = skipSpaces afterVal
      if not (BS.null sClose) && BS.head sClose == wCloseBrace
        then Just (kSlice, val, prefixSlice)
        else Nothing
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
      | i <= 0 = Nothing
      | otherwise =
          let b = BS.index bs i
           in if inStr
                then
                  if b == wDoubleQuote && (i == 0 || BS.index bs (i - 1) /= wBackslash)
                    then go (i - 1) depth False
                    else go (i - 1) depth True
                else
                  if b == wDoubleQuote
                    then go (i - 1) depth True
                    else if b == wCloseBrace || b == wCloseBracket
                      then go (i - 1) (depth + 1) False
                      else if b == wOpenBrace || b == wOpenBracket
                        then go (i - 1) (depth - 1) False
                        else if b == wComma && depth == 1
                          then Just i
                          else go (i - 1) depth False
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
splitFirstJsonField raw =
  let s0 = skipSpaces raw
   in if BS.null s0 || BS.head s0 /= wOpenBrace
        then Nothing
        else
          let s1 = skipSpaces (BS.drop 1 s0)
           in if BS.null s1 || BS.head s1 /= wDoubleQuote
                then Nothing
                else do
                  kSlice <- extractQuotedSlice (BS.drop 1 s1)
                  let afterK = skipSpaces (dropPastColon (BS.drop (1 + BS.length kSlice + 1) s1))
                  (val, afterVal) <- parseSimpleHeadValue afterK
                  let sComma = skipSpaces afterVal
                  if not (BS.null sComma) && BS.head sComma == wComma
                    then Just (kSlice, val, sComma)
                    else Nothing
{-# INLINE splitFirstJsonField #-}

parseSimpleHeadValue :: ByteString -> Maybe (Value, ByteString)
parseSimpleHeadValue bs
  | BS.null bs = Nothing
  | BS.head bs == wDoubleQuote = do
      strSlice <- extractQuotedSlice (BS.drop 1 bs)
      let rest = BS.drop (1 + BS.length strSlice + 1) bs
      Just (String (TE.decodeUtf8Lenient strSlice), rest)
  | "true" `BS.isPrefixOf` bs =
      Just (Bool True, BS.drop 4 bs)
  | "false" `BS.isPrefixOf` bs =
      Just (Bool False, BS.drop 5 bs)
  | otherwise = do
      let (neg, r0) = if BS.head bs == 0x2D then (True, BS.drop 1 bs) else (False, bs)
          digits    = BS.takeWhile (\w -> w >= 0x30 && w <= 0x39) r0
      if BS.null digits
        then Nothing
        else
          let restAfterInt = BS.drop (BS.length digits) r0
           in if not (BS.null restAfterInt) && BS.head restAfterInt == 0x2E -- '.'
                then
                  let fracDigits = BS.takeWhile (\w -> w >= 0x30 && w <= 0x39) (BS.drop 1 restAfterInt)
                      intPart    = BS.foldl' (\acc w -> acc * 10 + fromIntegral (w - 0x30)) (0 :: Double) digits
                      fracPart   = BS.foldl' (\acc w -> acc * 10 + fromIntegral (w - 0x30)) (0 :: Double) fracDigits
                      scale      = 10.0 ** fromIntegral (BS.length fracDigits)
                      num        = intPart + (fracPart / scale)
                      signed     = if neg then negate num else num
                      rest       = BS.drop (1 + BS.length fracDigits) restAfterInt
                   in Just (Number (realToFrac signed), rest)
                else
                  let !n64   = BS.foldl' (\acc w -> acc * 10 + fromIntegral (w - 0x30)) (0 :: Int64) digits
                      !s64   = if neg then negate n64 else n64
                   in Just (Number (fromIntegral s64), restAfterInt)
{-# INLINE parseSimpleHeadValue #-}

getJsonCacheStats :: JsonParamCache a -> IO JsonCacheStats
getJsonCacheStats cache =
  JsonCacheStats
    <$> readIORef (jpcExactHits cache)
    <*> readIORef (jpcTemplateHits cache)
    <*> readIORef (jpcMisses cache)
