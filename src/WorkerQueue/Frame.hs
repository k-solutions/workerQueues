{-# LANGUAGE GHC2024           #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

-- | 64-Byte Zero-Copy Frame & Typed `JobPayload` Codec.
-- Intended to be imported qualified:
--
-- @
-- import WorkerQueue.Frame qualified as Frame
-- @
module WorkerQueue.Frame
  ( -- * Priority & Source
    Priority (..)
  , Source (..)
  , toQueueName
  , parsePriority

    -- * 64-Byte Zero-Copy Frame Header
  , headerSize
  , magic
  , version
  , Params (..)
  , Header (..)
  , encode
  , decodeHeader
  , bumpRetry

    -- * Typed JobPayload Codec
  , JobPayload (..)
  , encodeJob
  ) where

import           Data.Aeson               (FromJSON, ToJSON)
import qualified Data.Aeson               as Aeson
import           Data.Bits                (shiftL, shiftR, (.&.), (.|.))
import           Data.ByteString          (ByteString)
import qualified Data.ByteString          as BS
import           Data.ByteString.Builder  (Builder)
import qualified Data.ByteString.Builder  as B
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Lazy     as BSL
import           Data.Int                 (Int64)
import           Data.Proxy               (Proxy (..))
import           Data.Word                (Word16, Word32, Word64, Word8)
import           Foreign.ForeignPtr       (plusForeignPtr, withForeignPtr)
import           Foreign.Marshal.Utils    (copyBytes, fillBytes)
import           Foreign.Ptr              (Ptr, plusPtr)
import           Foreign.Storable         (peekByteOff, pokeByteOff)
import           System.IO.Unsafe         (unsafeDupablePerformIO)

-- | Queue priority levels matching Sidekiq conventions.
data Priority
  = Critical
  | Default
  | Low
  deriving (Show, Eq, Ord, Enum, Bounded)

-- | Origin of a framed job.
data Source
  = SourceRails
  | SourceCron
  | SourceHaskell
  deriving (Show, Eq, Ord, Enum, Bounded)

toQueueName :: Priority -> ByteString
toQueueName Critical = "critical"
toQueueName Default  = "default"
toQueueName Low      = "low"
{-# INLINE toQueueName #-}

parsePriority :: ByteString -> Priority
parsePriority q
  | q == "critical" || q == "queue:critical" = Critical
  | q == "low"      || q == "queue:low"      = Low
  | otherwise                                = Default
{-# INLINE parsePriority #-}

encodePrioritySource :: Priority -> Source -> Word8
encodePrioritySource prio src =
  let pBits = fromIntegral (fromEnum prio) .&. 0x03
      sBits = (fromIntegral (fromEnum src) .&. 0x03) `shiftL` 2
   in pBits .|. sBits
{-# INLINE encodePrioritySource #-}

decodePrioritySource :: Word8 -> (Priority, Source)
decodePrioritySource w =
  let p = case w .&. 0x03 of
            0 -> Critical
            1 -> Default
            _ -> Low
      s = case (w `shiftR` 2) .&. 0x03 of
            0 -> SourceRails
            1 -> SourceCron
            _ -> SourceHaskell
   in (p, s)
{-# INLINE decodePrioritySource #-}

-- | Fixed 64-byte (single CPU L1 cache line) binary header size.
headerSize :: Int
headerSize = 64
{-# INLINE headerSize #-}

-- | Magic bytes: 0x5751 ("WQ" in ASCII).
magic :: Word16
magic = 0x5751
{-# INLINE magic #-}

-- | Wire format version 1.
version :: Word8
version = 1
{-# INLINE version #-}

-- | Parameters for encoding a 64-byte job frame and its contiguous slices.
data Params = Params
  { fpPriority     :: !Priority
  , fpSource       :: !Source
  , fpRetryCount   :: !Word16
  , fpMaxRetries   :: !Word16
  , fpJobId        :: !Word64
  , fpSidekiqJid   :: !ByteString -- ^ Sidekiq JID (normalized to 24 bytes)
  , fpCreatedAtNs  :: !Int64
  , fpQueueSlice   :: !ByteString
  , fpTagSlice     :: !ByteString -- ^ Sidekiq `class` name
  , fpPayloadSlice :: !ByteString -- ^ Raw JSON `args`
  } deriving (Show, Eq)

-- | Zero-copy decoded view of a 64-byte job frame and its contiguous slices.
data Header = Header
  { hdrMagic        :: !Word16
  , hdrVersion      :: !Word8
  , hdrPriority     :: !Priority
  , hdrSource       :: !Source
  , hdrRetryCount   :: !Word16
  , hdrMaxRetries   :: !Word16
  , hdrJobId        :: !Word64
  , hdrSidekiqJid   :: !ByteString -- ^ 24-byte slice at offset 16..39
  , hdrCreatedAtNs  :: !Int64
  , hdrQueueLen     :: !Word32
  , hdrTagLen       :: !Word32
  , hdrPayloadLen   :: !Word64
  , hdrQueueSlice   :: !ByteString -- ^ Zero-copy slice at offset 64
  , hdrTagSlice     :: !ByteString -- ^ Zero-copy slice for Sidekiq `class`
  , hdrPayloadSlice :: !ByteString -- ^ Zero-copy slice for raw JSON `args`
  } deriving (Show, Eq)

-- | Normalize a Sidekiq JID into exactly 24 ASCII bytes (padded with '0' if shorter).
normalizeJid24 :: ByteString -> ByteString
normalizeJid24 jid
  | BS.length jid == 24 = jid
  | BS.length jid > 24  = BS.take 24 jid
  | otherwise           = jid <> BS.replicate (24 - BS.length jid) 0x30
{-# INLINE normalizeJid24 #-}

-- | Construct a contiguous pinned ByteString containing the 64-byte header
-- followed immediately by `fpQueueSlice`, `fpTagSlice`, and `fpPayloadSlice`.
encode :: Params -> ByteString
encode Params {..} =
  let jid24    = normalizeJid24 fpSidekiqJid
      !qLen    = BS.length fpQueueSlice
      !tLen    = BS.length fpTagSlice
      !pLen    = BS.length fpPayloadSlice
      !totalSz = headerSize + qLen + tLen + pLen
   in BSI.unsafeCreate totalSz $ \ptr -> do
        fillBytes ptr 0 headerSize
        pokeByteOff ptr 0  magic
        pokeByteOff ptr 2  version
        pokeByteOff ptr 3  (encodePrioritySource fpPriority fpSource)
        pokeByteOff ptr 4  fpRetryCount
        pokeByteOff ptr 6  fpMaxRetries
        pokeByteOff ptr 8  fpJobId
        copyBsToPtr jid24  (ptr `plusPtr` 16)
        pokeByteOff ptr 40 fpCreatedAtNs
        pokeByteOff ptr 48 (fromIntegral qLen :: Word32)
        pokeByteOff ptr 52 (fromIntegral tLen :: Word32)
        pokeByteOff ptr 56 (fromIntegral pLen :: Word64)
        let qPtr = ptr `plusPtr` headerSize
            tPtr = qPtr `plusPtr` qLen
            pPtr = tPtr `plusPtr` tLen
        copyBsToPtr fpQueueSlice   qPtr
        copyBsToPtr fpTagSlice     tPtr
        copyBsToPtr fpPayloadSlice pPtr
{-# INLINE encode #-}

copyBsToPtr :: ByteString -> Ptr Word8 -> IO ()
copyBsToPtr (BSI.BS fp len) dst
  | len <= 0  = pure ()
  | otherwise = withForeignPtr fp $ \src -> copyBytes dst src len
{-# INLINE copyBsToPtr #-}

-- | Decode the 64-byte binary header and extract zero-copy ByteString slices
-- (`hdrSidekiqJid`, `hdrQueueSlice`, `hdrTagSlice`, `hdrPayloadSlice`)
-- pointing directly into the same underlying ForeignPtr buffer via `plusForeignPtr`.
decodeHeader :: ByteString -> Either String Header
decodeHeader (BSI.BS fp totalLen)
  | totalLen < headerSize =
      Left $ "Frame too short: " <> show totalLen <> " bytes (expected >= 64)"
  | otherwise = unsafeDupablePerformIO $ withForeignPtr fp $ \ptr -> do
      m <- peekByteOff ptr 0 :: IO Word16
      v <- peekByteOff ptr 2 :: IO Word8
      if m /= magic
        then pure $ Left ("Invalid frame magic: " <> show m)
        else if v /= version
          then pure $ Left ("Unsupported frame version: " <> show v)
          else do
            psByte    <- peekByteOff ptr 3  :: IO Word8
            retryCnt  <- peekByteOff ptr 4  :: IO Word16
            maxRet    <- peekByteOff ptr 6  :: IO Word16
            jId       <- peekByteOff ptr 8  :: IO Word64
            createdNs <- peekByteOff ptr 40 :: IO Int64
            qLenW32   <- peekByteOff ptr 48 :: IO Word32
            tLenW32   <- peekByteOff ptr 52 :: IO Word32
            pLenW64   <- peekByteOff ptr 56 :: IO Word64

            let (!prio, !src) = decodePrioritySource psByte
                !qLen         = fromIntegral qLenW32 :: Int
                !tLen         = fromIntegral tLenW32 :: Int
                !pLen         = fromIntegral pLenW64 :: Int
                !tOff         = headerSize + qLen
                !pOff         = tOff + tLen
                !expectedLen  = pOff + pLen

            if totalLen < expectedLen
              then pure $ Left $
                "Truncated frame slices: have " <> show totalLen <> ", need " <> show expectedLen
              else do
                let !jidSlice = BSI.BS (plusForeignPtr fp 16) 24
                    !qSlice   = BSI.BS (plusForeignPtr fp headerSize) qLen
                    !tSlice   = BSI.BS (plusForeignPtr fp tOff) tLen
                    !pSlice   = BSI.BS (plusForeignPtr fp pOff) pLen
                pure $ Right Header
                  { hdrMagic        = m
                  , hdrVersion      = v
                  , hdrPriority     = prio
                  , hdrSource       = src
                  , hdrRetryCount   = retryCnt
                  , hdrMaxRetries   = maxRet
                  , hdrJobId        = jId
                  , hdrSidekiqJid   = jidSlice
                  , hdrCreatedAtNs  = createdNs
                  , hdrQueueLen     = qLenW32
                  , hdrTagLen       = tLenW32
                  , hdrPayloadLen   = pLenW64
                  , hdrQueueSlice   = qSlice
                  , hdrTagSlice     = tSlice
                  , hdrPayloadSlice = pSlice
                  }
{-# INLINE decodeHeader #-}

-- | Increment `retryCount` at byte offset 4..5 in the 64-byte header.
-- Returns `Right (newRetryCount, maxRetries, updatedFrame)` or `Left err`.
bumpRetry :: ByteString -> Either String (Word16, Word16, ByteString)
bumpRetry (BSI.BS fp totalLen)
  | totalLen < headerSize = Left "Frame too short to bump retryCount"
  | otherwise = unsafeDupablePerformIO $ withForeignPtr fp $ \srcPtr -> do
      curRetry <- peekByteOff srcPtr 4 :: IO Word16
      maxRet   <- peekByteOff srcPtr 6 :: IO Word16
      let !nextRetry = curRetry + 1
      updated <- BSI.create totalLen $ \dstPtr -> do
        copyBytes dstPtr srcPtr totalLen
        pokeByteOff dstPtr 4 nextRetry
      pure $ Right (nextRetry, maxRet, updated)
{-# INLINE bumpRetry #-}

-- | Core typeclass implemented by typed Haskell worker payloads.
class (FromJSON a, ToJSON a) => JobPayload a where
  -- | Static Sidekiq `class` name matched by the Redis Lua filter and `Dispatcher`.
  jobClassTag :: Proxy a -> ByteString

  -- | Serialize typed args directly into a JSON Builder.
  encodePayload :: a -> Builder
  encodePayload = Aeson.fromEncoding . Aeson.toEncoding
  {-# INLINE encodePayload #-}

  -- | Decode directly from the zero-copy pinned ByteString JSON args slice.
  decodePayload :: ByteString -> Either String a
  decodePayload = Aeson.eitherDecodeStrict'
  {-# INLINE decodePayload #-}

-- | Convenience helper to encode a typed Haskell `JobPayload` directly into a 64-byte frame.
encodeJob
  :: forall a. JobPayload a
  => Priority
  -> Source
  -> Word16     -- ^ maxRetries
  -> Word64     -- ^ jobId
  -> ByteString -- ^ sidekiqJid (24 bytes)
  -> Int64      -- ^ createdAtNs
  -> a
  -> ByteString
encodeJob prio src maxRet jId jid createdNs payload =
  encode Params
    { fpPriority     = prio
    , fpSource       = src
    , fpRetryCount   = 0
    , fpMaxRetries   = maxRet
    , fpJobId        = jId
    , fpSidekiqJid   = jid
    , fpCreatedAtNs  = createdNs
    , fpQueueSlice   = toQueueName prio
    , fpTagSlice     = jobClassTag (Proxy @a)
    , fpPayloadSlice = BSL.toStrict (B.toLazyByteString (encodePayload payload))
    }
{-# INLINE encodeJob #-}
