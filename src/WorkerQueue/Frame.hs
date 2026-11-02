{-# LANGUAGE GHC2024 #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NumericUnderscores #-}

module WorkerQueue.Frame
  ( -- * Priority & Frame Source
    Priority (..)
  , FrameSource (..)
  , priorityToQueueName
  , parseQueuePriority

    -- * 64-Byte Zero-Copy Frame Header
  , frameHeaderSize
  , frameMagic
  , frameVersion
  , JobHeader (..)
  , encodeFrame
  , decodeHeaderZeroCopy
  , bumpFrameRetry

    -- * Typed JobPayload Codec
  , JobPayload (..)
  , encodeJobToFrame
  ) where

import Data.Aeson (FromJSON, ToJSON)
import Data.Aeson qualified as Aeson
import Data.Bits (shiftL, shiftR, (.&.), (.|.))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Builder (Builder)
import Data.ByteString.Builder qualified as B
import Data.ByteString.Internal qualified as BSI
import Data.ByteString.Lazy qualified as BSL
import Data.Int (Int64)
import Data.Proxy (Proxy (..))
import Data.Word (Word8, Word16, Word32, Word64)
import Foreign.ForeignPtr (plusForeignPtr, withForeignPtr)
import Foreign.Marshal.Utils (copyBytes, fillBytes)
import Foreign.Ptr (Ptr, plusPtr)
import Foreign.Storable (peekByteOff, pokeByteOff)
import System.IO.Unsafe (unsafeDupablePerformIO)

-- | Queue priority levels matching Sidekiq conventions.
data Priority
  = Critical
  | Default
  | Low
  deriving (Show, Eq, Ord, Enum, Bounded)

-- | Origin of a framed job.
data FrameSource
  = SourceRails
  | SourceCron
  | SourceHaskell
  deriving (Show, Eq, Ord, Enum, Bounded)

priorityToQueueName :: Priority -> ByteString
priorityToQueueName Critical = "critical"
priorityToQueueName Default  = "default"
priorityToQueueName Low      = "low"
{-# INLINE priorityToQueueName #-}

parseQueuePriority :: ByteString -> Priority
parseQueuePriority q
  | q == "critical" || q == "queue:critical" = Critical
  | q == "low"      || q == "queue:low"      = Low
  | otherwise                                = Default
{-# INLINE parseQueuePriority #-}

encodePrioritySource :: Priority -> FrameSource -> Word8
encodePrioritySource prio src =
  let pBits = fromIntegral (fromEnum prio) .&. 0x03
      sBits = (fromIntegral (fromEnum src) .&. 0x03) `shiftL` 2
   in pBits .|. sBits
{-# INLINE encodePrioritySource #-}

decodePrioritySource :: Word8 -> (Priority, FrameSource)
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
frameHeaderSize :: Int
frameHeaderSize = 64
{-# INLINE frameHeaderSize #-}

-- | Magic bytes: 0x5751 ("WQ" in ASCII).
frameMagic :: Word16
frameMagic = 0x5751
{-# INLINE frameMagic #-}

-- | Wire format version 1.
frameVersion :: Word8
frameVersion = 1
{-# INLINE frameVersion #-}

-- | Zero-copy decoded view of a 64-byte job frame and its contiguous slices.
data JobHeader = JobHeader
  { hdrMagic        :: !Word16
  , hdrVersion      :: !Word8
  , hdrPriority     :: !Priority
  , hdrSource       :: !FrameSource
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
-- followed immediately by `queueSlice`, `tagSlice`, and `payloadSlice`.
encodeFrame
  :: Priority
  -> FrameSource
  -> Word16     -- ^ retryCount
  -> Word16     -- ^ maxRetries
  -> Word64     -- ^ jobId
  -> ByteString -- ^ sidekiqJid (up to 24 bytes)
  -> Int64      -- ^ createdAtNs
  -> ByteString -- ^ queueSlice
  -> ByteString -- ^ tagSlice (Sidekiq class name)
  -> ByteString -- ^ payloadSlice (raw JSON args)
  -> ByteString
encodeFrame prio src retryCnt maxRet jId rawJid createdNs qSlice tSlice pSlice =
  let jid24   = normalizeJid24 rawJid
      !qLen   = BS.length qSlice
      !tLen   = BS.length tSlice
      !pLen   = BS.length pSlice
      !totalSz = frameHeaderSize + qLen + tLen + pLen
   in BSI.unsafeCreate totalSz $ \ptr -> do
        fillBytes ptr 0 frameHeaderSize
        pokeByteOff ptr 0  frameMagic
        pokeByteOff ptr 2  frameVersion
        pokeByteOff ptr 3  (encodePrioritySource prio src)
        pokeByteOff ptr 4  retryCnt
        pokeByteOff ptr 6  maxRet
        pokeByteOff ptr 8  jId
        copyBsToPtr jid24  (ptr `plusPtr` 16)
        pokeByteOff ptr 40 createdNs
        pokeByteOff ptr 48 (fromIntegral qLen :: Word32)
        pokeByteOff ptr 52 (fromIntegral tLen :: Word32)
        pokeByteOff ptr 56 (fromIntegral pLen :: Word64)
        let qPtr = ptr `plusPtr` frameHeaderSize
            tPtr = qPtr `plusPtr` qLen
            pPtr = tPtr `plusPtr` tLen
        copyBsToPtr qSlice qPtr
        copyBsToPtr tSlice tPtr
        copyBsToPtr pSlice pPtr
{-# INLINE encodeFrame #-}

copyBsToPtr :: ByteString -> Ptr Word8 -> IO ()
copyBsToPtr (BSI.BS fp len) dst
  | len <= 0  = pure ()
  | otherwise = withForeignPtr fp $ \src -> copyBytes dst src len
{-# INLINE copyBsToPtr #-}

-- | Decode the 64-byte binary header and extract zero-copy ByteString slices
-- (`hdrSidekiqJid`, `hdrQueueSlice`, `hdrTagSlice`, `hdrPayloadSlice`)
-- pointing directly into the same underlying ForeignPtr buffer via `plusForeignPtr`.
decodeHeaderZeroCopy :: ByteString -> Either String JobHeader
decodeHeaderZeroCopy (BSI.BS fp totalLen)
  | totalLen < frameHeaderSize =
      Left $ "Frame too short: " <> show totalLen <> " bytes (expected >= 64)"
  | otherwise = unsafeDupablePerformIO $ withForeignPtr fp $ \ptr -> do
      magic <- peekByteOff ptr 0 :: IO Word16
      ver   <- peekByteOff ptr 2 :: IO Word8
      if magic /= frameMagic
        then pure $ Left ("Invalid frame magic: " <> show magic)
        else if ver /= frameVersion
          then pure $ Left ("Unsupported frame version: " <> show ver)
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
                !tOff         = frameHeaderSize + qLen
                !pOff         = tOff + tLen
                !expectedLen  = pOff + pLen

            if totalLen < expectedLen
              then pure $ Left $
                "Truncated frame slices: have " <> show totalLen <> ", need " <> show expectedLen
              else do
                let !jidSlice = BSI.BS (plusForeignPtr fp 16) 24
                    !qSlice   = BSI.BS (plusForeignPtr fp frameHeaderSize) qLen
                    !tSlice   = BSI.BS (plusForeignPtr fp tOff) tLen
                    !pSlice   = BSI.BS (plusForeignPtr fp pOff) pLen
                pure $ Right JobHeader
                  { hdrMagic        = magic
                  , hdrVersion      = ver
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
{-# INLINE decodeHeaderZeroCopy #-}

-- | Increment `retryCount` at byte offset 4..5 in the 64-byte header.
-- Returns `Right (newRetryCount, maxRetries, updatedFrame)` or `Left err`.
bumpFrameRetry :: ByteString -> Either String (Word16, Word16, ByteString)
bumpFrameRetry (BSI.BS fp totalLen)
  | totalLen < frameHeaderSize = Left "Frame too short to bump retryCount"
  | otherwise = unsafeDupablePerformIO $ withForeignPtr fp $ \srcPtr -> do
      curRetry <- peekByteOff srcPtr 4 :: IO Word16
      maxRet   <- peekByteOff srcPtr 6 :: IO Word16
      let !nextRetry = curRetry + 1
      updated <- BSI.create totalLen $ \dstPtr -> do
        copyBytes dstPtr srcPtr totalLen
        pokeByteOff dstPtr 4 nextRetry
      pure $ Right (nextRetry, maxRet, updated)
{-# INLINE bumpFrameRetry #-}

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
encodeJobToFrame
  :: forall a. JobPayload a
  => Priority
  -> FrameSource
  -> Word16     -- ^ maxRetries
  -> Word64     -- ^ jobId
  -> ByteString -- ^ sidekiqJid (24 bytes)
  -> Int64      -- ^ createdAtNs
  -> a
  -> ByteString
encodeJobToFrame prio src maxRet jId jid createdNs payload =
  let qSlice = priorityToQueueName prio
      tSlice = jobClassTag (Proxy @a)
      pSlice = BSL.toStrict (B.toLazyByteString (encodePayload payload))
   in encodeFrame prio src 0 maxRet jId jid createdNs qSlice tSlice pSlice
{-# INLINE encodeJobToFrame #-}
