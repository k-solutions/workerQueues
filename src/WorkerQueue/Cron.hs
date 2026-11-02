{-# LANGUAGE GHC2024           #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

-- | Built-in YAML Cron Scheduler (`config/schedule.yml`).
-- Intended to be imported qualified:
--
-- @
-- import WorkerQueue.Cron qualified as Cron
-- @
module WorkerQueue.Cron
  ( Field (..)
  , Expr (..)
  , JobSpec (..)
  , ScheduleFile (..)
  , Env (..)
  , CronM
  , runCronM
  , fromWorkerEnv
  , parseExpr
  , matchesTime
  , parseScheduleYaml
  , loadScheduleFile
  , validateCapabilities
  , executeTick
  ) where

import           Control.Monad               (filterM)
import           Control.Monad.IO.Class      (liftIO)
import           Control.Monad.Reader        (ReaderT (..), ask)
import           Data.Aeson                  (FromJSON (..), Value (..), object,
                                              withObject, (.!=), (.:), (.:?))
import qualified Data.Aeson                  as Aeson
import           Data.Aeson.Types            (Parser)
import           Data.ByteString             (ByteString)
import qualified Data.ByteString.Char8       as B8
import qualified Data.ByteString.Lazy        as BSL
import           Data.Foldable               (for_)
import           Data.HashSet                (HashSet)
import qualified Data.HashSet                as HS
import           Data.IORef                  (IORef, atomicModifyIORef')
import           Data.Map.Strict             (Map)
import qualified Data.Map.Strict             as Map
import           Data.Text                   (Text)
import qualified Data.Text                   as T
import qualified Data.Text.Encoding          as TE
import           Data.Time.Calendar          (toGregorian)
import           Data.Time.Calendar.WeekDate (toWeekDate)
import           Data.Time.Clock             (UTCTime (..))
import           Data.Time.Clock.POSIX       (utcTimeToPOSIXSeconds)
import           Data.Time.LocalTime         (TimeOfDay (..), timeToTimeOfDay)
import           Data.Word                   (Word16, Word64)
import qualified Data.Yaml                   as Yaml
import           GHC.Clock                   (getMonotonicTimeNSec)
import           Text.Read                   (readMaybe)
import qualified WorkerQueue.Frame           as Frame
import           WorkerQueue.Metrics         (Metrics, incCronTriggered,
                                              incEnqueued)
import           WorkerQueue.Monad           (WorkerEnv (..))
import           WorkerQueue.Queue           (MultiPriorityQueue, enqueueFrame)
import           WorkerQueue.Redis           (RedisSyncHandle, acquireCronLock,
                                              recordWorkingLeaseAsync, rshStore)
import           WorkerQueue.Sidekiq         (frameToSidekiqJson)


-- | Matcher for a single field in a 5-field cron expression.
data Field
  = Any
  | Step !Int
  | Range !Int !Int
  | Values ![Int]
  deriving (Show, Eq)

-- | Parsed 5-field cron expression: `minute hour day-of-month month day-of-week`.
data Expr = Expr
  { eMinute     :: !Field
  , eHour       :: !Field
  , eDayOfMonth :: !Field
  , eMonth      :: !Field
  , eDayOfWeek  :: !Field
  } deriving (Show, Eq)

-- | Declarative YAML specification for a recurring job in `config/schedule.yml`.
data JobSpec = JobSpec
  { jsName        :: !Text
  , jsRaw         :: !Text
  , jsExpr        :: !Expr
  , jsClass       :: !ByteString
  , jsQueue       :: !ByteString
  , jsMaxRetries  :: !Word16
  , jsArgsBytes   :: !ByteString
  , jsDescription :: !(Maybe Text)
  } deriving (Show, Eq)

newtype ScheduleFile = ScheduleFile
  { sfJobs :: Map Text JobSpec
  } deriving (Show, Eq)

-- | Runtime environment for the Cron scheduler (`ReaderT Env IO`).
data Env = Env
  { ceSpecs      :: ![JobSpec]
  , ceRedisSync  :: !RedisSyncHandle
  , ceQueue      :: !MultiPriorityQueue
  , ceMetrics    :: !Metrics
  , ceJobCounter :: !(IORef Word64)
  }

type CronM = ReaderT Env IO

runCronM :: Env -> CronM a -> IO a
runCronM env action = runReaderT action env
{-# INLINE runCronM #-}

-- | Build a Cron `Env` directly from validated `JobSpec`s and `WorkerEnv ctx`.
fromWorkerEnv :: [JobSpec] -> WorkerEnv ctx -> Env
fromWorkerEnv specs WorkerEnv {..} = Env
  { ceSpecs      = specs
  , ceRedisSync  = envRedisSync
  , ceQueue      = envQueue
  , ceMetrics    = envMetrics
  , ceJobCounter = envJobCounter
  }
{-# INLINE fromWorkerEnv #-}

instance FromJSON ScheduleFile where
  parseJSON = withObject "ScheduleFile" $ \root -> do
    rawMap <- root .: "jobs" :: Parser (Map Text Value)
    parsed <- Map.traverseWithKey parseJobSpec rawMap
    pure (ScheduleFile parsed)
    where
      parseJobSpec name = withObject "JobSpec" $ \o -> do
        cronTxt <- o .: "cron"
        expr    <- case parseExpr cronTxt of
                     Right e  -> pure e
                     Left err -> fail err
        clsTxt  <- o .: "class"
        qTxt    <- o .:? "queue" .!= "default"
        maxRet  <- o .:? "retry" .!= 25
        argsVal <- o .:? "args"  .!= object []
        desc    <- o .:? "description"
        pure JobSpec
          { jsName        = name
          , jsRaw         = cronTxt
          , jsExpr        = expr
          , jsClass       = TE.encodeUtf8 clsTxt
          , jsQueue       = TE.encodeUtf8 qTxt
          , jsMaxRetries  = maxRet
          , jsArgsBytes   = BSL.toStrict (Aeson.encode argsVal)
          , jsDescription = desc
          }

parseExpr :: Text -> Either String Expr
parseExpr raw =
  case T.words (T.strip raw) of
    [fMin, fHr, fDom, fMon, fDow] ->
      Expr
        <$> parseField "minute" fMin
        <*> parseField "hour" fHr
        <*> parseField "day-of-month" fDom
        <*> parseField "month" fMon
        <*> parseField "day-of-week" fDow
    other ->
      Left $ "Invalid cron expression (expected 5 fields, got " <> show (length other) <> "): " <> T.unpack raw

parseField :: String -> Text -> Either String Field
parseField label txt
  | txt == "*" = Right Any
  | Just stepTxt <- T.stripPrefix "*/" txt =
      case readMaybe (T.unpack stepTxt) of
        Just s | s > 0 -> Right (Step s)
        _              -> Left ("Invalid step in " <> label <> ": " <> T.unpack txt)
  | T.isInfixOf "," txt = do
      let parts = T.splitOn "," txt
      vals <- traverse (readIntField label) parts
      Right (Values vals)
  | T.isInfixOf "-" txt =
      case T.splitOn "-" txt of
        [loTxt, hiTxt] -> do
          lo <- readIntField label loTxt
          hi <- readIntField label hiTxt
          Right (Range lo hi)
        _ -> Left ("Invalid range in " <> label <> ": " <> T.unpack txt)
  | otherwise = do
      v <- readIntField label txt
      Right (Values [v])

readIntField :: String -> Text -> Either String Int
readIntField label t =
  case readMaybe (T.unpack (T.strip t)) of
    Just n  -> Right n
    Nothing -> Left ("Invalid integer in " <> label <> ": " <> T.unpack t)

matchesField :: Field -> Int -> Bool
matchesField Any _           = True
matchesField (Step s) v      = v `rem` s == 0
matchesField (Range lo hi) v = v >= lo && v <= hi
matchesField (Values vs) v   = v `elem` vs

-- | Check whether a `UTCTime` matches a 5-field `Expr`.
matchesTime :: Expr -> UTCTime -> Bool
matchesTime expr utc =
  let day                = utctDay utc
      (_, month, dom)    = toGregorian day
      (_, _, isoDow)     = toWeekDate day -- 1 = Monday .. 7 = Sunday
      dow0               = isoDow `rem` 7 -- 0 = Sunday .. 6 = Saturday
      tod                = timeToTimeOfDay (utctDayTime utc)
      hr                 = todHour tod
      mn                 = todMin tod
      dowMatches         = matchesField (eDayOfWeek expr) dow0 || matchesField (eDayOfWeek expr) isoDow
   in matchesField (eMinute expr) mn
        && matchesField (eHour expr) hr
        && matchesField (eDayOfMonth expr) dom
        && matchesField (eMonth expr) month
        && dowMatches

-- | Parse a `config/schedule.yml` ByteString into a `ScheduleFile`.
parseScheduleYaml :: ByteString -> Either String ScheduleFile
parseScheduleYaml rawYaml =
  case Yaml.decodeEither' rawYaml of
    Right sched -> Right sched
    Left err    -> Left (Yaml.prettyPrintParseException err)

-- | Load and parse `config/schedule.yml` from disk.
loadScheduleFile :: FilePath -> IO (Either String ScheduleFile)
loadScheduleFile path = do
  res <- Yaml.decodeFileEither path
  pure $ case res of
    Right sched -> Right sched
    Left err    -> Left (Yaml.prettyPrintParseException err)

-- | Capability Validation: Filter `ScheduleFile` entries against the Haskell
-- `Dispatcher`'s `registeredClasses` whitelist so a node only schedules and locks
-- cron jobs it can actually execute.
-- Returns `(executableJobs, skippedJobNames)`.
validateCapabilities
  :: HashSet ByteString
  -> ScheduleFile
  -> ([JobSpec], [Text])
validateCapabilities allowedClasses (ScheduleFile jobsMap) =
  Map.foldrWithKey classify ([], []) jobsMap
  where
    classify name spec (execAcc, skipAcc)
      | HS.member (jsClass spec) allowedClasses =
          (spec : execAcc, skipAcc)
      | otherwise =
          (execAcc, name : skipAcc)

-- | Execute a single cron evaluation tick for `nowUtc` inside `ReaderT Env IO`:
-- 1. Filters executable cron jobs matching `nowUtc`.
-- 2. Attempts to acquire the distributed Redis lock `wq:cron:lock:<name>:<epoch_minute>`.
-- 3. For each won lock, frames the job into a 64-byte Hybrid Frame and enqueues it
--    write-through into `MultiPriorityQueue` and Redis working leases.
executeTick :: UTCTime -> CronM Int
executeTick nowUtc = do
  Env {..} <- ask
  liftIO $ do
    let nowEpochSec = realToFrac (utcTimeToPOSIXSeconds nowUtc) :: Double
        epochMinute = floor (nowEpochSec / 60.0) :: Int
        dueSpecs    = filter (\s -> matchesTime (jsExpr s) nowUtc) ceSpecs
        tryLock s   = do
          let lockKey = "wq:cron:lock:" <> TE.encodeUtf8 (jsName s) <> ":" <> B8.pack (show epochMinute)
          acquireCronLock (rshStore ceRedisSync) lockKey nowEpochSec 90.0

    wonSpecs <- filterM tryLock dueSpecs
    for_ wonSpecs $ \spec -> do
      jId   <- atomicModifyIORef' ceJobCounter (\n -> (n + 1, n + 1))
      nowNs <- fromIntegral <$> getMonotonicTimeNSec
      let prio  = Frame.parsePriority (jsQueue spec)
          jid24 = formatJid epochMinute jId
          frame = Frame.encode Frame.Params
            { fpPriority     = prio
            , fpSource       = Frame.SourceCron
            , fpRetryCount   = 0
            , fpMaxRetries   = jsMaxRetries spec
            , fpJobId        = jId
            , fpSidekiqJid   = jid24
            , fpCreatedAtNs  = nowNs
            , fpQueueSlice   = jsQueue spec
            , fpTagSlice     = jsClass spec
            , fpPayloadSlice = jsArgsBytes spec
            }
      for_ (frameToSidekiqJson frame Nothing) (recordWorkingLeaseAsync ceRedisSync jid24)
      enqueueFrame ceQueue frame
      incEnqueued ceMetrics 1
      incCronTriggered ceMetrics

    pure (length wonSpecs)

formatJid :: Int -> Word64 -> ByteString
formatJid epochMin jId =
  let raw = B8.pack ("cron" <> show epochMin <> "_" <> show jId)
   in if B8.length raw >= 24
        then B8.take 24 raw
        else raw <> B8.replicate (24 - B8.length raw) '0'
