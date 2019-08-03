{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE CPP #-}

module Database.EventDB
    ( Connection -- don't leak constructor
    , openConnection
    , closeConnection
    , withConnection
    , eventCount
    , writeEventsAsync
    , awaitFlush
    , readEventsFrom
    , inspect
    ) where

import Control.Exception.Safe
import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import qualified Data.ByteString.Lazy as B
import qualified System.Posix.IO.ByteString.Lazy as B
import Data.Binary
import Data.Foldable
import Data.Monoid
import Foreign.C.Types (CSize)
import Foreign.Storable
import GHC.IO.Device (SeekMode (..))
import Numeric.Natural
import System.Directory
import System.Posix.Files
import System.Posix.IO
import System.Posix.Types
import System.FilePath.Posix

magic :: B.ByteString
magic = "ed01"

magicSizeBytes :: Num a => a
magicSizeBytes = fromIntegral $ B.length magic

word64SizeBytes :: Num a => a
word64SizeBytes = fromIntegral $ sizeOf (0 :: Word64)

headerSizeBytes :: Num a => a
headerSizeBytes = magicSizeBytes + word64SizeBytes

-- | Represent a database connection.
data Connection = Connection
    { pathIdx     :: FilePath
    , pathLog     :: FilePath
    , writeQueue  :: TBQueue [B.ByteString]
    , writeThread :: ThreadId
    }

type File = (FilePath, Fd)
newtype IdxFile = IdxFile { unIdxFile :: File } deriving (Eq, Show)
newtype LogFile = LogFile { unLogFile :: File } deriving (Eq, Show)

-- | Open a database connection.
openConnection
    :: FilePath      -- ^ directory housing the database, will be created if needed
    -> Natural       -- ^ max number of transactions to queue before blocking (affects memory usage)
    -> IO Connection
openConnection dir queueSize = do
    createDirectoryIfMissing False dir
    fdIdx <- openWriteSync pthIdx
    fdLog <- openWriteSync pthLog
    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus fdIdx
    let fIdx = (pthIdx, fdIdx)
        fLog = (pthLog, fdLog)

    -- init db if it's empty - also helps to fail fast if we don't have write perms
    when (idxSize == 0) $ writeAt fIdx 0 magic

    wq <- newTBQueueIO queueSize

    Connection pthIdx pthLog wq <$> forkWriteThread (IdxFile fIdx) (LogFile fLog) wq

  where
    pthIdx = joinPath [dir, "idx"]
    pthLog = joinPath [dir, "log"]

    openWriteSync path = do
        fd <- openFd path ReadWrite (Just 0o644) defaultFileFlags
        setFdOption fd SynchronousWrites True -- TODO: consider O_DSYNC as a data sync may be quicker - http://man7.org/linux/man-pages/man2/fdatasync.2.html
        pure fd

    forkWriteThread :: IdxFile -> LogFile -> TBQueue [B.ByteString] -> IO ThreadId
    forkWriteThread fIdx fLog wq = forkIO $ bracket
        (pure ())
        (\() -> do
            pendingTransactions <- atomically $ flushTBQueue wq
            traverse_ (writeEvents fIdx fLog) pendingTransactions
            closeFd $ snd $ unLogFile fLog
            closeFd $ snd $ unIdxFile fIdx
        )
        (\() -> forever $ do
            evs <- atomically $ readTBQueue wq
            writeEvents fIdx fLog evs
        )

-- | Close a database connection.
closeConnection :: Connection -> IO ()
closeConnection conn = killThread $ writeThread conn

-- | Convenience function accepting a continuation for the connection. Opens and closes the connection for you.
withConnection :: FilePath -> Natural -> (Connection -> IO a) -> IO a
withConnection dir queueSize = bracket
    (openConnection dir queueSize)
    closeConnection

-- | Count of events currently stored in the database.
eventCount :: Connection -> IO Word64
eventCount conn = withRead conn $ \(fIdx, _) -> do
    let file = unIdxFile fIdx
    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus $ snd file
    if idxSize == magicSizeBytes
        then pure 0
        else do
            pIdxNext :: Word64 <- fmap decode $ readFrom file magicSizeBytes word64SizeBytes
            pure $ pIdxNext `div` word64SizeBytes

-- | Write a series of events as a single atomic transaction.
writeEventsAsync :: [B.ByteString] -> Connection -> STM ()
writeEventsAsync bs conn = writeTBQueue (writeQueue conn) bs

-- | Read all events from the specified index.
readEventsFrom :: Word64 -> Connection -> IO [(Word64, B.ByteString)]
readEventsFrom idx conn = withRead conn $ \(fIdx, fLog) -> do
    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus $ snd $ unIdxFile fIdx
    if idxSize == magicSizeBytes
        then pure []
        else do
            pIdxNext :: Word64 <- fmap decode $ readFrom (unIdxFile fIdx) magicSizeBytes word64SizeBytes
            let missingCommit = pIdxNext == 0
            if missingCommit
                then pure []
                else do
                    let lastIdx = (pIdxNext `natSubt` headerSizeBytes) `div` word64SizeBytes
                    (flip traverse) [idx..lastIdx] $ \i -> do
                        pLogStart <- if i == 0
                            then pure 0
                            else fmap decode $ readFrom (unIdxFile fIdx) (magicSizeBytes + (i * word64SizeBytes)) word64SizeBytes
                        pLogUpTo  <- fmap decode $ readFrom (unIdxFile fIdx) (magicSizeBytes + ((i+1) * word64SizeBytes)) word64SizeBytes
                        fmap (i,) $ readFrom (unLogFile fLog) pLogStart (fromIntegral $ pLogUpTo `natSubt` pLogStart)

-- | Block waiting for events to be written to the disk
awaitFlush :: Connection -> IO ()
awaitFlush conn = atomically $ do
    empty <- isEmptyTBQueue $ writeQueue conn
    when (not empty) retry

-- | Inspect a database, verifying its consistency and reporting on extraneous bytes leftover from failed writes, returning a simple notion of consistency.
inspect :: Connection -> IO Bool
inspect conn = withRead conn $ \(fIdx, fLog) -> do
    -- TODO: should catch exceptions here really
    -- TODO: no longer locks reads - could result in inconsistent results - it's only for test though so does it matter?
    awaitFlush conn

    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus $ snd $ unIdxFile fIdx
    logSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus $ snd $ unLogFile fLog

    let emptyDb = idxSize == magicSizeBytes

    pIdxNext :: Word64 <- if idxSize == magicSizeBytes
        then pure magicSizeBytes
        else fmap decode $ readFrom (unIdxFile fIdx) magicSizeBytes word64SizeBytes

    let missingCommit = pIdxNext == 0
        pIdxNext' = if missingCommit 
            then magicSizeBytes
            else pIdxNext

    let expectedCount = (pIdxNext' `natSubt` magicSizeBytes) `div` word64SizeBytes
    let idxCount = if emptyDb then 0 else (idxSize `natSubt` headerSizeBytes) `div` word64SizeBytes
    putStrLn $ "Index file size (bytes): " <> show idxSize
    putStrLn $ "Log file size (bytes): " <> show logSize
    putStrLn ""
    putStrLn $ "Expected count: " <> show expectedCount
    putStrLn $ "Count guess based on filesize: " <> show idxCount

    len <- fmap length $ readEventsFrom 0 conn
    putStrLn $ "Actual read data count: " <> show len
    -- if not equal then invalid
    pLogNext :: Word64 <- if emptyDb
        then pure 0
        else fmap decode $ readFrom (unIdxFile fIdx) pIdxNext' word64SizeBytes

    let idxExcessBytes = if emptyDb
            then 0
            else idxSize `natSubt` (pIdxNext' + headerSizeBytes)
        logExcessBytes = logSize `natSubt` pLogNext

    putStrLn ""

    putStrLn $ "Index excess bytes: " <> show idxExcessBytes
    putStrLn $ "Log excess bytes: " <> show logExcessBytes

    putStrLn ""

    let consistent = (idxCount >= expectedCount)
            && (fromIntegral len == expectedCount)
            && (logSize >= pLogNext)

    putStrLn $ if consistent
        then "Consistent :)"
        else "Inconsistent :("

    -- TODO: instead of printing, construct a data type
    pure consistent

writeEvents :: IdxFile -> LogFile -> [B.ByteString] -> IO ()
writeEvents fIdx fLog bss = case bss of
    [] -> pure ()
    _  -> do
        -- determine where in the log to write
        idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus (snd $ unIdxFile fIdx)
        let emptyDb = idxSize == magicSizeBytes
        (pIdxNext :: Word64, pLogNext :: Word64) <- if emptyDb
            then pure (magicSizeBytes, 0)
            else do
                pIdxNext <- fmap decode $ readFrom (unIdxFile fIdx) magicSizeBytes word64SizeBytes
                let missingCommit = pIdxNext == 0
                if missingCommit
                    then pure (magicSizeBytes, 0)
                    else do
                        pLogNext <- fmap decode $ readFrom (unIdxFile fIdx) pIdxNext word64SizeBytes
                        pure (pIdxNext, pLogNext)

        -- write the event data
        _ <- (flip traverse) bss $ writeAt (unLogFile fLog) pLogNext

        -- calculate new offsets
        let pIdxNext' = pIdxNext + word64SizeBytes
            pLogNext' = pLogNext + (getSum $ foldMap (Sum . fromIntegral . B.length) bss)

        -- write index ptr for next time
        writeAt (unIdxFile fIdx) pIdxNext' $ encode pLogNext'

            -- commit
#ifndef BREAKDB_OMIT_COMMIT
        writeAt (unIdxFile fIdx) magicSizeBytes $ encode pIdxNext'
#endif

-- Unsafe - don't leak this outside the module, or use the ST trick
withRead :: Connection -> ((IdxFile, LogFile) -> IO a) -> IO a
withRead conn = bracket
        ((,)
            <$> (do
                    fdIdx <- openFd (pathIdx conn) ReadOnly Nothing defaultFileFlags
                    pure $ IdxFile (pathIdx conn, fdIdx)
                )
            <*> (do
                    fdLog <- openFd (pathLog conn) ReadOnly Nothing defaultFileFlags
                    pure $ LogFile (pathLog conn, fdLog)
                )
        )
        (\(fIdx, fLog) -> do
            closeFd $ snd $ unLogFile fLog
            closeFd $ snd $ unIdxFile fIdx
        )

writeAt :: File -> Word64 -> B.ByteString -> IO ()
writeAt file addr bs = do
    let (_name, fd) = file
#ifdef DEBUG
    putStrLn $ "Writing " <> _name <> ": @" <> show addr <> ", " <> show (B.length bs) <> " bytes"
#endif
    _ <- fdSeek fd AbsoluteSeek $ fromIntegral addr
    _ <- B.fdWritev fd bs
    pure ()

readFrom :: File -> Word64 -> CSize -> IO B.ByteString
readFrom file addr count = do
    let (_name, fd) = file
#ifdef DEBUG
    putStrLn $ "Reading " <> _name <> ": @" <> show addr <> ", " <> show count <> " bytes"
#endif
    B.fdPread fd count (fromIntegral addr)

natSubt :: Word64 -> Word64 -> Word64
natSubt x y = if y > x
    then 0
    else x - y
