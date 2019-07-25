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
    , writeEvents
    , readEventsFrom
    , inspect
    ) where

import Control.Exception
import Control.Concurrent
import Control.Monad
import qualified Data.ByteString.Lazy as B
import qualified System.Posix.IO.ByteString.Lazy as B
import Data.Binary
import Data.Monoid
import Foreign.C.Types (CSize)
import Foreign.Storable
import GHC.IO.Device (SeekMode (..))
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
    { pathIdx :: FilePath
    , pathLog :: FilePath
    , lock    :: MVar (IdxFile, LogFile)
    }

type File = (FilePath, Fd)
newtype IdxFile = IdxFile { unIdxFile :: File } deriving (Eq, Show)
newtype LogFile = LogFile { unLogFile :: File } deriving (Eq, Show)

-- | Open a database connection.
openConnection
    :: FilePath      -- ^ directory housing the database, will be created if needed
    -> IO Connection
openConnection dir = do
    createDirectoryIfMissing False dir
    fdIdx <- openWriteSync pthIdx
    fdLog <- openWriteSync pthLog
    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus fdIdx
    let fIdx = (pthIdx, fdIdx)
        fLog = (pthLog, fdLog)
    when (idxSize == 0) $ writeAt fIdx 0 magic

    Connection pthIdx pthLog <$> newMVar (IdxFile fIdx, LogFile fLog)
  where
    pthIdx = joinPath [dir, "idx"]
    pthLog = joinPath [dir, "log"]

    openWriteSync path = do
        fd <- openFd path ReadWrite (Just 0o644) defaultFileFlags
        setFdOption fd SynchronousWrites True -- TODO: consider O_DSYNC as a data sync may be quicker - http://man7.org/linux/man-pages/man2/fdatasync.2.html
        pure fd

-- | Close a database connection.
closeConnection :: Connection -> IO ()
closeConnection conn = do
    (fIdx, fLog) <- takeMVar $ lock conn
    closeFd $ snd . unLogFile $ fLog
    closeFd $ snd . unIdxFile $ fIdx

-- | Convenience function accepting a continuation for the connection. Opens and closes the connection for you.
withConnection :: FilePath -> (Connection -> IO a) -> IO a
withConnection dir = bracket
    (openConnection dir)
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
writeEvents :: [B.ByteString] -> Connection -> IO Word64
writeEvents bss conn = withWrite conn $ \(fIdx, fLog) -> do
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
    -- TODO: write timestamp?
    _ <- (flip traverse) bss $ writeAt (unLogFile fLog) pLogNext

    -- calculate new offsets
    let pIdxNext' = pIdxNext + word64SizeBytes
        pLogNext' = pLogNext + (getSum $ foldMap (Sum . fromIntegral . B.length) bss)
        eventId   = (pIdxNext' - headerSizeBytes) `div` word64SizeBytes

    -- write index ptr for next time
    writeAt (unIdxFile fIdx) pIdxNext' $ encode pLogNext'

    -- commit
#ifndef BREAKDB_OMIT_COMMIT
    writeAt (unIdxFile fIdx) magicSizeBytes $ encode pIdxNext'
#endif

    pure eventId

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

-- | Inspect a database, verifying its consistency and reporting on extraneous bytes leftover from failed writes, returning a simple notion of consistency.
inspect :: Connection -> IO Bool
inspect conn = withWrite conn $ \(fIdx, fLog) -> do
    -- TODO: should catch exceptions here really

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
    putStrLn $ "Index count: " <> show idxCount

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

-- Unsafe - don't leak this outside the module, or use the ST trick
withWrite :: Connection -> ((IdxFile, LogFile) -> IO a) -> IO a
withWrite conn = let writeLock = lock conn in bracket
    (takeMVar writeLock)
    (putMVar writeLock)

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
