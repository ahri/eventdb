{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}

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
    , lock    :: MVar (Fd, Fd) -- TODO: newtypes over Idx, Log
    }

-- | Open a database connection.
openConnection
    :: FilePath      -- ^ directory housing the database, will be created if needed
    -> IO Connection
openConnection dir = do
    createDirectoryIfMissing False dir
    fdIdx <- openWriteSync pthIdx
    fdLog <- openWriteSync pthLog
    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus fdIdx
    when (idxSize == 0) $ writeAt fdIdx 0 magic

    Connection pthIdx pthLog <$> newMVar (fdIdx, fdLog)
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
    (fdIdx, fdLog) <- takeMVar $ lock conn
    closeFd fdLog
    closeFd fdIdx

-- | Convenience function accepting a continuation for the connection. Opens and closes the connection for you.
withConnection :: FilePath -> (Connection -> IO a) -> IO a
withConnection dir = bracket
    (openConnection dir)
    closeConnection

-- | Count of events currently stored in the database.
eventCount :: Connection -> IO Word64
eventCount conn = withRead conn $ \(fdIdx, _) -> do
    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus fdIdx
    if idxSize == magicSizeBytes
        then pure 0
        else do
            pIdxNext :: Word64 <- fmap decode $ readFrom fdIdx magicSizeBytes word64SizeBytes
            pure $ pIdxNext `div` word64SizeBytes

-- | Write a series of events as a single atomic transaction.
writeEvents :: [B.ByteString] -> Connection -> IO Word64
writeEvents bss conn = withWrite conn $ \(fdIdx, fdLog) -> do
    -- determine where in the log to write
    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus fdIdx
    (pIdxNext :: Word64, pLogNext :: Word64) <- if idxSize == magicSizeBytes
        then pure (magicSizeBytes, 0)
        else do
            pIdxNext <- fmap decode $ readFrom fdIdx magicSizeBytes word64SizeBytes
            pLogNext <- fmap decode $ readFrom fdIdx pIdxNext word64SizeBytes

            pure (pIdxNext, pLogNext)

    -- write the event data
    -- TODO: write timestamp?
    _ <- (flip traverse) bss $ writeAt fdLog pLogNext

    -- calculate new offsets
    let pIdxNext' = pIdxNext + word64SizeBytes
        pLogNext' = pLogNext + (getSum $ foldMap (Sum . fromIntegral . B.length) bss)
        eventId   = (pIdxNext' - headerSizeBytes) `div` word64SizeBytes

    -- write index ptr for next time
    writeAt fdIdx pIdxNext' $ encode pLogNext'

    -- commit
    writeAt fdIdx magicSizeBytes $ encode pIdxNext'

    pure eventId

-- | Read all events from the specified index.
readEventsFrom :: Word64 -> Connection -> IO [(Word64, B.ByteString)]
readEventsFrom idx conn = withRead conn $ \(fdIdx, fdLog) -> do
    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus fdIdx
    if idxSize == magicSizeBytes
        then pure []
        else do
            pIdxNext :: Word64 <- fmap decode $ readFrom fdIdx magicSizeBytes word64SizeBytes
            let lastIdx = (pIdxNext - headerSizeBytes) `div` word64SizeBytes
            (flip traverse) [idx..lastIdx] $ \i -> do
                pLogStart <- if i == 0
                    then pure 0
                    else fmap decode $ readFrom fdIdx (magicSizeBytes + (i * word64SizeBytes)) word64SizeBytes
                pLogUpTo  <- fmap decode $ readFrom fdIdx (magicSizeBytes + ((i+1) * word64SizeBytes)) word64SizeBytes
                fmap (i,) $ readFrom fdLog pLogStart (fromIntegral $ pLogUpTo - pLogStart)

-- | Inspect a database, verifying its consistency and reporting on extraneous bytes leftover from failed writes, returning a simple notion of consistency.
inspect :: Connection -> IO Bool
inspect conn = withWrite conn $ \(fdIdx, fdLog) -> do
    -- TODO: should catch exceptions here really

    pIdxNext :: Word64 <- fmap decode $ readFrom fdIdx magicSizeBytes word64SizeBytes
    let expectedCount = pIdxNext `div` word64SizeBytes
    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus fdIdx
    logSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus fdLog
    let idxCount = (idxSize - headerSizeBytes) `div` word64SizeBytes
    putStrLn $ "Index file size: " <> show idxSize
    putStrLn $ "Log file size: " <> show logSize
    putStrLn ""
    putStrLn $ "Expected count: " <> show expectedCount
    putStrLn $ "Index count: " <> show idxCount

    len <- fmap length $ readEventsFrom 0 conn
    putStrLn $ "Actual read data count: " <> show len
    -- if not equal then invalid
    pLogNext :: Word64 <- fmap decode $ readFrom fdIdx pIdxNext word64SizeBytes

    let idxExcessBytes = idxSize - (pIdxNext + word64SizeBytes)
        logExcessBytes = logSize - pLogNext

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
withRead :: Connection -> ((Fd, Fd) -> IO a) -> IO a
withRead conn = bracket
        ((,)
            <$> openFd (pathIdx conn) ReadOnly Nothing defaultFileFlags
            <*> openFd (pathLog conn) ReadOnly Nothing defaultFileFlags
        )
        (\(fdIdx, fdLog) -> do
            closeFd fdLog
            closeFd fdIdx
        )

-- Unsafe - don't leak this outside the module, or use the ST trick
withWrite :: Connection -> ((Fd, Fd) -> IO a) -> IO a
withWrite conn = let writeLock = lock conn in bracket
    (takeMVar writeLock)
    (putMVar writeLock)

writeAt :: Fd -> Word64 -> B.ByteString -> IO ()
writeAt fd addr bs = do
    _ <- fdSeek fd AbsoluteSeek $ fromIntegral addr
    _ <- B.fdWritev fd bs
    pure ()

readFrom :: Fd -> Word64 -> CSize -> IO B.ByteString
readFrom fd addr count = do
    _ <- fdSeek fd AbsoluteSeek $ fromIntegral addr
    B.fdRead fd count
