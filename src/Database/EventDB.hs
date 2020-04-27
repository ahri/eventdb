{-|
Module      : Database.EventDB
Description : An ACID-compliant and reasonably efficient database in which to
              store arbitrary binary events.
Copyright   : (c) Adam Piper, 2019
License     : Apache 2.0
Maintainer  : adam@ahri.net
Stability   : experimental
Portability : POSIX

A simple database to store a stream of events (facts) and retrieve them by
index. This is aimed at systems based on Event Sourcing where
complex state modelling/querying is left to other subsystems (e.g. your app, or
a relational/graph database).

The database is thread-safe for concurrent reads/writes, though writes are
locked to operate sequentially.

For more insight into the event driven/sourced programming style, see
<https://www.ahri.net/2019/07/practical-event-driven-and-sourced-programs-in-haskell/>.
-}

{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE CPP #-}

module Database.EventDB
    ( Connection -- don't leak constructor
    , Stream     -- don't leak constructor
    , Status (..)
    , IndexedEvent
    , openConnection
    , closeConnection
    , withConnection
    , eventCount
    , writeEvents
    , openStream
    , closeStream
    , withStream
    , readEvent
    , readSnapshot
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

-- | An event zipped with its position in the database.
type IndexedEvent a = (Word64, a)

data State = Open | Closed deriving (Eq, Show)

-- | A database connection. This is a mutable structure tracking allocated
-- resources.
data Connection a = Connection
    { connState      :: TVar State
    , pathIdx        :: FilePath
    , pathLog        :: FilePath
    , writeQueue     :: TQueue [B.ByteString]
    , evCount        :: TVar Word64
    , nextStreamId   :: TVar Natural
    , streams        :: TVar [Stream a]
    , cEvSerialize   :: (a -> B.ByteString)
    , cEvDeserialize :: (B.ByteString -> a)
    , closeLock      :: MVar ()
    , writeThread    :: ThreadId
    }

instance Eq (Connection a) where
    cx == cy = writeThread cx == writeThread cy

-- | The status of a database.
data Status = Status
    { indexCount         :: Word64 -- ^ number of events in the index
    , indexFileSizeBytes :: Word64 -- ^ size in bytes of the index
    , logFileSizeBytes   :: Word64 -- ^ size in bytes of the log
    , indexExcessBytes   :: Word64 -- ^ number of excess bytes in index
    , logExcessBytes     :: Word64 -- ^ number of excess bytes in log
    , consistent         :: Bool   -- ^ a determination of whether or not the database is consistent
    } deriving (Eq, Show)

-- | An event stream. This is a mutable structure tracking a pointer to an index
-- in the database allowing minimal memory usage when constructing an
-- application state from the stream.
data Stream a = Stream
    { streamId       :: Natural
    , streamConn     :: Connection a
    , streamState    :: TVar State
    , eventIndex     :: TVar Word64
    , fReadIdx       :: IdxFile
    , fReadLog       :: LogFile
    , sEvDeserialize :: (B.ByteString -> a)
    }

instance Eq (Stream a) where
    sx == sy = streamConn sx == streamConn sy && streamId sx == streamId sy

type File = (FilePath, Fd)
newtype IdxFile = IdxFile { unIdxFile :: File } deriving (Eq, Show)
newtype LogFile = LogFile { unLogFile :: File } deriving (Eq, Show)

-- | Open a database connection.
openConnection
    :: FilePath            -- ^ directory housing the database, will be created if needed
    -> (a -> B.ByteString) -- ^ event serialization function
    -> (B.ByteString -> a) -- ^ event deserialization function
    -> IO (Connection a)
openConnection dir fS fD = do
    doesDirectoryExist dir >>= (\dirExists -> unless dirExists $ do
        createDirectory dir
        setFileMode dir 0o700
        )

    pthIdxExists <- doesFileExist pthIdx
    pthLogExists <- doesFileExist pthLog

    fdIdx <- openWriteSync pthIdx
    fdLog <- openWriteSync pthLog

    -- when first creating db files, try to be secure
    unless pthIdxExists $ setFileMode pthIdx 0o600
    unless pthLogExists $ setFileMode pthLog 0o600

    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus fdIdx
    let fIdx = IdxFile (pthIdx, fdIdx)
        fLog = LogFile (pthLog, fdLog)

    -- init db if it's empty - also helps to fail fast if we don't have write perms
    evCount' <- if idxSize == 0
        then do
            writeAt (unIdxFile fIdx) 0 magic
            pure 0
        else bracket
            (fmap IdxFile $ openReadOnly pthIdx)
            (closeFd . snd . unIdxFile)
            eventCountFromFS

    st <- newTVarIO Open
    wq <- newTQueueIO
    ec <- newTVarIO evCount'
    ns <- newTVarIO 0
    ss <- newTVarIO []

    cl <- newEmptyMVar

    Connection st pthIdx pthLog wq ec ns ss fS fD cl
        <$> (forkIO $ bracket_
                (pure ())
                (do
                    transactions <- atomically $ flushTQueue wq
                    traverse_
                        (writeEventsFS fIdx fLog)
                        transactions

                    closeFd $ snd $ unLogFile fLog
                    closeFd $ snd $ unIdxFile fIdx

                    streams' <- readTVarIO ss
                    traverse_ closeStream streams'
                    atomically $ writeTVar ss []
                    putMVar cl ()
                )
                (forever
                    $   (atomically $ peekTQueue wq)
                    >>= writeEventsFS fIdx fLog
                    >> (atomically $ do
                        transactions <- readTQueue wq
                        ec' <- readTVar ec
                        writeTVar ec $ ec' + (fromIntegral $ length transactions)
                    )
                )
            )

  where
    pthIdx = joinPath [dir, "idx"]
    pthLog = joinPath [dir, "log"]

    openWriteSync path = do
        fd <- openFd path ReadWrite (Just 0o600) defaultFileFlags
        setFdOption fd SynchronousWrites True -- TODO: consider O_DSYNC as a data sync may be quicker - http://man7.org/linux/man-pages/man2/fdatasync.2.html
        pure fd

-- | Close a database connection. Writes all queued transactions, closes associated 'Stream's and frees all allocated resources. Blocks until done.
closeConnection :: Connection a -> IO ()
closeConnection conn = join . atomically $ assertConnState Open conn $ do
    -- design decision: casade behaviour from thread death rather than the state to avoid async exception mistakes
    atomically $ writeTVar (connState conn) Closed
    killThread $ writeThread conn
    readMVar $ closeLock conn

-- | Convenience function accepting a continuation for the connection. Opens and closes the connection, executing the continuation in an exception-safe context using 'bracket'.
withConnection
    :: FilePath               -- ^ directory housing the database, will be created if needed
    -> (a -> B.ByteString)    -- ^ event serialization function
    -> (B.ByteString -> a)    -- ^ event deserialization function
    -> (Connection a -> IO b) -- ^ action to execute with connection
    -> IO b
withConnection dir fS fD = bracket
    (openConnection dir fS fD)
    closeConnection

-- | Count of events currently stored in the database.
eventCount :: Connection a -> STM Word64
eventCount conn = join $ assertConnState Open conn $ readTVar $ evCount conn

eventCountFromFS :: IdxFile -> IO Word64
eventCountFromFS (IdxFile file) = do
    idxSize :: Word64 <- fmap (fromIntegral . fileSize) $ getFdStatus $ snd file
    if idxSize == magicSizeBytes
        then pure 0
        else do
            pIdxNext :: Word64 <- fmap decode $ readFrom file magicSizeBytes word64SizeBytes
            -- NB. even if pIdxNext is 0 here (i.e. first write was partial, so the commit is missing), the `div` works
            pure $ pIdxNext `div` word64SizeBytes

-- | Write a series of events as a single atomic transaction.
writeEvents :: [a] -> Connection a -> STM ()
writeEvents evs conn = join $ assertConnState Open conn $ writeTQueue (writeQueue conn) (fmap (cEvSerialize conn) evs)

-- | Open an event stream.
openStream :: Word64 -> Connection a -> IO (Stream a)
openStream from conn = join . atomically $ assertConnState Open conn $ do
    nxt <- readTVarIO $ nextStreamId conn
    stream <- Stream
        <$> pure nxt
        <*> pure conn
        <*> newTVarIO Open
        <*> newTVarIO from
        <*> (fmap IdxFile $ openReadOnly (pathIdx conn))
        <*> (fmap LogFile $ openReadOnly (pathLog conn))
        <*> (pure $ cEvDeserialize conn)

    atomically $ do
        streams' <- readTVar $ streams conn
        writeTVar (streams conn) $ stream : streams'
        writeTVar (nextStreamId conn) $ nxt + 1

    pure stream

-- | Close an event stream. Frees allocated resources.
closeStream :: Stream a -> IO ()
closeStream stream = join . atomically $ assertStreamState Open stream $ do
    atomically $ do
        let streams' = streams $ streamConn stream
        ss <- readTVar streams'
        let idx = streamId stream
        let filtered = foldr
                (\x xs ->
                    if streamId x == idx
                        then xs
                        else x : xs
                )
                []
                ss
        writeTVar streams' filtered

    closeFd $ snd $ unLogFile $ fReadLog stream
    closeFd $ snd $ unIdxFile $ fReadIdx stream

-- | Convenience function accepting a continuation for the stream. Opens and closes the stream, executing the continuation in an exception-safe context using 'bracket'.
withStream :: Word64 -> Connection a -> (Stream a -> IO b) -> IO b
withStream from conn = bracket
    (openStream from conn)
    closeStream

-- | Read an event. Blocks if none are available.
readEvent :: Stream a -> IO (IndexedEvent a)
readEvent stream = join . atomically $ assertStreamState Open stream $ do
    -- wait until db has one ready
    idx <- atomically $ do
        idx <- readTVar $ eventIndex stream
        count <- readTVar $ evCount $ streamConn stream
        unless (idx < count) retry
        pure idx

    -- read it
    evt <- (fmap.fmap) (sEvDeserialize stream) $ readEventFromFS (fReadIdx stream) (fReadLog stream) idx

    -- update our state
    atomically $ writeTVar (eventIndex stream) (idx + 1)

    pure evt

readEventFromFS :: IdxFile -> LogFile -> Word64 -> IO (IndexedEvent B.ByteString)
readEventFromFS fIdx fLog idx = do
    -- resolve ptrs
    (pLogFrom, pLogTo) <- if idx == 0
        then do
            pLogTo <- readFrom
                (unIdxFile fIdx)
                (headerSizeBytes + (word64SizeBytes * idx))
                word64SizeBytes
            pure (0, decode pLogTo)

        else do
            (pLogFrom, pLogTo) <- fmap (B.splitAt word64SizeBytes) $ readFrom
                (unIdxFile fIdx)
                (headerSizeBytes + (word64SizeBytes * (idx - 1)))
                (word64SizeBytes*2)
            pure (decode pLogFrom, decode pLogTo)

    -- read data
    fmap (idx,) $ readFrom (unLogFile fLog) pLogFrom (fromIntegral $ pLogTo - pLogFrom)

-- | Read a snapshot of the database at call time. Note that any events added while reading will not be iterated over.
readSnapshot :: Connection a -> Word64 -> (Word64 -> IndexedEvent a -> IO ()) -> IO ()
readSnapshot dbConn from f = do
    c <- atomically $ eventCount dbConn
    withStream from dbConn $ \stream -> readEvt stream from c

  where
    readEvt stream idx c = if idx == c
        then pure ()
        else do
            readEvent stream >>= f c
            readEvt stream (idx + 1) c

{-| __WARNING: reads the whole DB, can be very expensive in terms of time and resources.__

    Inspect a database, verifying its consistency and reporting on extraneous
    bytes leftover from failed writes, returning a simple notion of consistency.
-}
inspect :: FilePath -> IO Status
inspect dir = withConnection dir id id $ \conn -> withRead conn $ \(fIdx, fLog) -> do
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

    let drain stream = do
            count <- atomically $ eventCount conn
            if count == 0
                then pure ()
                else do
                    (idx, _) <- readEvent stream
                    if count - 1 == idx
                        then pure ()
                        else drain stream

    openStream 0 conn >>= drain -- we do this just to evaluate all written data

    pLogNext :: Word64 <- if emptyDb
        then pure 0
        else fmap decode $ readFrom (unIdxFile fIdx) pIdxNext' word64SizeBytes

    let idxExcessBytes = if emptyDb
            then 0
            else idxSize `natSubt` (pIdxNext' + headerSizeBytes)
        logExcessBytes' = logSize `natSubt` pLogNext

    let consistent' = (idxCount >= expectedCount)
            && (logSize >= pLogNext)

    pure $ Status expectedCount idxSize logSize idxExcessBytes logExcessBytes' consistent'

writeEventsFS :: IdxFile -> LogFile -> [B.ByteString] -> IO [IndexedEvent B.ByteString]
writeEventsFS fIdx fLog bss = case bss of
    [] -> pure []
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

        (_pIdxNext', _) <- foldM
            (\(pIdxNext', pLogNext') bs -> do
                -- calculate new offsets
                let pIdxNext'' = pIdxNext' + word64SizeBytes
                    pLogNext'' = pLogNext' + (fromIntegral . B.length $ bs)

                -- write the event data
                writeAt (unLogFile fLog) pLogNext' bs
                -- write index ptr for next time
                writeAt (unIdxFile fIdx) pIdxNext'' $ encode pLogNext''

                pure (pIdxNext'', pLogNext'')
            )
            (pIdxNext, pLogNext)
            bss

        let firstIdxWritten = (idxSize `natSubt` magicSizeBytes) `div` word64SizeBytes

#ifndef BREAKDB_OMIT_COMMIT
        -- commit
        writeAt (unIdxFile fIdx) magicSizeBytes $ encode _pIdxNext'
#endif
        pure $ zip [firstIdxWritten..] bss

-- Unsafe - don't leak this outside the module, or use the ST trick
withRead :: Connection a -> ((IdxFile, LogFile) -> IO b) -> IO b
withRead conn = bracket
        ((,)
            <$> (fmap IdxFile $ openReadOnly (pathIdx conn))
            <*> (fmap LogFile $ openReadOnly (pathLog conn))
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
    -- TODO: fdPwritev doesn't exist so there's an extra syscall here, consider calling pwritev(2) via foreign call
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

openReadOnly :: FilePath -> IO File
openReadOnly path = fmap (path,) $ openFd path ReadOnly Nothing defaultFileFlags

assertConnState :: State -> Connection a -> b -> STM b
assertConnState state conn x = do
    state' <- readTVar $ connState conn
    if state' /= state
        then error $ "Expected connection to be " <> show state
        else pure x

assertStreamState :: State -> Stream a -> b -> STM b
assertStreamState state stream x = do
    state' <- readTVar $ streamState stream
    if state' /= state
        then error $ "Expected stream to be " <> show state
        else pure x
