{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Data.Word
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad
import qualified Data.ByteString.Lazy as B
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import System.Exit
import System.Environment
import System.IO (hPutStr, stderr)

import Database.EventDB

main :: IO ()
main = do
    args <- getArgs
    when (length args < 2) $ do
        hPutStr stderr "Usage: dir_name [commands...]\n where commands are: spam, thrash, single, triple, inspect\n"
        exitFailure

    let (dir:optionalArgs) = args

    let
        empty :: Connection -> IO ()
        empty conn = atomically $ writeEventsAsync [] conn

        spam :: T.Text -> Connection -> IO ()
        spam msg conn = atomically $ writeEventsAsync [B.fromStrict $ T.encodeUtf8 msg] conn

        spamTriple :: T.Text -> Connection -> IO ()
        spamTriple msg conn = atomically $ writeEventsAsync (take 3 $ repeat (B.fromStrict $ T.encodeUtf8 msg)) conn

        triple :: Connection -> IO ()
        triple conn = atomically $ writeEventsAsync (fmap (B.fromStrict . T.encodeUtf8) ["foo", "bar", "baz"]) conn

        listAll :: Connection -> IO ()
        listAll conn = do
            _ <- (fmap.fmap.fmap) (T.decodeUtf8 . B.toStrict) $ readEventsFrom 0 conn
            pure ()

        listX :: Word64 -> Connection -> IO ()
        listX count conn = do
            actualCount <- eventCount conn
            let c = if actualCount < count
                then 0
                else actualCount - count
            _ <- (fmap.fmap.fmap) (T.decodeUtf8 . B.toStrict) $ readEventsFrom c conn
            pure ()

    _ <- (flip traverse) optionalArgs $ \case
        "thrash" -> forM_ [1..10::Int] $ \_ -> withConnection dir 1000 $ \conn ->
            mapConcurrently
                (\f -> forM_ [1..100::Int] $ \_ -> f conn)
                [ spam "foo"
                , listX 3
                , spam "bar"
                , listX 3
                , spam "baz"
                , listX 3
                ] >> awaitFlush conn

        -- takes 47.08s to read 3000 events 1000 times, so 300,000 events
        "listall" -> forM_ [1..10::Int] $ \_ -> withConnection dir 1000 $ \conn ->
            mapConcurrently
                (\f -> forM_ [1..100::Int] $ \_ -> f conn)
                [ listAll
                ] >> pure ()

        "spam" -> withConnection dir 1000 $ \conn ->
            mapConcurrently
                (\f -> forM_ [1..100::Int] $ \_ -> f conn)
                [ spam "foo"
                , spamTriple "bar"
                , spam "baz"
                ] >> awaitFlush conn

        "inspect" -> withConnection dir 1000 $ \conn -> do
            isConsistent <- inspect conn
            when (not isConsistent) exitFailure

        "empty" -> withConnection dir 1000 $ \conn -> empty conn >> awaitFlush conn

        "single" -> withConnection dir 1000 $ \conn -> spam "foo" conn >> awaitFlush conn

        "triple" -> withConnection dir 1000 $ \conn -> triple conn >> awaitFlush conn

        unknown -> do
            hPutStr stderr $ "Unknown arg: " <> unknown
            exitFailure

    pure ()
