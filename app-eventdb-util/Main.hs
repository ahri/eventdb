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
        empty :: Connection B.ByteString -> IO ()
        empty conn = atomically $ writeEvents [] conn

        spam :: T.Text -> Connection B.ByteString -> IO ()
        spam msg conn = atomically $ writeEvents [B.fromStrict $ T.encodeUtf8 msg] conn

        spamTriple :: T.Text -> Connection B.ByteString -> IO ()
        spamTriple msg conn = atomically $ writeEvents (take 3 $ repeat (B.fromStrict $ T.encodeUtf8 msg)) conn

        listAll :: Connection B.ByteString -> IO ()
        listAll conn = drain conn

        listX :: Word64 -> Connection B.ByteString -> IO ()
        listX count conn = do
            actualCount <- atomically $ eventCount conn
            let c = if actualCount < count
                then 0
                else actualCount - count
            drain' c conn

    _ <- (flip traverse) optionalArgs $ \case
        "thrash" -> forM_ [1..10::Int] $ \_ -> withConnection dir id id $ \conn ->
            mapConcurrently
                (\f -> forM_ [1..100::Int] $ \_ -> f conn)
                [ spam "foo"
                , listX 3
                , spam "bar"
                , listX 3
                , spam "baz"
                , listX 3
                ]

        -- takes 47.08s to read 3000 events 1000 times, so 300,000 events
        "listall" -> forM_ [1..10::Int] $ \_ -> withConnection dir id id $ \conn ->
            mapConcurrently
                (\f -> forM_ [1..100::Int] $ \_ -> f conn)
                [ listAll
                ]

        "spam" -> withConnection dir id id $ \conn ->
            mapConcurrently
                (\f -> forM_ [1..100::Int] $ \_ -> f conn)
                [ spam "foo"
                , spamTriple "bar"
                , spam "baz"
                ] >> pure()

        "inspect" -> do
            status <- inspect dir
            putStrLn $ "Index count: " <> (show $ indexCount status)
            unless (consistent status) exitFailure

        "empty" -> withConnection dir id id $ \conn -> empty conn

        "single" -> withConnection dir id id $ \conn -> spam "foo" conn

        "triple" -> withConnection dir id id $ \conn -> spamTriple "bar" conn

        unknown -> do
            hPutStr stderr $ "Unknown arg: " <> unknown
            exitFailure

    pure ()

  where
    drain conn = do
        count <- atomically $ eventCount conn
        drain' count conn

    drain' count conn = openStream 0 conn >>= drain'' count

    drain'' count stream = do
        (idx, _) <- readEvent stream
        if count - 1 == idx
            then pure ()
            else drain'' count stream
