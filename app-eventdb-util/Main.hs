{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Data.Word
import Control.Concurrent.Async
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
        hPutStr stderr "Usage: dir_name [commands...]\n where commands are: spam, thrash, single, inspect\n"
        exitFailure

    let (dir:optionalArgs) = args

    let
        spam :: T.Text -> Connection -> IO ()
        spam msg conn = do
            _ <- writeEvents [B.fromStrict $ T.encodeUtf8 msg] conn
            pure ()

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
        "thrash" -> forM_ [1..10::Int] $ \_ -> withConnection dir $ \conn ->
            mapConcurrently
                (\f -> forM_ [1..100::Int] $ \_ -> f conn)
                [ spam "foo"
                , listX 3
                , spam "bar"
                , listX 3
                , spam "baz"
                , listX 3
                ] >> pure ()

        -- takes 47.08s to read 3000 events 1000 times, so 300,000 events
        "listall" -> forM_ [1..10::Int] $ \_ -> withConnection dir $ \conn ->
            mapConcurrently
                (\f -> forM_ [1..100::Int] $ \_ -> f conn)
                [ listAll
                ] >> pure ()

        "spam" -> withConnection dir $ \conn ->
            mapConcurrently
                (\f -> forM_ [1..100::Int] $ \_ -> f conn)
                [ spam "foo"
                , spam "bar"
                , spam "baz"
                ] >> pure ()

        "inspect" -> withConnection dir $ \conn -> do
            isConsistent <- inspect conn
            when (not isConsistent) exitFailure

        "single" -> withConnection dir $ spam "foo"

        unknown -> do
            hPutStr stderr $ "Unknown arg: " <> unknown
            exitFailure

    pure ()
