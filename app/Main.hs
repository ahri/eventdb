{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

module Main where

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

    _ <- (flip traverse) optionalArgs $ \case
        "thrash" -> forM_ [1..10::Int] $ \_ -> withConnection dir $ \conn ->
            mapConcurrently
                (\f -> forM_ [1..100::Int] $ \_ -> f conn)
                [ spam "foo"
                , listAll
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
