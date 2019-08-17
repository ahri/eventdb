{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Database.EventDB

import qualified Data.ByteString.Lazy.Char8 as C
import Data.Foldable
import Control.Monad
import Control.Concurrent
import Control.Concurrent.STM
import System.Directory
import System.Exit

main :: IO ()
main = do
    let testData :: [[Int]] = [[1], [2, 3], [4, 5, 6], [7]]
    let dir = "/tmp/eventdb-client-demo" -- TODO: use getTemporaryDirectory

    removePathForcibly dir
    conn <- openConnection dir 100

    let (th:tt) = testData

    atomically $ writeEventsAsync (fmap serialise th) conn
    awaitFlush conn

    stream <- openEventStream 0 conn
    sinkVal' <- newTVarIO []
    _ <- forkIO $ forever $ do
        ev <- fmap (deserialise . snd) $ readEvent stream
        atomically $ modifyTVar sinkVal' (\evs -> ev:evs)

    atomically $ traverse_ (\transaction -> writeEventsAsync (fmap serialise transaction) conn) tt
    let expectedLen = length $ join testData
    waitFor (fmap ((==expectedLen) . length) $ readTVar sinkVal')

    sinkVal <- fmap reverse $ readTVarIO sinkVal'

    when (join testData /= sinkVal) $ do
        putStrLn "Failure: data value & order must match exactly"
        print $ join testData
        print sinkVal
        exitFailure

    putStrLn "Success!"

  where
    serialise = C.pack . show
    deserialise = read . C.unpack

    waitFor sPred = atomically $ do
        res <- sPred
        unless res retry
