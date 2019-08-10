{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Database.EventDB

import qualified Data.ByteString.Lazy.Char8 as C
import Data.Foldable
import Control.Monad
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
    waitForCount conn $ length th

    (eh, evChan) <- readEvents 0 conn

    atomically $ traverse_ (\transaction -> writeEventsAsync (fmap serialise transaction) conn) tt
    waitForCount conn $ length $ join testData

    sinkVal <- fmap (reverse . fmap (deserialise . snd)) $ atomically $ drain evChan eh

    when (join testData /= sinkVal) $ do
        putStrLn "Failure: data value & order must match exactly"
        print $ deserialise $ snd $ head eh
        print $ join testData
        print sinkVal
        exitFailure

    putStrLn "Success!"

  where
    serialise = C.pack . show
    deserialise = read . C.unpack

    waitForCount conn count = atomically go
      where go = do
                count' <- fmap fromIntegral $ eventCount conn
                if count' >= count
                    then pure ()
                    else go

    drain :: forall a. TChan a -> [a] -> STM [a]
    drain chan lst = do
        empty <- isEmptyTChan chan
        if empty
            then pure lst
            else do
                val <- readTChan chan
                drain chan (val:lst)
