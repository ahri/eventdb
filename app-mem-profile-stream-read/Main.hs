{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Database.EventDB

import Control.Concurrent.STM
import qualified Data.ByteString.Lazy.Char8 as B
import Data.Foldable
import Data.Monoid
import System.Directory
import System.Environment
import Text.Printf

main :: IO ()
main = do
    (dir:mbs:_) <- getArgs
    removePathForcibly dir
    let mbs' = read mbs
    removePathForcibly dir

    withConnection dir singleElementWriteBuffer $ \conn -> do
        (_, chan) <- readEvents 0 conn

        traverse_
            ( (\evs -> atomically $ writeEventsAsync evs conn)
            . pure
            . B.pack
            . printf "%01048576d"
            )
            [(1::Int)..mbs']

        awaitFlush conn

        evs <- atomically $ drain chan []
        print . getSum $ foldMap (Sum . readInteger . B.unpack . snd) evs


  where
    singleElementWriteBuffer = 1

    readInteger :: String -> Integer
    readInteger = read

    drain :: forall a. TChan a -> [a] -> STM [a]
    drain chan lst = do
        empty <- isEmptyTChan chan
        if empty
            then pure lst
            else do
                val <- readTChan chan
                drain chan (val:lst)
