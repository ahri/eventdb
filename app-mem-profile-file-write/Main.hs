{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Database.EventDB

import Control.Concurrent.STM
import qualified Data.ByteString.Lazy.Char8 as B
import Data.Foldable
import System.Directory
import System.Environment
import Text.Printf

main :: IO ()
main = do
    (dir:mbs:_) <- getArgs
    removePathForcibly dir
    let mbs' = read mbs

    withConnection dir $ \conn -> traverse_
        ( (\evs -> atomically $ writeEventsAsync evs conn)
        . pure
        . B.pack
        . printf "%01048576d"
        )
        [(1::Int)..mbs']
