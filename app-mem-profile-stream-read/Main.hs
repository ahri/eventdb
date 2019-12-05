{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Database.EventDB

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
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

    withConnection dir id id $ \conn -> do
        stream <- openStream 0 conn
        evs' <- newTVarIO []
        _ <- forkIO $ forever $ do
            ev <- readEvent stream
            atomically $ modifyTVar evs' (\evs -> ev:evs)

        traverse_
            ( (\evs -> atomically $ writeEvents evs conn)
            . pure
            . B.pack
            . printf "%01048576d"
            )
            [(1::Int)..mbs']

        waitFor (fmap ((==mbs') . length) $ readTVar evs')

        evs <- fmap reverse $ readTVarIO evs'
        print . getSum $ foldMap (Sum . readInteger . B.unpack . snd) evs

  where
    readInteger :: String -> Integer
    readInteger = read

    waitFor sPred = atomically $ do
        res <- sPred
        unless res retry
