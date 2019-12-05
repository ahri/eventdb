{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Database.EventDB

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import qualified Data.ByteString.Lazy.Char8 as B
import Data.Monoid
import System.Environment

main :: IO ()
main = do
    (dir:_) <- getArgs

    withConnection dir id id $ \conn -> do
        stream <- openStream 0 conn
        evs' <- newTVarIO []
        _ <- forkIO $ forever $ do
            ev <- readEvent stream
            atomically $ modifyTVar evs' (\evs -> ev:evs)

        count <- fmap fromIntegral $ atomically $ eventCount conn
        waitFor (fmap ((==count) . length) $ readTVar evs')

        evs <- fmap reverse $ readTVarIO evs'
        print . getSum $ foldMap (Sum . readInteger . B.unpack . snd) evs

  where
    readInteger :: String -> Integer
    readInteger = read

    waitFor sPred = atomically $ do
        res <- sPred
        unless res retry
