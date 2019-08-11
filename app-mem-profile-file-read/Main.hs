{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Database.EventDB

import qualified Data.ByteString.Lazy.Char8 as B
import Data.Monoid
import System.Environment

main :: IO ()
main = do
    (dir:_) <- getArgs

    withConnection dir singleElementWriteBuffer $ \conn -> do
        (evs, _) <- readEvents 0 conn
        print . getSum $ foldMap (Sum . readInteger . B.unpack . snd) evs

  where
    readInteger :: String -> Integer
    readInteger = read

    singleElementWriteBuffer = 1
