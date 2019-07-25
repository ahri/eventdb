{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import Database.EventDB

import qualified Data.ByteString.Lazy.Char8 as C
import Data.Foldable
import Control.Monad
import Control.Concurrent.STM
import Control.Concurrent.Async
import System.Directory
import System.Exit
import System.Random

data State = State
    { balance :: Integer
    } deriving (Show, Eq)

data Command
    = Deposit Integer
    | Withdraw Integer

data Event
    = Deposited Integer
    | Withdrew Integer
    deriving (Show, Read)

main :: IO ()
main = do
    let initialState = State 0
        dir = "/tmp/eventdb-bank-acct-demo"

    putStrLn $ "Initialising " <> show initialState <> " and creating DB in " <> show dir
    removePathForcibly dir
    conn <- openConnection dir
    state <- newTVarIO initialState

    putStrLn "Executing random commands concurrently..."
    replicateConcurrently_ 10 $ do
        cmd <- randomCommand
        evs <- atomically $ transact state cmd
        case evs of
            [] -> pure ()
            _  -> writeEvents (fmap (C.pack . show) evs) conn >> pure ()

    s <- readTVarIO state
    putStrLn $ "Resulting state: " <> show s
    when (balance s < 0) $ do
        putStrLn "ERROR: Balance should never be below zero"
        exitFailure

    putStrLn ""

    putStrLn $ "Replaying events against " <> show initialState
    s' <- readEventsFrom 0 conn >>= foldM
        (\s' (_idx, ev) -> do
            let ev' :: Event = read . C.unpack $ ev
            print ev'
            pure $ apply [ev'] s'
        )
        initialState

    putStrLn ""

    putStrLn "Comparing states..."
    let cmp = s == s'

    putStrLn $ show s <> " == " <> show s' <> " ~ " <> show cmp

    when (not cmp) $ do
        putStrLn "ERROR: States should always be equal"
        exitFailure

  where
    randomCommand :: IO Command
    randomCommand = do
        cmd::Float <- randomIO
        val::Float <- randomIO

        let valMinus10ToPlus10 = (floor $ val * 20) - 10

        pure $ if cmd < 0.5
            then Deposit valMinus10ToPlus10
            else Withdraw valMinus10ToPlus10

-- The STM monad allows us to express a custom transaction
transact :: TVar State -> Command -> STM [Event]
transact state cmd = do
    s <- readTVar state
    case exec cmd s of
        Left _    -> pure [] -- NB. through client validation we get fine-grained isolation
        Right evs -> do
            writeTVar state $ apply evs s
            pure $ evs

-- NB. all error checking happens here - isolation is in the control of client code
exec :: Command -> State -> Either String [Event]
exec cmd (State bal) = case cmd of
    Deposit x
        | x < 0     -> Left "Negative deposits are not allowed"
        | otherwise -> Right [Deposited x]

    Withdraw x
        | x > bal   -> Left "Insufficient balance"
        | x < 0     -> Left "Negative withdrawls are not allowed"
        | otherwise -> Right [Withdrew x]

-- NB. there is no error checking here; events are facts and must be applied
apply :: [Event] -> State -> State
apply evs state = foldl' f state evs
  where
    f (State bal) ev = case ev of
        Deposited x -> State $ bal + x
        Withdrew  x -> State $ bal - x
