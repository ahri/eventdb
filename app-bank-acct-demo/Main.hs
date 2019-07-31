{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
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

newtype AcctHolder = AcctHolder String
newtype Balance = Balance Integer

data State f = State
    { holder  :: f AcctHolder
    , balance :: f Balance
    }

data Command
    = Rename String
    | Deposit Integer
    | Withdraw Integer

data Event
    = Renamed String
    | Deposited Integer
    | Withdrew Integer
    deriving (Show, Read)

main :: IO ()
main = do
    let dir = "/tmp/eventdb-bank-acct-demo"
    state1 <- initialState
    s1init <- showState state1

    putStrLn $ "Initialising " <> s1init <> " and creating DB in " <> show dir
    removePathForcibly dir
    conn <- openConnection dir

    putStrLn "Executing random commands concurrently..."
    mapConcurrently_
        (>>= transact conn state1)
        $ (replicate 10 $ randomCommand) <> [pure $ Rename "Jemima Schmidt"]

    -- store a representation of the original state
    s1 <- showState state1

    (Balance b) <- readTVarIO $ balance state1
    putStrLn $ "Resulting state: " <> s1
    when (b < 0) $ do
        putStrLn "ERROR: Balance should never be below zero"
        exitFailure

    state2 <- initialState
    s2init <- showState state2
    putStrLn $ "\nReplaying events against " <> s2init
    readEventsFrom 0 conn >>= traverse_
        (\(_idx, ev) -> do
            let ev' :: Event = read . C.unpack $ ev
            print ev'
            atomically $ apply state2 [ev']
        )

    -- store a representation of the replayed state
    s2 <- showState state2

    putStrLn "\nComparing states..."
    let cmp = s1 == s2

    putStrLn $ s1 <> " == " <> s2 <> " ~ " <> show cmp

    when (not cmp) $ do
        putStrLn "ERROR: States should always be equal"
        exitFailure

  where
    initialState = State <$> newTVarIO (AcctHolder "John Smith") <*> newTVarIO (Balance 0)

    randomCommand :: IO Command
    randomCommand = do
        cmd::Float <- randomIO
        val::Float <- randomIO

        let valMinus10ToPlus10 = (floor $ val * 20) - 10

        pure $ if cmd < 0.5
            then Deposit valMinus10ToPlus10
            else Withdraw valMinus10ToPlus10

    showState :: State TVar -> IO String
    showState state = do
        (AcctHolder h) <- readTVarIO $ holder state
        (Balance b)    <- readTVarIO $ balance state
        pure $ "State '" <> h <> "' " <> show b

transact :: Connection -> State TVar -> Command -> IO ()
transact conn state cmd = do
    evs <- atomically $ do
        result <- exec state cmd
        case result of
            Left _    -> pure [] -- in this case we're just ignoring errors
            Right evs -> do
                apply state evs
                pure $ evs

    writeEvents (fmap (C.pack . show) evs) conn >> pure ()

-- NB. all error checking happens here - isolation is in the control of client code
exec :: State TVar -> Command -> STM (Either String [Event])
exec state cmd = case cmd of
    Rename x        -> pure $ Right [Renamed x]

    Deposit x
        | x < 1     -> pure $ Left "Deposits of less than 1 are not allowed"
        | otherwise -> pure $ Right [Deposited x]

    Withdraw x -> do
        (Balance bal) <- readTVar $ balance state -- here we express a fine-grained dependency
        pure $ if | x > bal   -> Left "Insufficient balance"
                  | x < 1     -> Left "Withdrawls of less than 1 are not allowed"
                  | otherwise -> Right [Withdrew x]

-- NB. there is no error checking here; events are facts and must be applied
-- we can express fine-grained dependencies on only the data we need to change
apply :: State TVar -> [Event] -> STM ()
apply state evs = (flip traverse_) evs $ \case
    Renamed x   -> writeTVar (holder state) $ AcctHolder x

    Deposited x -> do
        (Balance bal) <- readTVar $ balance state
        writeTVar (balance state) $ Balance $ bal + x

    Withdrew  x -> do
        (Balance bal) <- readTVar $ balance state
        writeTVar (balance state) $ Balance $ bal - x
