module Main where

import Lib

import Control.Monad
import Control.Concurrent.Async
import Control.Concurrent.MVar
import Control.Concurrent.STM

numMessages :: Int
numMessages = 1000 * 1000 * 1000

messages :: [Int]
messages = [1..numMessages]

go :: IO ()
go = do
    queue <- newTBQueueIO 100 :: IO (TBQueue Int)
    let producer = async (mapM_ (atomically . writeTBQueue queue) messages)
        consumer = async (replicateM_ numMessages (atomically (readTBQueue queue)))
    void (concurrently producer consumer)

main :: IO ()
main = async go >>= wait
