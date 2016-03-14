module Main where

import           Leaflet.PubSub

import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import           Control.Concurrent.STM
import           Control.Monad
import           Data.List.Split
import           Data.Word

numMessages :: Int
numMessages = 1000 * 1000

messages :: [Int]
messages = [1..numMessages]

keys :: [Word32]
keys = [ 441017009, 767825404, 2832976589, 367491820, 109012360 ]

go :: IO ()
go = do
    listeners <- replicateM (length keys) (newListener 10000)
    router <- newRouter

    -- *****
    putStrLn "Set up the router and listeners"
    -- *****

    zipWithM_ (\t l -> atomically $ subscribe router t l) keys listeners

    -- *****
    putStrLn "Subscribed"
    -- *****

    let listenerAction listener = do
            l <- atomically (listen listener)
            case l of
                Booted ->
                    putStrLn "Booted"
                (Received Nothing) ->
                    putStrLn "Made it to the end"
                (Received (Just _)) ->
                    listenerAction listener
    waitingListeners <- mapM (async . listenerAction) listeners

    -- *****
    putStrLn "Async listeners"
    -- *****

    let writerAction topic =
            async $ do
                let splitMessages = chunksOf 1000 messages
                forM_ splitMessages $ \m -> do
                    mapM_ (atomically . publish router topic) (Just <$> m)
                    yield
                atomically (publish router topic Nothing)
    writers <- mapM writerAction keys

    -- *****
    putStrLn "Writers, now waiting"
    -- *****

    mapM_ wait (waitingListeners ++ writers)

main :: IO ()
main = async go >>= wait
