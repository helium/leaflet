{-# LANGUAGE RecordWildCards #-}

module Leaflet.PubSub
    ( Listener(..)
    , Listen(..)
    , Router(..)
    , newListener
    , listen
    , newRouter
    , publish
    , subscribe
    ) where

import           Control.Concurrent.STM
import           Control.Monad
import           Data.Hashable
import           Data.Maybe             (fromMaybe)
import           Data.Set               (Set, delete, empty, insert, size,
                                         toList)
import           Data.Unique
import qualified STMContainers.Map      as STMMap

data Listener val = Listener
    { _queue  :: TBQueue val
    , _booted :: TMVar ()
    , _uniq   :: Unique
    }

instance Show (Listener val) where
    show (Listener _ _ u) = "Listener " ++ show (hashUnique u)

instance Eq (Listener val) where
    a == b = _uniq a == _uniq b

instance Ord (Listener val) where
    compare a b = compare (_uniq a) (_uniq b)

newListener :: Int -> IO (Listener val)
newListener bufSize = Listener
                        <$> newTBQueueIO bufSize
                        <*> newEmptyTMVarIO
                        <*> newUnique

data Listen val
    = Received val
    | Booted
    deriving (Show)

listen :: Listener val -> STM (Listen val)
listen Listener{..} =
    (Received <$> readTBQueue _queue)
        `orElse`
    (takeTMVar _booted >> pure Booted)

data PushListenerResult = PushSuccess | PushFull
    deriving (Show, Eq)

pushListener :: Listener val -> val -> STM PushListenerResult
pushListener Listener{..} val =
    (writeTBQueue _queue val >> pure PushSuccess)
        `orElse`
    pure PushFull

data Router topic val
    = Router (STMMap.Map topic (Set (Listener val)))

newRouter :: IO (Router topic val)
newRouter = Router <$> STMMap.newIO

publish
    :: STMMap.Key topic
    => Router topic val
    -> topic
    -> val
    -> STM ()
publish (Router router) topic val = do
    mListeners <- STMMap.lookup topic router
    case mListeners of
        (Just listeners) -> do
            pushResults <- mapM (`pushListener` val) (toList listeners)
            let resultWithListener = zip (toList listeners) pushResults
                needsRemoving = fst <$> filter (\(_,r) -> r == PushFull) resultWithListener
                deleted = foldr delete listeners needsRemoving
            STMMap.insert deleted topic router
            mapM_ (flip putTMVar () . _booted) needsRemoving
        Nothing ->
            return ()

subscribe
    :: (Show topic, STMMap.Key topic)
    => Router topic val
    -> topic
    -> Listener val
    -> STM ()
subscribe (Router router) topic newListener = do
    listeners <- fromMaybe empty <$> STMMap.lookup topic router
    let newListeners = insert newListener listeners
    STMMap.insert newListeners topic router
