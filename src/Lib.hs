{-# LANGUAGE RecordWildCards   #-}

module Lib
    ( Listener(..)
    , Router(..)
    , newListener
    , newRouter
    , publish
    , subscribe
    ) where

import Control.Concurrent.STM
import Data.Hashable
import Data.Unique
import Data.Maybe (fromMaybe)
import Data.Set (Set, toList, delete, empty, insert)
import qualified STMContainers.Map as STMMap

data Listener val = Listener
    { _queue :: TBQueue val
    , _uniq  :: Unique
    }

instance Eq (Listener val) where
    a == b = _uniq a == _uniq b

instance Ord (Listener val) where
    compare a b = compare (_uniq a) (_uniq b)

newListener :: Int -> IO (Listener val)
newListener size = Listener <$> newTBQueueIO size <*> newUnique

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
    :: (Eq topic, Hashable topic)
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
        Nothing ->
            return ()

subscribe
    :: (Eq topic, Hashable topic)
    => Router topic val
    -> topic
    -> Listener val
    -> STM ()
subscribe (Router router) topic newListener = do
    listeners <- fromMaybe empty <$> STMMap.lookup topic router
    let newListeners = insert newListener listeners
    STMMap.insert newListeners topic router
