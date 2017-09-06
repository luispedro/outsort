#!/usr/bin/env stack
-- stack script --resolver lts-9.2 --optimize
{-# LANGUAGE LambdaCase, OverloadedStrings, FlexibleContexts, BangPatterns, PackageImports #-}

module OutSort
    ( outsort
    , isolateBySize
    ) where

import "temporary" System.IO.Temp (withSystemTempDirectory)

import System.IO
import Control.Monad
import Control.Monad.IO.Class
import qualified Data.Vector as V
import qualified Control.Concurrent.Async as A
import qualified Data.Vector.Algorithms.Heap as VA
import qualified Data.Vector.Mutable as VM
import           Data.Vector.Algorithms.Intro (sortByBounds)
import           Data.Vector.Algorithms.Search
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit as C
import           Control.Monad.Catch

import "Glob" System.FilePath.Glob

import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import           Data.Conduit ((.|))
import           Data.Conduit.Algorithms
import           System.FilePath
import           Control.Monad.Trans.Resource
import           Control.Concurrent (getNumCapabilities)

type RIO = ResourceT IO

outsort :: Ord a => C.Conduit B.ByteString RIO a -> C.Conduit a RIO B.ByteString -> C.Conduit a RIO a -> C.Source RIO B.ByteString -> C.Sink B.ByteString RIO b -> IO b
outsort reader writer chunk input output= withSystemTempDirectory "sort" $ \tdir -> do
        fs <- C.runConduitRes $
            input
                .| reader
                .| partials
                .| writePartials tdir
        C.runConduitRes $
            mergeC [CB.sourceFile f .| reader | f <- fs]
                .| writer
                .| output
     where
        writePartials tdir = writePartials' tdir 0 []
        writePartials' tdir n fs = C.await >>= \case
            Nothing -> return fs
            Just vs -> do
                let tnext = tdir </> show n <.> "temp"
                liftIO $ do
                    vs' <- V.unsafeFreeze vs
                    C.runConduitRes $
                        CC.yieldMany vs'
                            .| writer
                            .| CB.sinkFileCautious tnext
                writePartials' tdir (n+1) (tnext:fs)
        partials = do
            vs <- chunk .| CC.sinkVector
            unless (V.null vs) $ do
                vs' <- liftIO $ V.unsafeThaw vs
                liftIO $ sortParallel vs'
                C.yield vs'
                partials

-- sort a vector in parallel threads
sortParallel :: (Ord e) => VM.IOVector e -> IO ()
sortParallel v = do
    n <- getNumCapabilities
    sortPByBounds n v 0 (VM.length v)

sortPByBounds :: (Ord e) => Int -> VM.IOVector e -> Int -> Int -> IO ()
sortPByBounds 1 v start end = sortByBounds compare v start end
sortPByBounds threads v start end
    | end - start < 1024 = sortByBounds compare v start end
    | otherwise = do
        mid <- pivot v start end
        k <- VGM.unstablePartition (< mid) v
        let t1 :: Int
            t1
                | k - start < 1024 = 1
                | end - k < 1024 = threads - 1
                | otherwise = threads `div` 2
            t2 = threads - t1
        void $ A.concurrently
            (sortPByBounds t1 v start k)
            (sortPByBounds t2 v k end)

pivot v start end = do
        a <- VM.read v start
        b <- VM.read v (end - 1)
        c <- VM.read v ((start + end) `div` 2)
        return $! median a b c
    where
        median a b c = if a <= b
            then min b c
            else max a c


isolateBySize :: Monad m => (a -> Int) -> Int -> C.Conduit a m a
isolateBySize sizer maxsize = C.await >>= \case
            Nothing -> return ()
            Just next -> do
                    C.yield next
                    isolateBySize' (sizer next)
    where
        isolateBySize' seen = C.await >>= \case
            Nothing -> return ()
            Just next
                | seen + sizer next < maxsize -> do
                    C.yield next
                    isolateBySize' $ seen + sizer next
                | otherwise -> C.leftover next