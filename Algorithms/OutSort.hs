#!/usr/bin/env stack
-- stack script --resolver lts-9.2 --optimize
{-# LANGUAGE LambdaCase, OverloadedStrings, FlexibleContexts, BangPatterns, PackageImports, ScopedTypeVariables, RankNTypes #-}

module Algorithms.OutSort
    ( outsort
    , outsortStorable
    , isolateBySize
    , RIO
    ) where

import "temporary" System.IO.Temp (withSystemTempDirectory)

import Control.Monad
import Control.Monad.IO.Class
import qualified Data.Vector as V
import qualified Control.Concurrent.Async as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as BU
import qualified Data.ByteString.Internal as BI
import qualified Data.Vector.Storable as VS
import qualified Data.Vector.Storable.Mutable as VSM
import           Data.Vector.Algorithms.Intro (sortByBounds, selectByBounds)
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit as C

import           Control.Monad.Primitive
import           Data.Conduit ((.|))
import           Data.Conduit.Algorithms.Utils (awaitJust)
import           Data.Conduit.Algorithms.Async
import           Data.Conduit.Algorithms
import           System.FilePath
import           Control.Monad.Trans.Resource
import           Foreign
import           Foreign.Marshal.Utils (copyBytes)
import           System.IO.SafeWrite (withOutputFile)

type RIO = ResourceT IO

outsort :: forall a b. Ord a =>
            C.Conduit B.ByteString RIO a -- ^ decoder
            -> C.Conduit a RIO B.ByteString -- ^ encoder
            -> C.Conduit a RIO a -- ^ isolate a block
            -> C.Source RIO B.ByteString -- ^ initial input
            -> C.Sink B.ByteString RIO b -- ^ final output
            -> IO b
outsort reader writer chunk input output= withSystemTempDirectory "sort" $ \tdir -> do
        fs <- C.runConduitRes $
            input
                .| reader
                .| partials
                .| writePartials tdir (\fp vs ->
                        C.runConduitRes $
                            yieldV vs
                                .| writer
                                .| CB.sinkFileCautious fp)
        C.runConduitRes $
            mergeC [CB.sourceFile f .| reader | f <- fs]
                .| writer
                .| output
     where
        partials = do
            vs <- chunk .| CC.sinkVector
            unless (V.null vs) $ do
                vs' <- liftIO $ V.unsafeThaw vs
                liftIO $ sortParallel vs'
                C.yield vs'
                partials

yieldV :: forall a v. VGM.MVector v a => v (PrimState IO) a -> C.Producer RIO a
yieldV v =
    forM_ [0 .. VGM.length v - 1] $ \ix -> do
        (liftIO $ VGM.read v ix) >>= C.yield

writePartials :: forall a v. VGM.MVector v a => FilePath -> (FilePath -> (v (PrimState IO) a) -> IO ()) -> C.Sink (v (PrimState IO) a) RIO [FilePath]
writePartials tdir writer = do
        empty <- liftIO $ A.async (return ())
        writePartials' (0 :: Int) [] empty
    where

        writePartials' :: Int -> [FilePath] -> A.Async () -> C.Sink (v (PrimState IO) a) RIO [FilePath]
        writePartials' n fs prev = do
            next <- C.await
            liftIO $ A.wait prev
            case next of
                Nothing -> return fs
                Just vs -> do
                    let tnext = tdir </> show n <.> "temp"
                    wt <- liftIO . A.async $ writer tnext vs
                    writePartials' (n+1) (tnext:fs) wt

outsortStorable :: forall a. (Ord a, Storable a, Show a) =>
           a -- ^ dummy element, necessary for specifying type, can be undefined
           -> Int -- ^ number of elements per block
           -> C.Source RIO B.ByteString -- ^ initial input
           -> C.Sink B.ByteString RIO () -- ^ final output
           -> IO ()
outsortStorable _ nsize input output = withSystemTempDirectory "sort" $ \tdir -> do
        fs <- C.runConduitRes $
            input
                .| partialsStorable
                .| writePartials tdir writeStorable
        C.runConduitRes $
            mergeC [CB.sourceFile f .| readStorable | f <- fs]
                .| CC.conduitVector 256
                .| asyncMapC 16 encodeV
                .| output
     where
        writeStorable fp vs =
            withOutputFile fp $ \h ->
                VSM.unsafeWith vs $ \p ->
                    B.hPut h =<< (BU.unsafePackCStringFinalizer (castPtr p) (nBytes * VSM.length vs) (return ()))

        nBytes :: Int
        nBytes = sizeOf (undefined :: a)

        encodeV :: VS.Vector a -> B.ByteString
        encodeV v =
            BI.unsafeCreate (nBytes * VS.length v) $ \pd ->
                VS.unsafeWith v $ \ps ->
                    copyBytes pd (castPtr ps) (nBytes * VS.length v)

        readStorable :: C.Conduit B.ByteString RIO a
        readStorable = do
            vs <- decodeN 64
            unless (VSM.null vs) $ do
                yieldV vs
                readStorable
        decodeN :: Int -> C.Consumer B.ByteString RIO (VSM.MVector (PrimState IO) a)
        decodeN size = do
                vec <- liftIO (VSM.unsafeNew size)
                written <- CC.takeE (nBytes * size)
                            .| decodeN' (VSM.unsafeCast vec) 0
                return $! if written == nBytes * nsize
                    then vec
                    else VSM.slice 0 (written `div` nBytes) vec
            where
                decodeN' :: (VSM.MVector (PrimState IO) Word8) -> Int -> C.Consumer B.ByteString RIO Int
                decodeN' dest ix = C.await >>= \case
                    Nothing -> return ix
                    Just block -> do
                            liftIO (VSM.unsafeWith dest $ \pd ->
                                BU.unsafeUseAsCString block $ \ps ->
                                    copyBytes (pd `plusPtr` ix) ps (B.length block))
                            decodeN' dest (ix + B.length block)
        partialsStorable :: C.Conduit B.ByteString RIO (VSM.MVector (PrimState IO) a)
        partialsStorable = do
            vs <- decodeN nsize
            unless (VGM.null vs) $ do
                liftIO $ sortParallel vs
                C.yield vs
                partialsStorable


-- sort a vector in parallel threads
sortParallel :: forall a v. (Ord a, VGM.MVector v a) => v (PrimState IO) a -> IO ()
sortParallel v = sortPByBounds (0 :: Int) v 0 (VGM.length v)

--sortPByBounds :: (Ord e) => Int -> VM.IOVector e -> Int -> Int -> IO ()
sortPByBounds dep v start end
    | end - start < 8192 || dep > 10 = sortByBounds compare v start end
    | otherwise = do
        let k = (start + end) `div` 2
        selectByBounds compare v (k - start) start end
        A.concurrently_
            (sortPByBounds (dep + 1) v start k)
            (sortPByBounds (dep + 1) v k end)


isolateBySize :: Monad m => (a -> Int) -> Int -> C.Conduit a m a
isolateBySize sizer maxsize = awaitJust $ \next -> do
                    C.yield next
                    isolateBySize' (sizer next)
    where
        isolateBySize' seen = awaitJust $ \next ->
                if seen + sizer next < maxsize
                    then do
                        C.yield next
                        isolateBySize' $ seen + sizer next
                    else C.leftover next
