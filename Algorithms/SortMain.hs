{-# LANGUAGE LambdaCase, OverloadedStrings, FlexibleContexts, BangPatterns, PackageImports #-}

module Algorithms.SortMain
    ( sortMain
    , sortMain'
    , mergeMain
    , mergeMain'
    ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL
import qualified Data.Conduit.Binary as CB
import qualified Data.Conduit.Algorithms as CAlg
import           Control.Concurrent (setNumCapabilities)
import           Control.Monad.IO.Class (liftIO)
import           Data.Conduit ((.|))
import           System.Console.GetOpt
import           System.Environment (getArgs)
import           Data.List (foldl')
import           Safe (atDef)
import           System.Exit (exitFailure)
import           Control.Monad.Trans.Resource (MonadResource)
import           Control.Monad.IO.Class (MonadIO)

import Algorithms.OutSort


readAllFiles :: (MonadIO m, MonadResource m) =>
                        Bool -- ^ verbose
                        -> [FilePath] -- ^ file list
                        -> C.Source m B.ByteString
readAllFiles False ifiles = sequence_ (map CB.sourceFile ifiles)
readAllFiles True  ifiles = readAllFiles' (zip [(1::Int)..] ifiles)
    where
        n = length ifiles
        readAllFiles' [] = return ()
        readAllFiles' ((i,f):rest) = do
            liftIO $ putStrLn ("Reading file "++f++" ("++show i++"/"++show n++")")
            CB.sourceFile f
            readAllFiles' rest

data CmdArgs = CmdArgs
                { optIFile :: FilePath
                , optOFile :: FilePath
                , optIFileList :: FilePath
                , optVerbose :: Bool
                , nJobs :: Int
                } deriving (Eq, Show)

data CmdFlags = OutputFile FilePath
                | InputFile FilePath
                | ListFile FilePath
                | NJobs Int
                | Verbose
                deriving (Eq, Show)

options :: [OptDescr CmdFlags]
options =
    [ Option ['v'] ["verbose"] (NoArg Verbose)  "verbose mode"
    , Option ['i'] ["input"] (ReqArg InputFile "FILE") "Input file"
    , Option ['F'] ["file-list"] (ReqArg ListFile "FILE") "Input is a list of files"
    , Option ['o'] ["output"] (ReqArg OutputFile "FILE") "Output file"
    , Option ['j'] ["threads", "jobs"] (ReqArg (NJobs . read) "INT") "Nr threads"
    ]


parseArgs :: [String] -> CmdArgs
parseArgs argv = foldl' p (CmdArgs ifile ofile "" False 1) flags
    where
        (flags, args, _extraOpts) = getOpt Permute options argv
        ifile = atDef "" args 0
        ofile = atDef "" args 1

        p c (OutputFile o) = c { optOFile = o }
        p c (InputFile i) = c { optIFile = i }
        p c (ListFile f) = c { optIFileList = f }
        p c (NJobs n) = c { nJobs = n }
        p c Verbose = c { optVerbose = True }

extractIFiles :: CmdArgs -> IO [FilePath]
extractIFiles opts = case (optIFile opts, optIFileList opts) of
    (ifile, "") -> return [ifile]
    ("", ffile) -> C.runConduitRes $
                        CB.sourceFile ffile
                            .| CB.lines
                            .| CL.map B8.unpack
                            .| CL.consume
    _ -> do
        putStrLn "Cannot pass both input file and -F argument"
        exitFailure


sortMain' :: Ord a =>
            [String] -- ^ command line arguments
            -> C.Conduit B.ByteString RIO a -- ^ decoder
            -> C.Conduit a RIO B.ByteString -- ^ encoder
            -> C.Conduit a RIO a -- ^ isolate a block
            -> IO ()
sortMain' args decoder encoder isolator = do
    let opts = parseArgs args
        nthreads = nJobs opts
    setNumCapabilities nthreads
    ifiles <- extractIFiles opts
    outsort
        decoder
        encoder
        isolator
        (readAllFiles (optVerbose opts) ifiles)
        (CB.sinkFileCautious $ optOFile opts)


-- | Simple main function
sortMain :: Ord a =>
                C.Conduit B8.ByteString RIO a
                -> C.Conduit a RIO B8.ByteString
                -> C.Conduit a RIO a
                -> IO ()
sortMain decoder encoder isolator = do
    args <- getArgs
    sortMain' args decoder encoder isolator

mergeMain' :: Ord a =>
            [String] -- ^ command line arguments
            -> C.Conduit B.ByteString RIO a -- ^ decoder
            -> C.Conduit a RIO B.ByteString -- ^ encoder
            -> IO ()
mergeMain' args decoder encoder = do
    let opts = parseArgs args
        nthreads = nJobs opts
    setNumCapabilities nthreads
    ifiles <- extractIFiles opts
    C.runConduitRes $
        CAlg.mergeC [(CB.sourceFile f .| decoder) | f <- ifiles]
        .| encoder
        .| CB.sinkFileCautious (optOFile opts)

mergeMain :: Ord a =>
                C.Conduit B8.ByteString RIO a -- ^ decoder
                -> C.Conduit a RIO B8.ByteString -- ^ encoder
                -> IO ()
mergeMain decoder encoder = do
    args <- getArgs
    mergeMain' args decoder encoder
