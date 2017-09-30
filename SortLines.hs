#!/usr/bin/env stack
{- stack
    script
    --resolver lts-9.2
    --optimize
    --package vector
    --package conduit
    --package conduit-extra
    --package conduit-combinators
    --package bytestring
    --package vector-algorithms
    --package transformers
    --package containers
    --package temporary
    --package async
    --package exceptions
    --package Glob
    --package filepath
    --package resourcet
    --package conduit-algorithms-0.0.4.0
-}
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.Binary as CB

import Algorithms.OutSort (isolateBySize)
import Algorithms.SortMain (sortMain)

main :: IO ()
main = sortMain
    CB.lines
    CC.unlinesAscii
    (isolateBySize (const 1) 500000)
