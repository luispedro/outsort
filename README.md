# Outsort: generic (Haskell-based) external sorting


Example

```haskell
    import qualified Data.Conduit.Combinators as CC
    import qualified Data.Conduit.Binary as CB

    import Algorithms.OutSort (isolateBySize)
    import Algorithms.SortMain (sortMain)

    main :: IO ()
    main = sortMain
        CB.lines
        CC.unlinesAscii
        (isolateBySize (const 1) 500000)
```

All that is needed is a decoder (`ConduitT ByteString a m ()`), an encoder
(`ConduitT ByteString a m ()`), and a function to split the input into blocks
(`ConduitT a a m ()`). Given these elements, the result is a programme which can
sort arbitrarily large inputs using external memory.

Licence: MIT

Author: [Luis Pedro Coelho](http://luispedro.org) (email:
[coelho@embl.de](mailto:coelho@embl.de)) (on twitter:
[@luispedrocoelho](https://twitter.com/luispedrocoelho))

