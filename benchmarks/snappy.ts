import { snappyCompress as wasmCompress, snappyDecompress as wasmDecompress } from '@platformatic/wasm-utils'
import cronometro from 'cronometro'
import { randomBytes } from 'node:crypto'
import { compressSync as nativeCompress, uncompressSync as nativeDecompress } from 'snappy'

const size = 1024

await cronometro(
  {
    native () {
      const value = randomBytes(size)
      const compressed = nativeCompress(value)
      return nativeDecompress(compressed)
    },
    wasm () {
      const value = randomBytes(size)
      const compressed = wasmCompress(value)
      return wasmDecompress(compressed)
    }
  },
  { print: { compare: true, compareMode: 'previous' } }
)
