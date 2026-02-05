import cronometro from 'cronometro'
import { compressFrameSync as nativeCompress, decompressFrameSync as nativeDecompress } from 'lz4-napi'
import { randomBytes } from 'node:crypto'
import { lz4Compress as wasmCompress, lz4Decompress as wasmDecompress } from '../src/protocol/native.ts'

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
