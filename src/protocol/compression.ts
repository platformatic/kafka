import { compress as moduleZstdCompress, decompress as moduleZstdDecompress } from '@mongodb-js/zstd'
import BufferList from 'bl'
import { compress as lz4Compress, uncompress as lz4Decompress } from 'lz4-napi'
import { promisify } from 'node:util'
import zlib from 'node:zlib'
import { compress as snappyCompress, uncompress as snappyDecompress } from 'snappy'

const gzipCompress = promisify(zlib.gzip)
const gzipDecompress = promisify(zlib.gunzip)
// @ts-ignore
const zstdCompress = zlib.zstdCompress ? promisify(zlib.zstdCompress) : moduleZstdCompress
// @ts-ignore
const zstdDecompress = zlib.zstdDecompress ? promisify(zlib.zstdDecompress) : moduleZstdDecompress

export interface CompressionAlgorithm {
  encode(data: Buffer | BufferList): Promise<Buffer>
  decode(data: Buffer | BufferList): Promise<Buffer>
  bitmask: number
}

function ensureBuffer (data: Buffer | BufferList): Buffer {
  return BufferList.isBufferList(data) ? (data as BufferList).slice() : (data as Buffer)
}

export const compressionsAlgorithms = {
  gzip: {
    encode (data: Buffer | BufferList): Promise<Buffer> {
      return gzipCompress(ensureBuffer(data))
    },
    decode (data: Buffer | BufferList): Promise<Buffer> {
      return gzipDecompress(ensureBuffer(data))
    },
    bitmask: 1
  },
  snappy: {
    encode (data: Buffer | BufferList): Promise<Buffer> {
      return snappyCompress(ensureBuffer(data))
    },
    decode (data: Buffer | BufferList): Promise<Buffer> {
      return snappyDecompress(ensureBuffer(data)) as Promise<Buffer>
    },
    bitmask: 2
  },
  lz4: {
    encode (data: Buffer | BufferList): Promise<Buffer> {
      return lz4Compress(ensureBuffer(data))
    },
    decode (data: Buffer | BufferList): Promise<Buffer> {
      return lz4Decompress(ensureBuffer(data))
    },
    bitmask: 3
  },
  zstd: {
    encode (data: Buffer | BufferList): Promise<Buffer> {
      return zstdCompress(ensureBuffer(data))
    },
    decode (data: Buffer | BufferList): Promise<Buffer> {
      return zstdDecompress(ensureBuffer(data))
    },
    bitmask: 4
  }
} as const

export const compressionsAlgorithmsByBitmask = Object.fromEntries(
  Object.values(compressionsAlgorithms).map(a => [a.bitmask, a])
)

export type CompressionAlgorithms = keyof typeof compressionsAlgorithms | 'none'
