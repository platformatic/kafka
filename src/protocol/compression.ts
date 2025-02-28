import BufferList from 'bl'
import { compressSync as lz4CompressSync, uncompressSync as lz4DecompressSync } from 'lz4-napi'
import zlib from 'node:zlib'
import { compressSync as snappyCompressSync, uncompressSync as snappyDecompressSync } from 'snappy'
import { UnsupportedCompressionError } from '../errors.ts'

// @ts-ignore
const { zstdCompressSync, zstdDecompressSync, gzipSync, gunzipSync } = zlib

export type SyncCompressionPhase = (data: Buffer | BufferList) => Buffer

// Right now we support sync compressing only since the average time spent on compressing small sizes of
// data will be smaller than transferring the same data between threads to perform async (de)comporession.
// The interface naming already accounts for future expansion to async (de)compression.
export interface CompressionAlgorithm {
  compressSync: SyncCompressionPhase
  decompressSync: SyncCompressionPhase
  bitmask: number
}

function ensureBuffer (data: Buffer | BufferList): Buffer {
  return BufferList.isBufferList(data) ? (data as BufferList).slice() : (data as Buffer)
}

export const compressionsAlgorithms: Record<string, CompressionAlgorithm> = {
  gzip: {
    compressSync (data: Buffer | BufferList): Buffer {
      return gzipSync(ensureBuffer(data))
    },
    decompressSync (data: Buffer | BufferList): Buffer {
      return gunzipSync(ensureBuffer(data)) as Buffer
    },
    bitmask: 1
  },
  snappy: {
    compressSync (data: Buffer | BufferList): Buffer {
      return snappyCompressSync(ensureBuffer(data))
    },
    decompressSync (data: Buffer | BufferList): Buffer {
      return snappyDecompressSync(ensureBuffer(data)) as Buffer
    },
    bitmask: 2
  },
  lz4: {
    compressSync (data: Buffer | BufferList): Buffer {
      return lz4CompressSync(ensureBuffer(data))
    },
    decompressSync (data: Buffer | BufferList): Buffer {
      return lz4DecompressSync(ensureBuffer(data))
    },
    bitmask: 3
  },
  zstd: {
    compressSync (data: Buffer | BufferList): Buffer {
      if (!zstdCompressSync) {
        throw new UnsupportedCompressionError('zstd is not supported in the current Node.js version')
      }

      return zstdCompressSync(ensureBuffer(data))
    },
    decompressSync (data: Buffer | BufferList): Buffer {
      if (!zstdCompressSync) {
        throw new UnsupportedCompressionError('zstd is not supported in the current Node.js version')
      }

      return zstdDecompressSync(ensureBuffer(data))
    },
    bitmask: 4
  }
} as const

export const compressionsAlgorithmsByBitmask = Object.fromEntries(
  Object.values(compressionsAlgorithms).map(a => [a.bitmask, a])
)

export type CompressionAlgorithms = keyof typeof compressionsAlgorithms | 'none'
