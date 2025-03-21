import BufferList from 'bl'
import { createRequire } from 'node:module'
import zlib from 'node:zlib'
import { UnsupportedCompressionError } from '../errors.ts'

const require = createRequire(import.meta.url)

// @ts-ignore
const { zstdCompressSync, zstdDecompressSync, gzipSync, gunzipSync } = zlib

export type SyncCompressionPhase = (data: Buffer | BufferList) => Buffer
export type CompressionOperation = (data: Buffer) => Buffer

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

let snappyCompressSync: CompressionOperation | undefined
let snappyDecompressSync: CompressionOperation | undefined
let lz4CompressSync: CompressionOperation | undefined
let lz4DecompressSync: CompressionOperation | undefined

function loadSnappy () {
  try {
    const snappy = require('snappy')
    snappyCompressSync = snappy.compressSync
    snappyDecompressSync = snappy.uncompressSync
  } catch (e) {
    throw new UnsupportedCompressionError(
      'Cannot load lz4-napi module, which is an optionalDependency. Please check your local installation.'
    )
  }
}

function loadLZ4 () {
  try {
    const lz4 = require('lz4')
    lz4CompressSync = lz4.compressSync
    lz4DecompressSync = lz4.uncompressSync
  } catch (e) {
    throw new UnsupportedCompressionError(
      'Cannot load lz4-napi module, which is an optionalDependency. Please check your local installation.'
    )
  }
}

export const compressionsAlgorithms = {
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
      if (!snappyCompressSync) {
        loadSnappy()
      }

      return snappyCompressSync!(ensureBuffer(data))
    },
    decompressSync (data: Buffer | BufferList): Buffer {
      if (!snappyDecompressSync) {
        loadSnappy()
      }

      return snappyDecompressSync!(ensureBuffer(data)) as Buffer
    },
    bitmask: 2
  },
  lz4: {
    compressSync (data: Buffer | BufferList): Buffer {
      if (!lz4CompressSync) {
        loadLZ4()
      }

      return lz4CompressSync!(ensureBuffer(data))
    },
    decompressSync (data: Buffer | BufferList): Buffer {
      if (!lz4DecompressSync) {
        loadLZ4()
      }

      return lz4DecompressSync!(ensureBuffer(data))
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
