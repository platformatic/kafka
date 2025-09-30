import { createRequire } from 'node:module'
import zlib from 'node:zlib'
import { UnsupportedCompressionError } from '../errors.ts'
import { DynamicBuffer } from './dynamic-buffer.ts'

const require = createRequire(import.meta.url)

const { zstdCompressSync, zstdDecompressSync, gzipSync, gunzipSync } = zlib

export type SyncCompressionPhase = (data: Buffer | DynamicBuffer) => Buffer
export type CompressionOperation = (data: Buffer) => Buffer

// Right now we support sync compressing only since the average time spent on compressing small sizes of
// data will be smaller than transferring the same data between threads to perform async (de)comporession.
// The interface naming already accounts for future expansion to async (de)compression.
export interface CompressionAlgorithmSpecification {
  compressSync: SyncCompressionPhase
  decompressSync: SyncCompressionPhase
  bitmask: number
  available?: boolean
}

export const CompressionAlgorithms = {
  NONE: 'none',
  GZIP: 'gzip',
  SNAPPY: 'snappy',
  LZ4: 'lz4',
  ZSTD: 'zstd'
} as const

export const allowedCompressionsAlgorithms = Object.values(CompressionAlgorithms) as CompressionAlgorithmValue[]
export type CompressionAlgorithm = keyof typeof CompressionAlgorithms
export type CompressionAlgorithmValue = (typeof CompressionAlgorithms)[keyof typeof CompressionAlgorithms]

function ensureBuffer (data: Buffer | DynamicBuffer): Buffer {
  return DynamicBuffer.isDynamicBuffer(data) ? (data as DynamicBuffer).slice() : (data as Buffer)
}

let snappyCompressSync: CompressionOperation | undefined
let snappyDecompressSync: CompressionOperation | undefined
let lz4CompressFrameSync: CompressionOperation | undefined
let lz4DecompressFrameSync: CompressionOperation | undefined

function loadSnappy () {
  try {
    const snappy = require('snappy')
    snappyCompressSync = snappy.compressSync
    snappyDecompressSync = snappy.uncompressSync
    /* c8 ignore next 5 - In tests snappy is always available */
  } catch (e) {
    throw new UnsupportedCompressionError(
      'Cannot load snappy module, which is an optionalDependency. Please check your local installation.'
    )
  }
}

function loadLZ4 () {
  try {
    const lz4 = require('lz4-napi')
    lz4CompressFrameSync = lz4.compressFrameSync
    lz4DecompressFrameSync = lz4.decompressFrameSync
    /* c8 ignore next 5 - In tests lz4-napi is always available */
  } catch (e) {
    throw new UnsupportedCompressionError(
      'Cannot load lz4-napi module, which is an optionalDependency. Please check your local installation.'
    )
  }
}

export const compressionsAlgorithms = {
  /* c8 ignore next 8 - 'none' is actually never used but this is to please Typescript */
  none: {
    compressSync (data: Buffer | DynamicBuffer): Buffer {
      return ensureBuffer(data)
    },
    decompressSync (data: Buffer | DynamicBuffer): Buffer {
      return ensureBuffer(data)
    },
    bitmask: 0,
    available: true
  },
  gzip: {
    compressSync (data: Buffer | DynamicBuffer): Buffer {
      return gzipSync(ensureBuffer(data))
    },
    decompressSync (data: Buffer | DynamicBuffer): Buffer {
      return gunzipSync(ensureBuffer(data)) as Buffer
    },
    bitmask: 1,
    available: true
  },
  snappy: {
    compressSync (data: Buffer | DynamicBuffer): Buffer {
      /* c8 ignore next 4 - In tests snappy is always available */
      if (!snappyCompressSync) {
        loadSnappy()
      }

      return snappyCompressSync!(ensureBuffer(data))
    },
    decompressSync (data: Buffer | DynamicBuffer): Buffer {
      /* c8 ignore next 4 - In tests snappy is always available */
      if (!snappyDecompressSync) {
        loadSnappy()
      }

      return snappyDecompressSync!(ensureBuffer(data)) as Buffer
    },
    bitmask: 2,
    available: true
  },
  lz4: {
    compressSync (data: Buffer | DynamicBuffer): Buffer {
      /* c8 ignore next 4 - In tests lz4-napi is always available */
      if (!lz4CompressFrameSync) {
        loadLZ4()
      }

      return lz4CompressFrameSync!(ensureBuffer(data))
    },
    decompressSync (data: Buffer | DynamicBuffer): Buffer {
      /* c8 ignore next 4 - In tests lz4-napi is always available */
      if (!lz4DecompressFrameSync) {
        loadLZ4()
      }

      return lz4DecompressFrameSync!(ensureBuffer(data))
    },
    bitmask: 3,
    available: true
  },
  zstd: {
    /* c8 ignore next 7 - Tests are only run on Node.js versions that support zstd */
    compressSync (data: Buffer | DynamicBuffer): Buffer {
      if (!zstdCompressSync) {
        throw new UnsupportedCompressionError('zstd is not supported in the current Node.js version')
      }

      return zstdCompressSync(ensureBuffer(data))
    },
    /* c8 ignore next 7 - Tests are only run on Node.js versions that support zstd */
    decompressSync (data: Buffer | DynamicBuffer): Buffer {
      if (!zstdCompressSync) {
        throw new UnsupportedCompressionError('zstd is not supported in the current Node.js version')
      }

      return zstdDecompressSync(ensureBuffer(data))
    },
    bitmask: 4,
    available: typeof zstdCompressSync === 'function'
  }
} as const

export const compressionsAlgorithmsByBitmask = Object.fromEntries(
  Object.values(compressionsAlgorithms).map(a => [a.bitmask, a])
)
