import { DynamicBuffer } from '@platformatic/dynamic-buffer'
import { lz4Compress, lz4Decompress, snappyCompress, snappyDecompress } from '@platformatic/wasm-utils'
import zlib from 'node:zlib'
import { UnsupportedCompressionError } from '../errors.ts'

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
  return DynamicBuffer.isDynamicBuffer(data) ? (data as DynamicBuffer).buffer : (data as Buffer)
}

const snappyCompressSync: CompressionOperation = snappyCompress
const lz4CompressFrameSync: CompressionOperation = lz4Compress
const lz4DecompressFrameSync: CompressionOperation = lz4Decompress
const xerialSnappyHeader = Buffer.from([0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00])

function snappyDecompressSync (data: Buffer): Buffer {
  if (!data.subarray(0, xerialSnappyHeader.length).equals(xerialSnappyHeader)) {
    return snappyDecompress(data)
  }

  const decompressed = new DynamicBuffer()
  let offset = 16

  while (offset < data.length) {
    /* c8 ignore next 3 - Hard to test */
    if (offset + 4 > data.length) {
      throw new Error('Invalid xerial snappy chunk length')
    }

    const chunkLength = data.readUInt32BE(offset)
    offset += 4

    /* c8 ignore next 3 - Hard to test */
    if (offset + chunkLength > data.length) {
      throw new Error('Invalid xerial snappy chunk data')
    }

    decompressed.append(snappyDecompress(data.subarray(offset, offset + chunkLength)))
    offset += chunkLength
  }

  return decompressed.buffer
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
      return snappyCompressSync(ensureBuffer(data))
    },
    decompressSync (data: Buffer | DynamicBuffer): Buffer {
      return snappyDecompressSync(ensureBuffer(data)) as Buffer
    },
    bitmask: 2,
    available: true
  },
  lz4: {
    compressSync (data: Buffer | DynamicBuffer): Buffer {
      return lz4CompressFrameSync(ensureBuffer(data))
    },
    decompressSync (data: Buffer | DynamicBuffer): Buffer {
      return lz4DecompressFrameSync(ensureBuffer(data))
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
