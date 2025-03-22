import BufferList from 'bl'
import { strictEqual, deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { UnsupportedCompressionError } from '../../src/errors.ts'
import {
  compressionsAlgorithms,
  compressionsAlgorithmsByBitmask,
  type CompressionAlgorithm
} from '../../src/protocol/compression.ts'

test('compressionsAlgorithms contains expected algorithms', () => {
  // Check that all expected compression algorithms are defined
  strictEqual(typeof compressionsAlgorithms.gzip, 'object')
  strictEqual(typeof compressionsAlgorithms.snappy, 'object')
  strictEqual(typeof compressionsAlgorithms.lz4, 'object')
  strictEqual(typeof compressionsAlgorithms.zstd, 'object')
})

test('compressionsAlgorithmsByBitmask has correct mapping', () => {
  // Verify bitmask mapping
  strictEqual(compressionsAlgorithmsByBitmask[1], compressionsAlgorithms.gzip)
  strictEqual(compressionsAlgorithmsByBitmask[2], compressionsAlgorithms.snappy)
  strictEqual(compressionsAlgorithmsByBitmask[3], compressionsAlgorithms.lz4)
  strictEqual(compressionsAlgorithmsByBitmask[4], compressionsAlgorithms.zstd)
})

test('gzip compression works correctly', () => {
  const data = Buffer.from('test data for compression')
  const compressed = compressionsAlgorithms.gzip.compressSync(data)
  
  // Compressed data should be different from original
  strictEqual(compressed.equals(data), false)
  
  // Decompression should restore original data
  const decompressed = compressionsAlgorithms.gzip.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for compression')
})

test('compression works with BufferList', () => {
  const bufferList = new BufferList(Buffer.from('test data in buffer list'))
  const compressed = compressionsAlgorithms.gzip.compressSync(bufferList)
  
  // Compressed data should be a Buffer
  strictEqual(Buffer.isBuffer(compressed), true)
  
  // Decompression should restore original data
  const decompressed = compressionsAlgorithms.gzip.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data in buffer list')
})

// This test is skipped because we don't want to rely on external modules in CI
test('zstd compression throws error if not supported', { skip: true }, () => {
  // Mock zstd not being available
  const originalZstdCompressSync = (globalThis as any).zstdCompressSync
  const originalZstdDecompressSync = (globalThis as any).zstdDecompressSync
  
  try {
    (globalThis as any).zstdCompressSync = undefined
    (globalThis as any).zstdDecompressSync = undefined
    
    const data = Buffer.from('test data for zstd compression')
    
    throws(() => {
      compressionsAlgorithms.zstd.compressSync(data)
    }, (err: any) => {
      return err instanceof UnsupportedCompressionError &&
        err.message.includes('zstd is not supported')
    })
    
    throws(() => {
      compressionsAlgorithms.zstd.decompressSync(data)
    }, (err: any) => {
      return err instanceof UnsupportedCompressionError &&
        err.message.includes('zstd is not supported')
    })
  } finally {
    // Restore
    (globalThis as any).zstdCompressSync = originalZstdCompressSync
    (globalThis as any).zstdDecompressSync = originalZstdDecompressSync
  }
})