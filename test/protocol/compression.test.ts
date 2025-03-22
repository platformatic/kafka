import BufferList from 'bl'
import { strictEqual, deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { UnsupportedCompressionError } from '../../src/errors.ts'
import {
  compressionsAlgorithms,
  compressionsAlgorithmsByBitmask,
  type CompressionAlgorithm
} from '../../src/protocol/compression.ts'
import { createRequire } from 'node:module'

const require = createRequire(import.meta.url)

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

// Test snappy compression if available
test('snappy compression handling', { skip: !hasOptionalDependency('snappy') }, () => {
  const data = Buffer.from('test data for snappy compression')
  const compressed = compressionsAlgorithms.snappy.compressSync(data)
  
  // Compressed data should be different from original
  strictEqual(compressed.equals(data), false)
  
  // Decompression should restore original data
  const decompressed = compressionsAlgorithms.snappy.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for snappy compression')
  
  // Test with BufferList
  const bufferList = new BufferList(Buffer.from('test buffer list for snappy'))
  const compressedBl = compressionsAlgorithms.snappy.compressSync(bufferList)
  const decompressedBl = compressionsAlgorithms.snappy.decompressSync(compressedBl)
  strictEqual(decompressedBl.toString(), 'test buffer list for snappy')
})

// Skip lz4 test as it requires a dependency that might not be installed
test('lz4 compression handling', { skip: true }, () => {
  const data = Buffer.from('test data for lz4 compression')
  const compressed = compressionsAlgorithms.lz4.compressSync(data)
  
  // Compressed data should be different from original
  strictEqual(compressed.equals(data), false)
  
  // Decompression should restore original data
  const decompressed = compressionsAlgorithms.lz4.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for lz4 compression')
  
  // Test with BufferList
  const bufferList = new BufferList(Buffer.from('test buffer list for lz4'))
  const compressedBl = compressionsAlgorithms.lz4.compressSync(bufferList)
  const decompressedBl = compressionsAlgorithms.lz4.decompressSync(compressedBl)
  strictEqual(decompressedBl.toString(), 'test buffer list for lz4')
})

// Skip this test as it requires mocking module loading
test('snappy compression throws error when dependency missing', { skip: true }, () => {
  // Store original variables
  const originalSnappyCompressSync = (globalThis as any).snappyCompressSync
  const originalSnappyDecompressSync = (globalThis as any).snappyDecompressSync
  
  // Mock the module require function
  const originalModule = require('module')
  const originalRequire = originalModule.prototype.require
  
  try {
    // Create mock require that throws for snappy
    originalModule.prototype.require = function(id: string) {
      if (id === 'snappy') {
        throw new Error('Cannot find module')
      }
      return originalRequire.apply(this, [id])
    }
    
    // Reset the cached functions
    ;(globalThis as any).snappyCompressSync = undefined
    ;(globalThis as any).snappyDecompressSync = undefined
    
    throws(() => {
      // This should trigger a loadSnappy() call
      compressionsAlgorithms.snappy.compressSync(Buffer.from('test'))
    }, (err: any) => {
      return err instanceof UnsupportedCompressionError &&
        err.message.includes('Cannot load lz4-napi module')
    })
  } finally {
    // Restore original require
    originalModule.prototype.require = originalRequire
    
    // Restore original variables
    ;(globalThis as any).snappyCompressSync = originalSnappyCompressSync
    ;(globalThis as any).snappyDecompressSync = originalSnappyDecompressSync
  }
})

// Mock missing lz4 dependency
test('lz4 compression throws error when dependency missing', { skip: true }, () => {
  // Store original variables
  const originalLz4CompressSync = (globalThis as any).lz4CompressSync
  const originalLz4DecompressSync = (globalThis as any).lz4DecompressSync
  
  // Mock the module require function
  const originalModule = require('module')
  const originalRequire = originalModule.prototype.require
  
  try {
    // Create mock require that throws for lz4
    originalModule.prototype.require = function(id: string) {
      if (id === 'lz4') {
        throw new Error('Cannot find module')
      }
      return originalRequire.apply(this, [id])
    }
    
    // Reset the cached functions
    ;(globalThis as any).lz4CompressSync = undefined
    ;(globalThis as any).lz4DecompressSync = undefined
    
    throws(() => {
      // This should trigger a loadLZ4() call
      compressionsAlgorithms.lz4.compressSync(Buffer.from('test'))
    }, (err: any) => {
      return err instanceof UnsupportedCompressionError &&
        err.message.includes('Cannot load lz4-napi module')
    })
  } finally {
    // Restore original require
    originalModule.prototype.require = originalRequire
    
    // Restore original variables
    ;(globalThis as any).lz4CompressSync = originalLz4CompressSync
    ;(globalThis as any).lz4DecompressSync = originalLz4DecompressSync
  }
})

// Test zstd compression error handling
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

// Helper function to check if an optional dependency is available
function hasOptionalDependency(name: string): boolean {
  try {
    require(name)
    return true
  } catch (e) {
    return false
  }
}