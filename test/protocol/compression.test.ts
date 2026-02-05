import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import { createRequire } from 'node:module'
import test from 'node:test'
import zlib from 'node:zlib'
import { compressionsAlgorithms, compressionsAlgorithmsByBitmask, DynamicBuffer } from '../../src/index.ts'

const require = createRequire(import.meta.url)

function hasOptionalDependency (name: string): boolean {
  try {
    require(name)
    return true
  } catch (e) {
    return false
  }
}

test('compressionsAlgorithms contains expected algorithms', () => {
  strictEqual(typeof compressionsAlgorithms.gzip, 'object')
  strictEqual(typeof compressionsAlgorithms.snappy, 'object')
  strictEqual(typeof compressionsAlgorithms.lz4, 'object')
  strictEqual(typeof compressionsAlgorithms.zstd, 'object')
})

test('compressionsAlgorithmsByBitmask has correct mapping', () => {
  strictEqual(compressionsAlgorithmsByBitmask[1], compressionsAlgorithms.gzip)
  strictEqual(compressionsAlgorithmsByBitmask[2], compressionsAlgorithms.snappy)
  strictEqual(compressionsAlgorithmsByBitmask[3], compressionsAlgorithms.lz4)
  strictEqual(compressionsAlgorithmsByBitmask[4], compressionsAlgorithms.zstd)
})

test('compression works with DynamicBuffer', () => {
  const input = new DynamicBuffer(Buffer.from('test data in buffer list'))
  const compressed = compressionsAlgorithms.gzip.compressSync(input)

  strictEqual(Buffer.isBuffer(compressed), true)

  const decompressed = compressionsAlgorithms.gzip.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data in buffer list')
})

test('gzip compression works correctly', () => {
  const input = Buffer.from('test data for compression')
  const validCompressed = Buffer.from(
    '1f8b08000000000000132b492d2e5148492c495448cb2f5248cecf2d284a2d2ececccf0300d2ea6ea619000000',
    'hex'
  )

  const compressed = compressionsAlgorithms.gzip.compressSync(input)
  deepStrictEqual(compressed, validCompressed)

  const decompressed = compressionsAlgorithms.gzip.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for compression')
})

test('snappy compression works correctly', () => {
  const input = Buffer.from('test data for compression')
  const validCompressed = Buffer.from('196074657374206461746120666f7220636f6d7072657373696f6e', 'hex')

  const compressed = compressionsAlgorithms.snappy.compressSync(input)
  deepStrictEqual(compressed, validCompressed)

  const decompressed = compressionsAlgorithms.snappy.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for compression')
})

test('lz4 compression works correctly', () => {
  const input = Buffer.from('test data for compression')
  const validCompressed = Buffer.from(
    '04224d186040821900008074657374206461746120666f7220636f6d7072657373696f6e00000000',
    'hex'
  )

  const compressed = compressionsAlgorithms.lz4.compressSync(input)
  deepStrictEqual(compressed, validCompressed)

  const decompressed = compressionsAlgorithms.lz4.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for compression')
})

test('zstd compression works correctly', () => {
  const input = Buffer.from('test data for compression')
  const validCompressed = Buffer.from('28b52ffd2019c9000074657374206461746120666f7220636f6d7072657373696f6e', 'hex')

  const compressed = compressionsAlgorithms.zstd.compressSync(input)
  deepStrictEqual(compressed, validCompressed)

  const decompressed = compressionsAlgorithms.zstd.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for compression')
})

test('throws when zstd is not available', { skip: 'zstdCompressSync' in zlib }, () => {
  const data = Buffer.from('test data for compression')

  throws(() => {
    compressionsAlgorithms.zstd.compressSync(data)
  }, /zstd is not supported in the current Node.js version/)
})
