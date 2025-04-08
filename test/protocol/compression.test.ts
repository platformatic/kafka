import BufferList from 'bl'
import { strictEqual, throws } from 'node:assert'
import { createRequire } from 'node:module'
import test from 'node:test'
import zlib from 'node:zlib'
import { compressionsAlgorithms, compressionsAlgorithmsByBitmask } from '../../src/index.ts'

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

test('compression works with BufferList', () => {
  const bufferList = new BufferList(Buffer.from('test data in buffer list'))
  const compressed = compressionsAlgorithms.gzip.compressSync(bufferList)

  strictEqual(Buffer.isBuffer(compressed), true)

  const decompressed = compressionsAlgorithms.gzip.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data in buffer list')
})

test('gzip compression works correctly', () => {
  const data = Buffer.from('test data for compression')
  const compressed = compressionsAlgorithms.gzip.compressSync(data)

  strictEqual(compressed.equals(data), false)

  const decompressed = compressionsAlgorithms.gzip.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for compression')
})

test('snappy compression works correctly', { skip: !hasOptionalDependency('snappy') }, () => {
  const data = Buffer.from('test data for compression')
  const compressed = compressionsAlgorithms.snappy.compressSync(data)

  strictEqual(compressed.equals(data), false)

  const decompressed = compressionsAlgorithms.snappy.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for compression')
})

test('throws when snappy is not installed', { skip: hasOptionalDependency('snappy') }, () => {
  const data = Buffer.from('test data for compression')

  throws(() => {
    compressionsAlgorithms.snappy.compressSync(data)
  }, /Cannot load snappy module, which is an optionalDependency. Please check your local installation./)
})

test('lz4 compression works correctly', { skip: !hasOptionalDependency('lz4-napi') }, () => {
  const data = Buffer.from('test data for compression')
  const compressed = compressionsAlgorithms.lz4.compressSync(data)

  strictEqual(compressed.equals(data), false)

  const decompressed = compressionsAlgorithms.lz4.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for compression')
})

test('throws when snappy is not installed', { skip: hasOptionalDependency('lz4-napi') }, () => {
  const data = Buffer.from('test data for compression')

  throws(() => {
    compressionsAlgorithms.lz4.compressSync(data)
  }, /Cannot load lz4-napi module, which is an optionalDependency. Please check your local installation./)
})

test('zstd compression works correctly', { skip: !('zstdCompressSync' in zlib) }, () => {
  const data = Buffer.from('test data for compression')
  const compressed = compressionsAlgorithms.zstd.compressSync(data)

  strictEqual(compressed.equals(data), false)

  const decompressed = compressionsAlgorithms.zstd.decompressSync(compressed)
  strictEqual(decompressed.toString(), 'test data for compression')
})

test('throws when zstd is not available', { skip: 'zstdCompressSync' in zlib }, () => {
  const data = Buffer.from('test data for compression')

  throws(() => {
    compressionsAlgorithms.zstd.compressSync(data)
  }, /zstd is not supported in the current Node.js version/)
})
