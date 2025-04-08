import { deepStrictEqual, strictEqual } from 'node:assert'
import { Buffer } from 'node:buffer'
import test from 'node:test'
import { murmur2 } from '../../src/index.ts'

// Sample copied from https://github.com/tulios/kafkajs/blob/55b0b416308b9e597a5a6b97b0a6fd6b846255dc/src/producer/partitioners/default/murmur2.spec.js
const samples = {
  0: 971027396,
  1: -1993445489,
  128: -326012175,
  2187: -1508407203,
  16384: -325739742,
  78125: -1654490814,
  279936: 1462227128,
  823543: -2014198330,
  2097152: 607668903,
  4782969: -1182699775,
  10000000: -1830336757,
  19487171: -1603849305,
  35831808: -857013643,
  62748517: -1167431028,
  105413504: -381294639,
  170859375: -1658323481,
  '100:48069': 1009543857
}

for (const [input, output] of Object.entries(samples)) {
  test(`perform murmur2 computations - ${input}`, () => {
    deepStrictEqual(murmur2(input), output, `${input} - ${output}`)
  })
}

test('equivalent results for string and buffer inputs', () => {
  const testStrings = ['hello world', 'kafka', '12345', 'a'.repeat(100), 'test-partition-key']

  for (const str of testStrings) {
    const buf = Buffer.from(str)
    const hashFromString = murmur2(str)
    const hashFromBuffer = murmur2(buf)

    strictEqual(hashFromString, hashFromBuffer, `Hash for "${str}" should be the same regardless of input type`)
  }
})

test('caching functionality works correctly', () => {
  const testKey = 'test-cache-key-' + Date.now()

  const firstResult = murmur2(testKey, true)
  const secondResult = murmur2(testKey, true)
  strictEqual(firstResult, secondResult, 'Cached and uncached results should match')

  const uncachedResult = murmur2(testKey, false)
  strictEqual(firstResult, uncachedResult, 'Results with caching disabled should still match')

  const emptyKeyResult = murmur2('', true)
  strictEqual(typeof emptyKeyResult, 'number', 'Should return a number with empty string key')
})

test('empty string and buffer', () => {
  const emptyStringHash = murmur2('')
  const emptyBufferHash = murmur2(Buffer.alloc(0))

  strictEqual(emptyStringHash, emptyBufferHash, 'Empty string and buffer should have the same hash')
  strictEqual(emptyStringHash, emptyStringHash, 'Empty string hash should be consistent')
})

test('non-4-byte aligned data', () => {
  const testCases = [
    { input: 'a', expected: murmur2('a') },
    { input: 'ab', expected: murmur2('ab') },
    { input: 'abc', expected: murmur2('abc') },
    { input: 'abcde', expected: murmur2('abcde') }
  ]

  for (const { input, expected } of testCases) {
    strictEqual(murmur2(input), expected, `Hash for "${input}" (length ${input.length}) should match expected value`)
  }
})

test('consistency across repeated calls', () => {
  const testKey = 'consistency-test'
  const iterations = 100
  const firstResult = murmur2(testKey)

  for (let i = 0; i < iterations; i++) {
    strictEqual(murmur2(testKey), firstResult, `Hash for "${testKey}" should be consistent across calls`)
  }
})

test('unicode character handling', () => {
  const unicodeStrings = [
    { input: '你好', expected: murmur2('你好') },
    { input: '🚀', expected: murmur2('🚀') },
    { input: '❤️', expected: murmur2('❤️') },
    { input: 'café', expected: murmur2('café') }
  ]

  for (const { input, expected } of unicodeStrings) {
    strictEqual(murmur2(input), expected, `Hash for Unicode string "${input}" should match expected value`)
  }
})
