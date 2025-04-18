import { deepStrictEqual, strictEqual } from 'node:assert'
import { mock, test } from 'node:test'
import {
  DynamicBuffer,
  NumericMap,
  ajv,
  debugDump,
  enumErrorMessage,
  groupByProperty,
  humanize,
  listErrorMessage,
  niceJoin,
  setDebugDumpLogger
} from '../src/index.ts'

test('ajv setup', () => {
  {
    const schema = {
      type: 'object',
      properties: {
        value: { bigint: true }
      }
    }

    const validate = ajv.compile(schema)
    strictEqual(validate({ value: 42n }), true)
    strictEqual(validate({ value: 42 }), false)
  }

  {
    const schema = {
      type: 'object',
      properties: {
        value: { map: true }
      }
    }

    const validate = ajv.compile(schema)
    strictEqual(validate({ value: new Map() }), true)
    strictEqual(validate({ value: 42 }), false)
  }

  {
    const schema = {
      type: 'object',
      properties: {
        callback: { function: true }
      }
    }

    const validate = ajv.compile(schema)
    strictEqual(validate({ callback: () => {} }), true)
    strictEqual(validate({ callback: 42 }), false)
  }

  {
    const schema = {
      type: 'object',
      properties: {
        value: { buffer: true }
      }
    }

    const validate = ajv.compile(schema)
    strictEqual(validate({ value: Buffer.alloc(0) }), true)
    strictEqual(validate({ value: 42 }), false)
  }
})

test('niceJoin formats arrays properly', () => {
  strictEqual(niceJoin([]), '')

  strictEqual(niceJoin(['apple']), 'apple')

  strictEqual(niceJoin(['apple', 'banana']), 'apple and banana')

  strictEqual(niceJoin(['apple', 'banana', 'cherry']), 'apple, banana and cherry')
  strictEqual(niceJoin(['one', 'two', 'three', 'four']), 'one, two, three and four')

  strictEqual(niceJoin(['apple', 'banana'], ' or '), 'apple or banana')
  strictEqual(niceJoin(['apple', 'banana', 'cherry'], ' or ', '; '), 'apple; banana or cherry')
})

test('listErrorMessage generates correct error messages', () => {
  strictEqual(listErrorMessage(['A', 'B', 'C']), 'should be one of A, B or C')
  strictEqual(listErrorMessage(['apple']), 'should be one of apple')
  strictEqual(listErrorMessage([]), 'should be one of ')
})

test('enumErrorMessage generates correct error messages', () => {
  const enumObject = { A: 1, B: 2, C: 3 }
  strictEqual(enumErrorMessage(enumObject), 'should be one of 1 (A), 2 (B) or 3 (C)')
  strictEqual(enumErrorMessage(enumObject, true), 'should be one of A, B or C')

  const emptyEnum = {}
  strictEqual(enumErrorMessage(emptyEnum), 'should be one of ')
  strictEqual(enumErrorMessage(emptyEnum, true), 'should be one of ')
})

test('groupByProperty groups array items by property', () => {
  const items = [
    { id: 1, category: 'A', name: 'Item 1' },
    { id: 2, category: 'B', name: 'Item 2' },
    { id: 3, category: 'A', name: 'Item 3' },
    { id: 4, category: 'C', name: 'Item 4' },
    { id: 5, category: 'B', name: 'Item 5' }
  ]

  const grouped = groupByProperty(items, 'category')

  strictEqual(grouped.length, 3)

  const categoryA = grouped.find(([key]) => key === 'A')
  const categoryB = grouped.find(([key]) => key === 'B')
  const categoryC = grouped.find(([key]) => key === 'C')

  deepStrictEqual(categoryA?.[0], 'A')
  deepStrictEqual(categoryA?.[1], [
    { id: 1, category: 'A', name: 'Item 1' },
    { id: 3, category: 'A', name: 'Item 3' }
  ])

  deepStrictEqual(categoryB?.[0], 'B')
  deepStrictEqual(categoryB?.[1], [
    { id: 2, category: 'B', name: 'Item 2' },
    { id: 5, category: 'B', name: 'Item 5' }
  ])

  deepStrictEqual(categoryC?.[0], 'C')
  deepStrictEqual(categoryC?.[1], [{ id: 4, category: 'C', name: 'Item 4' }])

  const emptyGrouped = groupByProperty([], 'category')
  deepStrictEqual(emptyGrouped, [])
})

test('humanize formats buffer contents correctly', () => {
  const buffer = Buffer.from([0xde, 0xad, 0xbe, 0xef, 0x12, 0x34])
  const result = humanize('Test Buffer', buffer)
  strictEqual(result, 'Test Buffer (6 bytes): dead beef 1234')

  const bl = new DynamicBuffer([Buffer.from([0xaa, 0xbb, 0xcc, 0xdd]), Buffer.from([0xee, 0xff])])
  const blResult = humanize('Test DynamicBuffer', bl)
  strictEqual(blResult, 'Test DynamicBuffer (6 bytes): aabb ccdd eeff')

  const emptyBuffer = Buffer.alloc(0)
  const emptyResult = humanize('Empty Buffer', emptyBuffer)
  strictEqual(emptyResult, 'Empty Buffer (0 bytes): ')
})

test('debugDump logs value correctly', () => {
  const logger = mock.fn()
  setDebugDumpLogger(logger)

  const testObject = { a: 1, b: 'test', c: [1, 2, 3] }
  debugDump(testObject)

  strictEqual(logger.mock.calls.length, 1)
  strictEqual(logger.mock.calls[0].arguments.length, 2)

  logger.mock.resetCalls()

  const label = 'Test Object'
  debugDump(label, testObject)

  strictEqual(logger.mock.calls.length, 1)
  strictEqual(logger.mock.calls[0].arguments.length, 3)
  strictEqual(logger.mock.calls[0].arguments[1], label)
})

test('NumericMap.getWithDefault gets value or fallback', () => {
  const map = new NumericMap()

  strictEqual(map.getWithDefault('key1', 42), 42)

  map.set('key1', 10)
  strictEqual(map.getWithDefault('key1', 42), 10)

  strictEqual(map.getWithDefault('nonexistent', 100), 100)
})

test('NumericMap.preIncrement increments and returns new value', () => {
  const map = new NumericMap()
  map.set('key3', 5)

  strictEqual(map.preIncrement('key1', 5, 0), 5)
  strictEqual(map.get('key1'), 5)

  strictEqual(map.preIncrement('key1', 3, 0), 8)
  strictEqual(map.get('key1'), 8)

  strictEqual(map.preIncrement('key2', 1, 10), 11)
  strictEqual(map.get('key2'), 11)

  strictEqual(map.preIncrement('key3', 1, 10), 6)
  strictEqual(map.get('key3'), 6)
})

test('NumericMap.postIncrement increments and returns old value', () => {
  const map = new NumericMap()
  map.set('key3', 5)

  strictEqual(map.postIncrement('key1', 5, 0), 0)
  strictEqual(map.get('key1'), 5)

  strictEqual(map.postIncrement('key1', 3, 0), 5)
  strictEqual(map.get('key1'), 8)

  strictEqual(map.postIncrement('key2', 1, 10), 10)
  strictEqual(map.get('key2'), 11)

  strictEqual(map.postIncrement('key3', 1, 10), 5)
  strictEqual(map.get('key3'), 6)
})
