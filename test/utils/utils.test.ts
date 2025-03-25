import BufferList from 'bl'
import { deepStrictEqual, strictEqual } from 'node:assert'
import { mock, test } from 'node:test'
import { ajv, debugDump, groupByProperty, humanize, niceJoin, setDebugDumpLogger } from '../../src/utils.ts'

test('ajv setup', () => {
  // Test bigint keyword validation
  const bigintSchema = {
    type: 'object',
    properties: {
      value: { bigint: true }
    }
  }

  const validate = ajv.compile(bigintSchema)
  strictEqual(validate({ value: 42n }), true)
  strictEqual(validate({ value: 42 }), false)

  // Test function keyword validation
  const functionSchema = {
    type: 'object',
    properties: {
      callback: { function: true }
    }
  }

  const validateFunc = ajv.compile(functionSchema)
  strictEqual(validateFunc({ callback: () => {} }), true)
  strictEqual(validateFunc({ callback: 'not a function' }), false)
})

test('niceJoin formats arrays properly', () => {
  // Empty array
  strictEqual(niceJoin([]), '')

  // Single item
  strictEqual(niceJoin(['apple']), 'apple')

  // Two items
  strictEqual(niceJoin(['apple', 'banana']), 'apple and banana')

  // Multiple items
  strictEqual(niceJoin(['apple', 'banana', 'cherry']), 'apple, banana and cherry')
  strictEqual(niceJoin(['one', 'two', 'three', 'four']), 'one, two, three and four')

  // Custom separators
  strictEqual(niceJoin(['apple', 'banana'], ' or '), 'apple or banana')
  strictEqual(niceJoin(['apple', 'banana', 'cherry'], ' or ', '; '), 'apple; banana or cherry')
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

  strictEqual(grouped.length, 3) // 3 different categories

  // Check that each group has the correct items
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

  // Test with empty array
  const emptyGrouped = groupByProperty([], 'category')
  deepStrictEqual(emptyGrouped, [])
})

test('humanize formats buffer contents correctly', () => {
  // Test with Buffer
  const buffer = Buffer.from([0xde, 0xad, 0xbe, 0xef, 0x12, 0x34])
  const result = humanize('Test Buffer', buffer)
  strictEqual(result, 'Test Buffer (6 bytes): dead beef 1234')

  // Test with BufferList
  const bl = new BufferList([Buffer.from([0xaa, 0xbb, 0xcc, 0xdd]), Buffer.from([0xee, 0xff])])
  const blResult = humanize('Test BufferList', bl)
  strictEqual(blResult, 'Test BufferList (6 bytes): aabb ccdd eeff')

  // Test with empty buffer
  const emptyBuffer = Buffer.alloc(0)
  const emptyResult = humanize('Empty Buffer', emptyBuffer)
  strictEqual(emptyResult, 'Empty Buffer (0 bytes): ')
})

test('debugDump logs value correctly', () => {
  const logger = mock.fn()
  setDebugDumpLogger(logger)

  // Test case 1: Single argument (value only)
  const testObject = { a: 1, b: 'test', c: [1, 2, 3] }
  debugDump(testObject)

  // Verify console.error was called with inspect output
  strictEqual(logger.mock.calls.length, 1)
  strictEqual(logger.mock.calls[0].arguments.length, 1)

  logger.mock.resetCalls()

  // Test case 2: Two arguments (label and value)
  const label = 'Test Object'
  debugDump(label, testObject)

  // Verify console.error was called with label and inspect output
  strictEqual(logger.mock.calls.length, 1)
  strictEqual(logger.mock.calls[0].arguments.length, 2)
  strictEqual(logger.mock.calls[0].arguments[0], label)
})
