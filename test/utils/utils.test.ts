import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import { afterEach, mock, test } from 'node:test'
import BufferList from 'bl'
import { setTimeout as sleep } from 'node:timers/promises'
import { ajv, niceJoin, groupByProperty, inspectBuffer, invokeAPIWithRetry } from '../../src/utils.ts'

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
  deepStrictEqual(categoryC?.[1], [
    { id: 4, category: 'C', name: 'Item 4' }
  ])

  // Test with empty array
  const emptyGrouped = groupByProperty([], 'category')
  deepStrictEqual(emptyGrouped, [])
})

test('inspectBuffer formats buffer contents correctly', () => {
  // Test with Buffer
  const buffer = Buffer.from([0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34])
  const result = inspectBuffer('Test Buffer', buffer)
  strictEqual(result, 'Test Buffer (6 bytes): dead beef 1234')

  // Test with BufferList
  const bl = new BufferList([Buffer.from([0xAA, 0xBB, 0xCC, 0xDD]), Buffer.from([0xEE, 0xFF])])
  const blResult = inspectBuffer('Test BufferList', bl)
  strictEqual(blResult, 'Test BufferList (6 bytes): aabb ccdd eeff')

  // Test with empty buffer
  const emptyBuffer = Buffer.alloc(0)
  const emptyResult = inspectBuffer('Empty Buffer', emptyBuffer)
  strictEqual(emptyResult, 'Empty Buffer (0 bytes): ')
})

test('invokeAPIWithRetry retries operations on retryable errors', async () => {
  const operationId = 'test-operation'
  let attempts = 0

  // Mock operation that fails twice then succeeds
  const operation = mock.fn(() => {
    attempts++
    if (attempts < 3) {
      const error = new Error('Temporary failure')
      // @ts-ignore: Setting custom properties on error for testing
      error.cause = {
        errors: [{ canRetry: true }]
      }
      throw error
    }
    return 'success'
  })

  const onFailedAttempt = mock.fn()
  const beforeFailure = mock.fn()

  // Mock sleep to make test faster
  const originalSleep = sleep
  global.setTimeout = mock.fn(() => Promise.resolve()) as any

  try {
    const result = await invokeAPIWithRetry(
      operationId,
      operation,
      0,
      3,
      10,
      onFailedAttempt,
      beforeFailure
    )

    strictEqual(result, 'success')
    strictEqual(operation.mock.calls.length, 3)
    strictEqual(onFailedAttempt.mock.calls.length, 2)
    strictEqual(beforeFailure.mock.calls.length, 0)
  } finally {
    // Restore original sleep
    global.setTimeout = originalSleep
  }
})

test('invokeAPIWithRetry fails after max attempts', async () => {
  const operationId = 'test-operation'
  
  // Mock operation that always fails with retryable error
  const operation = mock.fn(() => {
    const error = new Error('Temporary failure')
    // @ts-ignore: Setting custom properties on error for testing
    error.cause = {
      errors: [{ canRetry: true }]
    }
    throw error
  })

  const onFailedAttempt = mock.fn()
  const beforeFailure = mock.fn()

  // Mock console.log to prevent output during test
  const originalConsoleLog = console.log
  console.log = mock.fn()
  
  // Mock sleep to make test faster
  const originalSleep = sleep
  global.setTimeout = mock.fn(() => Promise.resolve()) as any

  try {
    let caughtError: Error | undefined
    try {
      await invokeAPIWithRetry(
        operationId,
        operation,
        0,
        2, // Max attempts = 2
        10,
        onFailedAttempt,
        beforeFailure
      )
    } catch (error) {
      caughtError = error as Error
    }

    strictEqual(caughtError?.message, 'Temporary failure')
    strictEqual(operation.mock.calls.length, 3) // Initial + 2 retries
    strictEqual(onFailedAttempt.mock.calls.length, 2)
    strictEqual(beforeFailure.mock.calls.length, 1)
    strictEqual((console.log as any).mock.calls.length, 1) // Log message about failure
  } finally {
    console.log = originalConsoleLog
    global.setTimeout = originalSleep
  }
})

test('invokeAPIWithRetry does not retry on non-retryable errors', async () => {
  const operationId = 'test-operation'
  
  // Mock operation that fails with non-retryable error
  const operation = mock.fn(() => {
    const error = new Error('Permanent failure')
    // @ts-ignore: Setting custom properties on error for testing
    error.cause = {
      errors: [{ canRetry: false }]
    }
    throw error
  })

  const onFailedAttempt = mock.fn()
  const beforeFailure = mock.fn()

  let caughtError: Error | undefined
  try {
    await invokeAPIWithRetry(
      operationId,
      operation,
      0,
      3,
      10,
      onFailedAttempt,
      beforeFailure
    )
  } catch (error) {
    caughtError = error as Error
  }

  strictEqual(caughtError?.message, 'Permanent failure')
  strictEqual(operation.mock.calls.length, 1) // No retries
  strictEqual(onFailedAttempt.mock.calls.length, 0)
  strictEqual(beforeFailure.mock.calls.length, 0)
})

test('invokeAPIWithRetry handles errors without cause property', async () => {
  const operationId = 'test-operation'
  
  // Mock operation that fails with error missing cause property
  const operation = mock.fn(() => {
    throw new Error('Generic error')
  })

  let caughtError: Error | undefined
  try {
    await invokeAPIWithRetry(operationId, operation)
  } catch (error) {
    caughtError = error as Error
  }

  strictEqual(caughtError?.message, 'Generic error')
  strictEqual(operation.mock.calls.length, 1) // No retries
})

afterEach(() => {
  mock.restoreAll()
})