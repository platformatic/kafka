import { deepStrictEqual, strictEqual } from 'node:assert'
import { test } from 'node:test'
import {
  createPromisifiedCallback,
  kCallbackPromise,
  MultipleErrors,
  noopCallback,
  runConcurrentCallbacks
} from '../../src/index.ts'

test('noopCallback exists and is a function', () => {
  strictEqual(typeof noopCallback, 'function')
})

test('createPromisifiedCallback returns a callback with associated promise', async () => {
  const callback = createPromisifiedCallback<string>()

  // Verify the callback has a promise property
  strictEqual(typeof callback[kCallbackPromise], 'object')
  strictEqual(callback[kCallbackPromise] instanceof Promise, true)

  // Schedule callback call for later
  setTimeout(() => {
    callback(null, 'test-value')
  }, 10)

  // Wait for the promise to resolve
  const result = await callback[kCallbackPromise]!
  strictEqual(result, 'test-value')
})

test('createPromisifiedCallback rejects promise on error', async () => {
  const callback = createPromisifiedCallback<string>()

  // Schedule error callback call for later
  setTimeout(() => {
    callback(new Error('test-error'), undefined as unknown as string)
  }, 10)

  // Ensure promise rejects with the right error
  try {
    await callback[kCallbackPromise]!
    throw new Error('Promise should have rejected')
  } catch (err: any) {
    strictEqual(err.message, 'test-error')
  }
})

test('runConcurrentCallbacks with array works correctly', async () => {
  const testArray = ['item1', 'item2', 'item3']

  const operation = (item: string, cb: (error: Error | null, result?: string) => void) => {
    // Simulate async operation
    setTimeout(() => {
      cb(null, item.toUpperCase())
    }, 10)
  }

  await new Promise<void>(resolve => {
    runConcurrentCallbacks('Test error message', testArray, operation, (error, results) => {
      strictEqual(error, null)
      deepStrictEqual(results, ['ITEM1', 'ITEM2', 'ITEM3'])
      resolve()
    })
  })
})

test('runConcurrentCallbacks with Set works correctly', async () => {
  const testSet = new Set(['item1', 'item2', 'item3'])

  const operation = (item: string, cb: (error: Error | null, result?: string) => void) => {
    // Simulate async operation
    setTimeout(() => {
      cb(null, item.toUpperCase())
    }, 10)
  }

  await new Promise<void>(resolve => {
    runConcurrentCallbacks('Test error message', testSet, operation, (error, results) => {
      strictEqual(error, null)
      // Set iteration order is guaranteed to be insertion order
      deepStrictEqual(results, ['ITEM1', 'ITEM2', 'ITEM3'])
      resolve()
    })
  })
})

test('runConcurrentCallbacks with Map works correctly', async () => {
  const testMap = new Map([
    ['key1', 'value1'],
    ['key2', 'value2'],
    ['key3', 'value3']
  ])

  const operation = (entry: [string, string], cb: (error: Error | null, result?: string) => void) => {
    // Simulate async operation
    setTimeout(() => {
      cb(null, `${entry[0]}-${entry[1]}`.toUpperCase())
    }, 10)
  }

  await new Promise<void>(resolve => {
    runConcurrentCallbacks('Test error message', testMap, operation, (error, results) => {
      strictEqual(error, null)
      // Map iteration order is guaranteed to be insertion order
      deepStrictEqual(results, ['KEY1-VALUE1', 'KEY2-VALUE2', 'KEY3-VALUE3'])
      resolve()
    })
  })
})

test('runConcurrentCallbacks handles errors correctly', async () => {
  const testArray = ['item1', 'error-item', 'item3']

  const operation = (item: string, cb: (error: Error | null, result?: string) => void) => {
    // Simulate async operation
    setTimeout(() => {
      if (item === 'error-item') {
        cb(new Error('Test operation error'))
      } else {
        cb(null, item.toUpperCase())
      }
    }, 10)
  }

  await new Promise<void>(resolve => {
    runConcurrentCallbacks('Test error message', testArray, operation, (error, results) => {
      strictEqual(error instanceof MultipleErrors, true)
      // Even with errors, all results should be populated
      strictEqual(Array.isArray(results), true)
      strictEqual(results.length, 3)
      strictEqual(results[0], 'ITEM1')
      strictEqual(results[2], 'ITEM3')

      if (error instanceof MultipleErrors) {
        strictEqual(error.message, 'Test error message')
        strictEqual(error.errors.length, 3)
        strictEqual(error.errors[1]?.message, 'Test operation error')
      }

      resolve()
    })
  })
})

test('runConcurrentCallbacks with empty collection', { timeout: 200 }, (_, done) => {
  const testArray: string[] = []

  // With empty collection, the callback should be called immediately
  runConcurrentCallbacks(
    'Test error message',
    testArray,
    () => {
      // This should never be called
      throw new Error('Should not be called')
    },
    (error, results) => {
      strictEqual(error, null)
      deepStrictEqual(results, [])
      done()
    }
  )
})

test('runConcurrentCallbacks with multiple errors', async () => {
  const testArray = ['error-1', 'item2', 'error-3']

  const operation = (item: string, cb: (error: Error | null, result?: string) => void) => {
    // Simulate async operation
    setTimeout(() => {
      if (item.startsWith('error-')) {
        cb(new Error(`Error from ${item}`))
      } else {
        cb(null, item.toUpperCase())
      }
    }, 10)
  }

  await new Promise<void>(resolve => {
    runConcurrentCallbacks('Multiple errors occurred', testArray, operation, (error, results) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(Array.isArray(results), true)
      strictEqual(results.length, 3)
      strictEqual(results[1], 'ITEM2')

      if (error instanceof MultipleErrors) {
        strictEqual(error.message, 'Multiple errors occurred')
        strictEqual(error.errors.length, 3)
        strictEqual(error.errors[0]?.message, 'Error from error-1')
        strictEqual(error.errors[2]?.message, 'Error from error-3')
      }

      resolve()
    })
  })
})
