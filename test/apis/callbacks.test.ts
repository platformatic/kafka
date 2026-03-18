import { deepStrictEqual, ok } from 'assert'
import { test } from 'node:test'
import { GenericError, MultipleErrors } from '../../src/errors.ts'
import { runConcurrentCallbacks } from '../../src/apis/callbacks.ts'

test('runConcurrentCallbacks - all operations succeed', (_, done) => {
  const collection = ['a', 'b', 'c']

  runConcurrentCallbacks<string, string>(
    'should not error',
    collection,
    (item, cb) => {
      cb(null, item.toUpperCase())
    },
    (error, results) => {
      deepStrictEqual(error, null)
      deepStrictEqual(results, ['A', 'B', 'C'])
      done()
    }
  )
})

test('runConcurrentCallbacks - all operations fail', (_, done) => {
  const collection = ['a', 'b']

  runConcurrentCallbacks<string, string>(
    'all failed',
    collection,
    (item, cb) => {
      cb(new GenericError('PLT_KFK_USER', `failed: ${item}`))
    },
    (error, results) => {
      ok(error)
      ok(MultipleErrors.isMultipleErrors(error))
      const multi = error as MultipleErrors
      deepStrictEqual(multi.errors.length, 2)
      deepStrictEqual(multi.errors[0].message, 'failed: a')
      deepStrictEqual(multi.errors[1].message, 'failed: b')
      done()
    }
  )
})

test('runConcurrentCallbacks - partial failures should not contain undefined in errors', (_, done) => {
  const collection = ['a', 'b', 'c']

  runConcurrentCallbacks<string, string>(
    'partial failure',
    collection,
    (item, cb) => {
      if (item === 'b') {
        cb(new GenericError('PLT_KFK_USER', 'failed: b'))
      } else {
        cb(null, item.toUpperCase())
      }
    },
    (error, results) => {
      ok(error)
      ok(MultipleErrors.isMultipleErrors(error))
      const multi = error as MultipleErrors

      // The errors array should only contain actual errors, no undefined entries
      deepStrictEqual(multi.errors.length, 1)
      deepStrictEqual(multi.errors[0].message, 'failed: b')

      for (const e of multi.errors) {
        ok(e !== undefined, 'errors array should not contain undefined entries')
        ok(e instanceof Error, 'every entry should be an Error')
      }

      // Successful results should still be available
      deepStrictEqual(results[0], 'A')
      deepStrictEqual(results[2], 'C')
      done()
    }
  )
})

test('runConcurrentCallbacks - empty collection', (_, done) => {
  runConcurrentCallbacks<string, string>(
    'empty',
    [],
    (_item, _cb) => {
      throw new Error('should not be called')
    },
    (error, results) => {
      deepStrictEqual(error, null)
      deepStrictEqual(results, [])
      done()
    }
  )
})
