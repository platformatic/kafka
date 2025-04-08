import { deepStrictEqual, ok } from 'assert'
import { test } from 'node:test'
import {
  AuthenticationError,
  ERROR_PREFIX,
  GenericError,
  MultipleErrors,
  NetworkError,
  ProtocolError,
  ResponseError,
  TimeoutError,
  UnexpectedCorrelationIdError,
  UnfinishedWriteBufferError,
  UnsupportedCompressionError,
  UnsupportedError,
  UserError,
  errorCodes
} from '../src/index.ts'

test('should export error codes with correct prefix', () => {
  for (const code of errorCodes) {
    ok(code.startsWith(ERROR_PREFIX), `Error code ${code} should start with ${ERROR_PREFIX}`)
  }
})

test('GenericError constructor', () => {
  const error = new GenericError('PLT_KFK_USER', 'test message', { cause: new Error('cause'), foo: 'bar' })
  deepStrictEqual(error.message, 'test message')
  deepStrictEqual(error.code, 'PLT_KFK_USER')
  deepStrictEqual(error.foo, 'bar')
  ok(error.cause instanceof Error)
  deepStrictEqual(error.cause.message, 'cause')

  // Test enumerable properties
  const errorObj = JSON.parse(JSON.stringify(error))
  deepStrictEqual(errorObj.message, 'test message')
  deepStrictEqual(errorObj.code, 'PLT_KFK_USER')
  deepStrictEqual(errorObj.foo, 'bar')
  ok('stack' in errorObj)
})

test('GenericError.isGenericError', () => {
  const error = new GenericError('PLT_KFK_USER', 'test message')
  ok(GenericError.isGenericError(error))
  deepStrictEqual(GenericError.isGenericError(new Error('regular error')), false)
})

test('GenericError.findBy', () => {
  const error = new GenericError('PLT_KFK_USER', 'test message', { foo: 'bar', baz: 123 })
  deepStrictEqual(error.findBy('foo', 'bar'), error)
  deepStrictEqual(error.findBy('baz', 123), error)
  deepStrictEqual(error.findBy('foo', 'not-bar'), null)
  deepStrictEqual(error.findBy('unknown', 'value'), null)
})

test('MultipleErrors constructor', () => {
  const error1 = new GenericError('PLT_KFK_USER', 'error1')
  const error2 = new GenericError('PLT_KFK_NETWORK', 'error2')
  const multiError = new MultipleErrors('multiple errors', [error1, error2], {
    cause: new Error('cause'),
    meta: 'data'
  })

  deepStrictEqual(multiError.message, 'multiple errors')
  deepStrictEqual(multiError.code, 'PLT_KFK_MULTIPLE')
  deepStrictEqual(multiError.meta, 'data')
  ok(multiError.cause instanceof Error)
  deepStrictEqual(multiError.cause.message, 'cause')
  deepStrictEqual(multiError.errors.length, 2)
  deepStrictEqual(multiError.errors[0], error1)
  deepStrictEqual(multiError.errors[1], error2)

  // Test enumerable properties
  const errorObj = JSON.parse(JSON.stringify(multiError))
  deepStrictEqual(errorObj.message, 'multiple errors')
  deepStrictEqual(errorObj.code, 'PLT_KFK_MULTIPLE')
  deepStrictEqual(errorObj.meta, 'data')
  ok('stack' in errorObj)
})

test('MultipleErrors.isGenericError', () => {
  const error = new MultipleErrors('multiple errors', [])
  ok(MultipleErrors.isGenericError(error))
  deepStrictEqual(MultipleErrors.isGenericError(new Error('regular error')), false)
})

test('MultipleErrors.isMultipleErrors', () => {
  const error = new MultipleErrors('multiple errors', [])
  const genericError = new GenericError('PLT_KFK_USER', 'test message')

  ok(MultipleErrors.isMultipleErrors(error))
  deepStrictEqual(MultipleErrors.isMultipleErrors(genericError), false)
  deepStrictEqual(MultipleErrors.isMultipleErrors(new Error('regular error')), false)
})

test('MultipleErrors.findBy - direct match', () => {
  const multiError = new MultipleErrors('multiple errors', [], { foo: 'bar' })
  deepStrictEqual(multiError.findBy('foo', 'bar'), multiError)
  deepStrictEqual(multiError.findBy('foo', 'not-bar'), null)
})

test('MultipleErrors.findBy - nested errors', () => {
  const error1 = new GenericError('PLT_KFK_USER', 'error1', { foo: 'bar' })
  const error2 = new GenericError('PLT_KFK_NETWORK', 'error2', { baz: 'qux' })
  const multiError = new MultipleErrors('multiple errors', [error1, error2])

  deepStrictEqual(multiError.findBy('foo', 'bar'), error1)
  deepStrictEqual(multiError.findBy('baz', 'qux'), error2)
  deepStrictEqual(multiError.findBy('unknown', 'value'), null)
})

test('MultipleErrors.findBy - works with direct properties', () => {
  const multiError = new MultipleErrors('outer errors', [], { outerProp: 'outer-value' })
  deepStrictEqual(multiError.findBy('outerProp', 'outer-value'), multiError)
})

test('MultipleErrors.findBy - recursively finds properties in nested errors', () => {
  // This test shows how MultipleErrors.findBy actually works with the current implementation
  const innerError = new GenericError('PLT_KFK_USER', 'inner error', { deepProp: 'value' })
  const middleError = new MultipleErrors('middle errors', [innerError], { middleProp: 'middle-value' })
  const outerError = new MultipleErrors('outer errors', [middleError], { outerProp: 'outer-value' })

  // Should find middleError directly in outerError.errors array
  deepStrictEqual(outerError.findBy('middleProp', 'middle-value'), middleError)

  // The current implementation of findBy doesn't return the original error object but the error itself
  // In this case, the findBy should return the middleError since it contains an error with deepProp
  const foundNested = outerError.findBy('deepProp', 'value')
  ok(foundNested)
  deepStrictEqual(foundNested.code, 'PLT_KFK_MULTIPLE') // middleError.code

  // But the innerError can be found directly from middleError
  const foundFromMiddle = middleError.findBy('deepProp', 'value')!
  deepStrictEqual(foundFromMiddle, innerError)
  deepStrictEqual(foundFromMiddle.code, 'PLT_KFK_USER')

  // Direct property of the outer error
  deepStrictEqual(outerError.findBy('outerProp', 'outer-value'), outerError)
})

test('AuthenticationError', () => {
  const error = new AuthenticationError('authentication failed', { detail: 'invalid credentials' })
  deepStrictEqual(error.message, 'authentication failed')
  deepStrictEqual(error.code, 'PLT_KFK_AUTHENTICATION')
  deepStrictEqual(error.detail, 'invalid credentials')
  ok(GenericError.isGenericError(error))
})

test('NetworkError', () => {
  const error = new NetworkError('connection failed', { host: 'localhost' })
  deepStrictEqual(error.message, 'connection failed')
  deepStrictEqual(error.code, 'PLT_KFK_NETWORK')
  deepStrictEqual(error.host, 'localhost')
  ok(GenericError.isGenericError(error))
})

test('ProtocolError with string code', () => {
  const error = new ProtocolError('UNKNOWN_TOPIC_OR_PARTITION', { topic: 'test-topic' })
  deepStrictEqual(error.code, 'PLT_KFK_PROTOCOL')
  deepStrictEqual(error.message, 'This server does not host this topic-partition.')
  deepStrictEqual(error.apiId, 'UNKNOWN_TOPIC_OR_PARTITION')
  deepStrictEqual(error.apiCode, 3)
  deepStrictEqual(error.topic, 'test-topic')
  ok(error.hasStaleMetadata)
  ok(error.canRetry)
})

test('ProtocolError with numeric code', () => {
  const error = new ProtocolError(3, { partition: 1 })
  deepStrictEqual(error.code, 'PLT_KFK_PROTOCOL')
  deepStrictEqual(error.message, 'This server does not host this topic-partition.')
  deepStrictEqual(error.apiId, 'UNKNOWN_TOPIC_OR_PARTITION')
  deepStrictEqual(error.apiCode, 3)
  deepStrictEqual(error.partition, 1)
  ok(error.hasStaleMetadata)
  ok(error.canRetry)
})

test('ProtocolError with response containing memberId', () => {
  const response = { memberId: 'test-member-id' }
  const error = new ProtocolError('REBALANCE_IN_PROGRESS', {}, response)
  deepStrictEqual(error.code, 'PLT_KFK_PROTOCOL')
  deepStrictEqual(error.apiId, 'REBALANCE_IN_PROGRESS')
  ok(error.rebalanceInProgress)
  ok(error.needsRejoin)
  deepStrictEqual(error.memberId, 'test-member-id')
})

test('ResponseError', () => {
  const apiName = 3 // Metadata
  const apiVersion = 1
  const errors = {
    'topics[0]': 3, // UNKNOWN_TOPIC_OR_PARTITION
    'topics[1]': 5 // LEADER_NOT_AVAILABLE
  }
  const response = { topics: ['topic1', 'topic2'] }

  const error = new ResponseError(apiName, apiVersion, errors, response, { requestId: '123' })

  deepStrictEqual(error.code, 'PLT_KFK_RESPONSE')
  deepStrictEqual(error.message, 'Received response with error while executing API Metadata(v1)')
  deepStrictEqual(error.requestId, '123')
  deepStrictEqual(error.response, response)
  deepStrictEqual(error.errors.length, 2)

  // Check that the nested errors are ProtocolErrors
  ok(error.errors[0] instanceof ProtocolError)
  ok(error.errors[1] instanceof ProtocolError)

  // Check first nested error
  deepStrictEqual(error.errors[0].apiId, 'UNKNOWN_TOPIC_OR_PARTITION')
  deepStrictEqual(error.errors[0].path, 'topics[0]')

  // Check second nested error
  deepStrictEqual(error.errors[1].apiId, 'LEADER_NOT_AVAILABLE')
  deepStrictEqual(error.errors[1].path, 'topics[1]')

  // Try finding by property
  deepStrictEqual(error.findBy('apiId', 'UNKNOWN_TOPIC_OR_PARTITION'), error.errors[0])
  deepStrictEqual(error.findBy('apiId', 'LEADER_NOT_AVAILABLE'), error.errors[1])
})

test('TimeoutError', () => {
  const error = new TimeoutError('request timed out', { requestId: '123' })
  deepStrictEqual(error.message, 'request timed out')
  deepStrictEqual(error.code, 'PLT_KFK_NETWORK') // Note: uses NetworkError.code
  deepStrictEqual(error.requestId, '123')
  ok(GenericError.isGenericError(error))
})

test('UnexpectedCorrelationIdError', () => {
  const error = new UnexpectedCorrelationIdError('wrong correlation id', { expected: 1, actual: 2 })
  deepStrictEqual(error.message, 'wrong correlation id')
  deepStrictEqual(error.code, 'PLT_KFK_UNEXPECTED_CORRELATION_ID')
  deepStrictEqual(error.expected, 1)
  deepStrictEqual(error.actual, 2)
  ok(GenericError.isGenericError(error))
})

test('UnfinishedWriteBufferError', () => {
  const error = new UnfinishedWriteBufferError('unfinished write buffer', { bytesWritten: 100 })
  deepStrictEqual(error.message, 'unfinished write buffer')
  deepStrictEqual(error.code, 'PLT_KFK_UNFINISHED_WRITE_BUFFER')
  deepStrictEqual(error.bytesWritten, 100)
  ok(GenericError.isGenericError(error))
})

test('UnsupportedCompressionError', () => {
  const error = new UnsupportedCompressionError('unsupported compression', { type: 'lz4' })
  deepStrictEqual(error.message, 'unsupported compression')
  deepStrictEqual(error.code, 'PLT_KFK_UNSUPPORTED_COMPRESSION')
  deepStrictEqual(error.type, 'lz4')
  ok(GenericError.isGenericError(error))
})

test('UnsupportedError', () => {
  const error = new UnsupportedError('unsupported feature', { feature: 'transactions' })
  deepStrictEqual(error.message, 'unsupported feature')
  deepStrictEqual(error.code, 'PLT_KFK_UNSUPPORTED')
  deepStrictEqual(error.feature, 'transactions')
  ok(GenericError.isGenericError(error))
})

test('UserError', () => {
  const error = new UserError('invalid configuration', { param: 'clientId' })
  deepStrictEqual(error.message, 'invalid configuration')
  deepStrictEqual(error.code, 'PLT_KFK_USER')
  deepStrictEqual(error.param, 'clientId')
  ok(GenericError.isGenericError(error))
})
