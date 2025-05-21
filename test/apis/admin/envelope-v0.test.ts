import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, envelopeV0 } from '../../../src/index.ts'

const { createRequest, parseResponse } = envelopeV0

test('createRequest serializes parameters correctly', () => {
  const requestData = Buffer.from('request data')
  const requestPrincipal = Buffer.from('request principal')
  const clientHostAddress = Buffer.from('127.0.0.1')

  const writer = createRequest(requestData, requestPrincipal, clientHostAddress)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read the request data
  const serializedRequestData = reader.readBytes()

  // Read the request principal
  const serializedRequestPrincipal = reader.readNullableBytes()

  // Read the client host address
  const serializedClientHostAddress = reader.readBytes()

  // Verify serialized data
  ok(Buffer.isBuffer(serializedRequestData), 'Request data should be serialized as a Buffer')
  deepStrictEqual(serializedRequestData.toString(), 'request data', 'Request data content should match')

  ok(Buffer.isBuffer(serializedRequestPrincipal), 'Request principal should be serialized as a Buffer')
  deepStrictEqual(serializedRequestPrincipal.toString(), 'request principal', 'Request principal content should match')

  ok(Buffer.isBuffer(serializedClientHostAddress), 'Client host address should be serialized as a Buffer')
  deepStrictEqual(serializedClientHostAddress.toString(), '127.0.0.1', 'Client host address content should match')
})

test('createRequest handles null request principal correctly', () => {
  const requestData = Buffer.from('request data')
  const requestPrincipal = null
  const clientHostAddress = Buffer.from('127.0.0.1')

  const writer = createRequest(requestData, requestPrincipal, clientHostAddress)
  const reader = Reader.from(writer)

  // Skip request data
  reader.readBytes()

  // Read the request principal
  const serializedRequestPrincipal = reader.readNullableBytes()

  // Verify null request principal
  deepStrictEqual(serializedRequestPrincipal, null, 'Null request principal should be serialized correctly')
})

test('createRequest handles undefined request principal correctly', () => {
  const requestData = Buffer.from('request data')
  const requestPrincipal = undefined
  const clientHostAddress = Buffer.from('127.0.0.1')

  const writer = createRequest(requestData, requestPrincipal, clientHostAddress)
  const reader = Reader.from(writer)

  // Skip request data
  reader.readBytes()

  // Read the request principal
  const serializedRequestPrincipal = reader.readNullableBytes()

  // Verify undefined request principal is serialized as null
  deepStrictEqual(serializedRequestPrincipal, null, 'Undefined request principal should be serialized as null')
})

test('parseResponse correctly processes a successful response', () => {
  const responseData = Buffer.from('response data')

  // Create a successful response
  const writer = Writer.create()
    .appendBytes(responseData) // responseData
    .appendInt16(0) // errorCode (success)
    .appendTaggedFields()

  const response = parseResponse(1, 58, 0, Reader.from(writer))

  // Verify response structure
  ok(Buffer.isBuffer(response.responseData), 'Response data should be a Buffer')
  deepStrictEqual(response.responseData!.toString(), 'response data', 'Response data content should match')
  deepStrictEqual(response.errorCode, 0, 'Error code should be 0 for success')
})

test('parseResponse correctly processes a null response data', () => {
  // Create a response with null data
  const writer = Writer.create()
    .appendBytes(null) // null responseData
    .appendInt16(0) // errorCode (success)
    .appendTaggedFields()

  const response = parseResponse(1, 58, 0, Reader.from(writer))

  // Verify null response data
  deepStrictEqual(response.responseData, null, 'Response data should be null')
  deepStrictEqual(response.errorCode, 0, 'Error code should be 0 for success')
})

test('parseResponse throws ResponseError on error response', () => {
  // Create an error response
  const writer = Writer.create()
    .appendBytes(null) // null responseData
    .appendInt16(58) // errorCode INVALID_TOKEN
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 58, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error details
      deepStrictEqual(err.response.errorCode, 58, 'Error code should be preserved in the response')

      return true
    }
  )
})

test('parseResponse throws ResponseError with non-null responseData', () => {
  const errorData = Buffer.from('error details')

  // Create an error response with non-null responseData
  const writer = Writer.create()
    .appendBytes(errorData) // non-null responseData with error details
    .appendInt16(58) // errorCode INVALID_TOKEN
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 58, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error details
      deepStrictEqual(err.response.errorCode, 58, 'Error code should be preserved in the response')

      // Verify error responseData
      ok(Buffer.isBuffer(err.response.responseData), 'Error response data should be a Buffer')
      deepStrictEqual(
        err.response.responseData!.toString(),
        'error details',
        'Error response data content should be preserved'
      )

      return true
    }
  )
})
