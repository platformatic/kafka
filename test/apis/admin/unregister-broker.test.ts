import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, unregisterBrokerV0 } from '../../../src/index.ts'

const { createRequest, parseResponse } = unregisterBrokerV0

test('createRequest serializes broker ID correctly', () => {
  const brokerId = 1

  const writer = createRequest(brokerId)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read broker ID
  const serializedBrokerId = reader.readInt32()

  // Verify serialized data
  deepStrictEqual(serializedBrokerId, 1, 'Broker ID should be serialized correctly')
})

test('createRequest handles different broker IDs correctly', () => {
  const brokerId = 42

  const writer = createRequest(brokerId)
  const reader = Reader.from(writer)

  // Read broker ID
  const serializedBrokerId = reader.readInt32()

  // Verify broker ID
  deepStrictEqual(serializedBrokerId, 42, 'Different broker ID should be serialized correctly')
})

test('createRequest handles zero broker ID correctly', () => {
  const brokerId = 0

  const writer = createRequest(brokerId)
  const reader = Reader.from(writer)

  // Read broker ID
  const serializedBrokerId = reader.readInt32()

  // Verify zero broker ID
  deepStrictEqual(serializedBrokerId, 0, 'Zero broker ID should be serialized correctly')
})

test('createRequest handles negative broker ID correctly', () => {
  const brokerId = -1

  const writer = createRequest(brokerId)
  const reader = Reader.from(writer)

  // Read broker ID
  const serializedBrokerId = reader.readInt32()

  // Verify negative broker ID
  deepStrictEqual(serializedBrokerId, -1, 'Negative broker ID should be serialized correctly')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage
    .appendTaggedFields()

  const response = parseResponse(1, 64, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    {
      throttleTimeMs: response.throttleTimeMs,
      errorCode: response.errorCode,
      errorMessage: response.errorMessage
    },
    {
      throttleTimeMs: 0,
      errorCode: 0,
      errorMessage: null
    },
    'Successful response should match expected structure'
  )
})

test('parseResponse throws ResponseError on error response', () => {
  // Create an error response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(37) // errorCode BROKER_ID_NOT_REGISTERED
    .appendString('Broker ID not registered') // errorMessage
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 64, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error details
      deepStrictEqual(err.response.errorCode, 37, 'Error code should be preserved in the response')

      deepStrictEqual(
        err.response.errorMessage,
        'Broker ID not registered',
        'Error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse handles different error responses correctly', () => {
  // Create an error response with a different error code
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(58) // errorCode INVALID_TOKEN
    .appendString('Invalid token') // errorMessage
    .appendTaggedFields()

  // Verify that parsing throws ResponseError with the correct error code
  throws(
    () => {
      parseResponse(1, 64, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify different error code
      deepStrictEqual(err.response.errorCode, 58, 'Different error code should be preserved in the response')

      return true
    }
  )
})

test('parseResponse handles non-zero throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage
    .appendTaggedFields()

  const response = parseResponse(1, 64, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
