import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { initProducerIdV4, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = initProducerIdV4

test('createRequest serializes parameters correctly with transactional ID', () => {
  const transactionalId = 'transaction-123'
  const transactionTimeoutMs = 60000
  const producerId = 1234n
  const producerEpoch = 5

  const writer = createRequest(transactionalId, transactionTimeoutMs, producerId, producerEpoch)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all values and verify them at once
  deepStrictEqual(
    {
      transactionalId: reader.readString(),
      transactionTimeoutMs: reader.readInt32(),
      producerId: reader.readInt64(),
      producerEpoch: reader.readInt16()
    },
    {
      transactionalId,
      transactionTimeoutMs,
      producerId,
      producerEpoch
    }
  )
})

test('createRequest serializes parameters correctly without transactional ID', () => {
  const transactionalId = null
  const transactionTimeoutMs = 60000
  const producerId = 1234n
  const producerEpoch = 5

  const writer = createRequest(transactionalId, transactionTimeoutMs, producerId, producerEpoch)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all values and verify them at once
  deepStrictEqual(
    {
      transactionalId: reader.readNullableString(),
      transactionTimeoutMs: reader.readInt32(),
      producerId: reader.readInt64(),
      producerEpoch: reader.readInt16()
    },
    {
      transactionalId,
      transactionTimeoutMs,
      producerId,
      producerEpoch
    }
  )
})

test('createRequest with default values initializes a new producer', () => {
  const writer = createRequest(
    null, // No transactional ID
    60000, // Default transaction timeout
    -1n, // Default producer ID for new producers
    -1 // Default producer epoch for new producers
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all values and verify them at once
  deepStrictEqual(
    {
      transactionalId: reader.readNullableString(),
      transactionTimeoutMs: reader.readInt32(),
      producerId: reader.readInt64(),
      producerEpoch: reader.readInt16()
    },
    {
      transactionalId: null,
      transactionTimeoutMs: 60000,
      producerId: -1n,
      producerEpoch: -1
    }
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt64(1234567890n) // producerId
    .appendInt16(5) // producerEpoch
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 22, 5, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    producerId: 1234567890n,
    producerEpoch: 5
  })
})

test('parseResponse handles response with throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    .appendInt16(0) // errorCode (success)
    .appendInt64(1234567890n) // producerId
    .appendInt16(5) // producerEpoch
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 22, 5, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    errorCode: 0,
    producerId: 1234567890n,
    producerEpoch: 5
  })
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(38) // errorCode (NOT_COORDINATOR error)
    .appendInt64(-1n) // producerId (invalid for error)
    .appendInt16(-1) // producerEpoch (invalid for error)
    .appendInt8(0) // Root tagged fields

  throws(
    () => {
      parseResponse(1, 22, 5, Reader.from(writer))
    },
    (err: any) => {
      // Verify error is the right type
      ok(err instanceof ResponseError, 'Should be a ResponseError')
      ok(
        err.message.includes('Received response with error while executing API'),
        'Error message should mention API execution error'
      )

      // Check for existence of response property
      ok(err.response !== undefined, 'Response object should be attached to the error')

      // Verify the error response structure
      deepStrictEqual(
        err.response,
        {
          errorCode: 38,
          throttleTimeMs: 0,
          producerId: -1n,
          producerEpoch: -1
        },
        'Response should contain correct error code, throttle time, and invalid producer information'
      )

      // Check that errors object exists
      ok(err.errors !== undefined && typeof err.errors === 'object', 'Errors object should exist')

      return true
    }
  )
})
