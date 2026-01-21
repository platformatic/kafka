import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { endTxnV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = endTxnV3

test('createRequest serializes parameters correctly for commit', () => {
  const transactionalId = 'transaction-123'
  const producerId = 1234567890n
  const producerEpoch = 5
  const committed = true // Committing the transaction

  const writer = createRequest(transactionalId, producerId, producerEpoch, committed)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all values and verify them at once
  deepStrictEqual(
    {
      transactionalId: reader.readString(),
      producerId: reader.readInt64(),
      producerEpoch: reader.readInt16(),
      committed: reader.readBoolean()
    },
    {
      transactionalId,
      producerId,
      producerEpoch,
      committed
    }
  )
})

test('createRequest serializes parameters correctly for abort', () => {
  const transactionalId = 'transaction-123'
  const producerId = 1234567890n
  const producerEpoch = 5
  const committed = false // Aborting the transaction

  const writer = createRequest(transactionalId, producerId, producerEpoch, committed)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all values and verify them at once
  deepStrictEqual(
    {
      transactionalId: reader.readString(),
      producerId: reader.readInt64(),
      producerEpoch: reader.readInt16(),
      committed: reader.readBoolean()
    },
    {
      transactionalId,
      producerId,
      producerEpoch,
      committed
    }
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 26, 3, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0
  })
})

test('parseResponse handles response with throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    .appendInt16(0) // errorCode (success)
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 26, 3, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    errorCode: 0
  })
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(47) // errorCode (INVALID_TXN_STATE)
    .appendInt8(0) // Root tagged fields

  throws(
    () => {
      parseResponse(1, 26, 4, Reader.from(writer))
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

      // Verify the response structure
      deepStrictEqual(
        err.response,
        {
          errorCode: 47,
          throttleTimeMs: 0
        },
        'Response should contain correct error code and throttle time'
      )

      // Check that errors object exists
      ok(err.errors !== undefined && typeof err.errors === 'object', 'Errors object should exist')

      return true
    }
  )
})
