import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { addOffsetsToTxnV4, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = addOffsetsToTxnV4

test('createRequest serializes parameters correctly', () => {
  const transactionalId = 'transaction-123'
  const producerId = 1234567890n
  const producerEpoch = 5
  const groupId = 'consumer-group-1'

  const writer = createRequest(transactionalId, producerId, producerEpoch, groupId)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all serialized data
  const serializedData = {
    transactionalId: reader.readString(),
    producerId: reader.readInt64(),
    producerEpoch: reader.readInt16(),
    groupId: reader.readString()
  }

  // Verify all values match expected values
  deepStrictEqual(serializedData, {
    transactionalId,
    producerId,
    producerEpoch,
    groupId
  })
})

test('createRequest with different group ID', () => {
  const transactionalId = 'transaction-123'
  const producerId = 1234567890n
  const producerEpoch = 5
  const groupId = 'another-consumer-group'

  const writer = createRequest(transactionalId, producerId, producerEpoch, groupId)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all serialized data
  const serializedData = {
    transactionalId: reader.readString(),
    producerId: reader.readInt64(),
    producerEpoch: reader.readInt16(),
    groupId: reader.readString()
  }

  // Verify all values match expected values
  deepStrictEqual(serializedData, {
    transactionalId,
    producerId,
    producerEpoch,
    groupId
  })
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 25, 4, Reader.from(writer))

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

  const response = parseResponse(1, 25, 4, Reader.from(writer))

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
    .appendInt16(49) // errorCode (e.g., INVALID_PRODUCER_EPOCH)
    .appendInt8(0) // Root tagged fields

  throws(
    () => {
      parseResponse(1, 25, 4, Reader.from(writer))
    },
    (err: any) => {
      // Verify error is the right type
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check for existence of response property
      ok(err.response !== undefined, 'Response object should be attached to the error')

      // Verify the response structure
      deepStrictEqual(err.response, {
        errorCode: 49,
        throttleTimeMs: 0
      })

      // Check that errors object exists and has the right type
      ok(err.errors && typeof err.errors === 'object', 'Errors object should exist')

      return true
    }
  )
})
