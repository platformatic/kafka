import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { heartbeatV4, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = heartbeatV4

test('createRequest serializes basic parameters correctly', () => {
  const groupId = 'test-group'
  const generationId = 5
  const memberId = 'test-member-1'
  const groupInstanceId = null

  const writer = createRequest(groupId, generationId, memberId, groupInstanceId)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Verify all the parameters and tagged fields
  const data = {
    groupId: reader.readString(),
    generationId: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readNullableString()
  }

  // Verify the serialized data matches expected values
  deepStrictEqual(data, {
    groupId,
    generationId,
    memberId,
    groupInstanceId
  })
})

test('createRequest with group instance ID', () => {
  const groupId = 'test-group'
  const generationId = 5
  const memberId = 'test-member-1'
  const groupInstanceId = 'test-instance-id'

  const writer = createRequest(groupId, generationId, memberId, groupInstanceId)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read all parameters and verify correctness
  const data = {
    groupId: reader.readString(),
    generationId: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readString()
  }

  // Verify all parameters match expected values
  deepStrictEqual(data, {
    groupId,
    generationId,
    memberId,
    groupInstanceId
  })

  // Verify tags count
  deepStrictEqual(reader.readUnsignedVarInt(), 0)
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 12, 4, writer.bufferList)

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0
  })
})

test('parseResponse handles throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    .appendInt16(0) // errorCode (success)
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 12, 4, writer.bufferList)

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
    .appendInt16(16) // errorCode (e.g., UNKNOWN_MEMBER_ID)
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 12, 4, writer.bufferList)
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        errorCode: 16
      })

      return true
    }
  )
})

test('parseResponse handles rebalance in progress', () => {
  // Create a response with REBALANCE_IN_PROGRESS error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(27) // errorCode (REBALANCE_IN_PROGRESS)
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 12, 4, writer.bufferList)
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        errorCode: 27
      })

      return true
    }
  )
})
