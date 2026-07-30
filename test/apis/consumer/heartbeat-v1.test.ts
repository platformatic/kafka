import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { heartbeatV1, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = heartbeatV1

test('createRequest serializes basic parameters correctly', () => {
  const groupId = 'test-group'
  const generationId = 5
  const memberId = 'test-member-1'
  const groupInstanceId = null

  const writer = createRequest(groupId, generationId, memberId, groupInstanceId)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Verify all the serialized parameters
  const data = {
    groupId: reader.readString(false),
    generationId: reader.readInt32(),
    memberId: reader.readString(false)
  }

  // Verify the serialized data matches expected values
  deepStrictEqual(data, { groupId, generationId, memberId })
  deepStrictEqual(reader.remaining, 0)

  let headers: unknown[] = []
  api({ send: (...args: unknown[]) => { headers = args } } as never, groupId, generationId, memberId, groupInstanceId)
  deepStrictEqual(headers.slice(4, 6), [false, false])
})

test('createRequest ignores group instance ID', () => {
  const groupId = 'test-group'
  const generationId = 5
  const memberId = 'test-member-1'
  const groupInstanceId = 'test-instance-id'

  const reader = Reader.from(createRequest(groupId, generationId, memberId, groupInstanceId))

  // Read all parameters and verify correctness
  const data = {
    groupId: reader.readString(false),
    generationId: reader.readInt32(),
    memberId: reader.readString(false)
  }

  // Verify old protocol versions do not serialize static membership
  deepStrictEqual(data, { groupId, generationId, memberId })
  deepStrictEqual(reader.remaining, 0)
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)

  const response = parseResponse(1, 12, 1, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, { throttleTimeMs: 0, errorCode: 0 })
})

test('parseResponse handles throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    .appendInt16(0) // errorCode (success)

  const response = parseResponse(1, 12, 1, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response, { throttleTimeMs: 100, errorCode: 0 })
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(16) // errorCode (e.g., UNKNOWN_MEMBER_ID)

  // Verify that parsing throws ResponseError
  throws(
    () => { parseResponse(1, 12, 1, Reader.from(writer)) },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response, { throttleTimeMs: 0, errorCode: 16 })
      return true
    }
  )
})

test('parseResponse handles rebalance in progress', () => {
  // Create a response with REBALANCE_IN_PROGRESS error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(27) // errorCode (REBALANCE_IN_PROGRESS)

  // Verify that parsing throws ResponseError
  throws(
    () => { parseResponse(1, 12, 1, Reader.from(writer)) },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response, { throttleTimeMs: 0, errorCode: 27 })
      return true
    }
  )
})
