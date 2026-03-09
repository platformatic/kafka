import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { consumerGroupHeartbeatV1, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = consumerGroupHeartbeatV1

test('createRequest serializes subscribed topic regex correctly', () => {
  const writer = createRequest(
    'test-group',
    'test-member-1',
    5,
    null,
    null,
    30000,
    ['topic1', 'topic2'],
    '^topic-.*$',
    'uniform',
    []
  )

  ok(writer instanceof Writer)

  const reader = Reader.from(writer)

  deepStrictEqual(reader.readString(), 'test-group')
  deepStrictEqual(reader.readString(), 'test-member-1')
  deepStrictEqual(reader.readInt32(), 5)
  deepStrictEqual(reader.readNullableString(), null)
  deepStrictEqual(reader.readNullableString(), null)
  deepStrictEqual(reader.readInt32(), 30000)
  deepStrictEqual(
    reader.readArray(r => r.readString(), true, false),
    ['topic1', 'topic2']
  )
  deepStrictEqual(reader.readNullableString(), '^topic-.*$')
  deepStrictEqual(reader.readNullableString(), 'uniform')
  deepStrictEqual(
    reader.readArray(() => null),
    []
  )
})

test('parseResponse correctly processes a successful response', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendString(null)
    .appendString('test-member-1')
    .appendInt32(5)
    .appendInt32(3000)
    .appendInt8(-1)
    .appendInt8(0)
    .appendInt8(0)

  const response = parseResponse(1, 68, 1, Reader.from(writer))

  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    memberId: 'test-member-1',
    memberEpoch: 5,
    heartbeatIntervalMs: 3000,
    assignment: null
  })
})

test('parseResponse throws ResponseError on error response', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(112)
    .appendString('unsupported assignor')
    .appendString(null)
    .appendInt32(0)
    .appendInt32(0)
    .appendInt8(-1)
    .appendInt8(0)
    .appendInt8(0)

  throws(
    () => {
      parseResponse(1, 68, 1, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      deepStrictEqual(err.response.errorCode, 112)
      return true
    }
  )
})
