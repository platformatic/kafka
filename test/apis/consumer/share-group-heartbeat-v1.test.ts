import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { shareGroupHeartbeatV1 } from '../../../src/apis/index.ts'
import { Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = shareGroupHeartbeatV1

const topicId = '12345678-1234-1234-1234-123456789012'

test('api is ShareGroupHeartbeat v1', () => {
  strictEqual(api.key, 76)
  strictEqual(api.version, 1)
})

test('createRequest serializes a join request', () => {
  const reader = Reader.from(createRequest('group', 'member', 0, 'rack-a', ['topic-1', 'topic-2']))

  strictEqual(reader.readString(), 'group')
  strictEqual(reader.readString(), 'member')
  strictEqual(reader.readInt32(), 0)
  strictEqual(reader.readNullableString(), 'rack-a')
  deepStrictEqual(
    reader.readArray(r => r.readString(), true, false),
    ['topic-1', 'topic-2']
  )
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('createRequest keeps a null subscription distinct from an empty one', () => {
  // Null means the subscription did not change since the last heartbeat.
  const cases: Array<[string[] | null, number]> = [
    [null, 0],
    [[], 1]
  ]

  for (const [subscribedTopicNames, expectedLength] of cases) {
    const reader = Reader.from(createRequest('group', 'member', -1, null, subscribedTopicNames))

    reader.readString()
    reader.readString()
    strictEqual(reader.readInt32(), -1)
    strictEqual(reader.readNullableString(), null)
    strictEqual(reader.readUnsignedVarInt(), expectedLength)
    reader.readTaggedFields()
    strictEqual(reader.remaining, 0)
  }
})

test('parseResponse parses an assignment', () => {
  const writer = Writer.create()
    .appendInt32(10)
    .appendInt16(0)
    .appendString(null)
    .appendString('member')
    .appendInt32(3)
    .appendInt32(5000)
    .appendInt8(1)
    .appendArray([{ topicId, partitions: [0, 2] }], (w, t) => {
      w.appendUUID(t.topicId).appendArray(t.partitions, (w, p) => w.appendInt32(p), true, false)
    })
    .appendTaggedFields()
    .appendTaggedFields()

  const reader = Reader.from(writer)

  deepStrictEqual(parseResponse(1, 76, 1, reader), {
    throttleTimeMs: 10,
    errorCode: 0,
    errorMessage: null,
    memberId: 'member',
    memberEpoch: 3,
    heartbeatIntervalMs: 5000,
    assignment: { topicPartitions: [{ topicId, partitions: [0, 2] }] }
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse parses a missing assignment', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendString(null)
    .appendString('member')
    .appendInt32(1)
    .appendInt32(5000)
    .appendInt8(-1)
    .appendTaggedFields()

  const reader = Reader.from(writer)

  strictEqual(parseResponse(1, 76, 1, reader).assignment, null)
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws ResponseError with the server message', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(25)
    .appendString('Member is not known.')
    .appendString(null)
    .appendInt32(0)
    .appendInt32(0)
    .appendInt8(-1)
    .appendTaggedFields()

  throws(
    () => parseResponse(1, 76, 1, Reader.from(writer)),
    (err: any) => {
      ok(err instanceof ResponseError)
      strictEqual(err.response.errorCode, 25)
      deepStrictEqual(
        err.errors.map((e: any) => [e.path, e.apiId, e.serverErrorMessage]),
        [['/', 'UNKNOWN_MEMBER_ID', 'Member is not known.']]
      )
      return true
    }
  )
})
