import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
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
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('createRequest preserves null topic partitions distinctly from an empty array', () => {
  const cases: Array<[Parameters<typeof createRequest>[9], number]> = [[null, 0], [[], 1]]
  for (const [topicPartitions, expectedLength] of cases) {
    const reader = Reader.from(createRequest('group', 'member', 1, null, null, 1, null, null, null, topicPartitions))
    reader.readString()
    reader.readString()
    reader.readInt32()
    reader.readNullableString()
    reader.readNullableString()
    reader.readInt32()
    reader.readArray(() => null)
    reader.readNullableString()
    reader.readNullableString()
    strictEqual(reader.readUnsignedVarInt(), expectedLength)
    reader.readTaggedFields()
    strictEqual(reader.remaining, 0)
  }
})

test('createRequest serializes populated topic partitions correctly', () => {
  const topicPartitions = [
    { topicId: '12345678-1234-1234-1234-123456789012', partitions: [0, 1, 2] },
    { topicId: '87654321-4321-4321-4321-210987654321', partitions: [3, 4] }
  ]
  const reader = Reader.from(createRequest('group', 'member', 1, null, null, 1, null, null, null, topicPartitions))

  reader.readString()
  reader.readString()
  reader.readInt32()
  reader.readNullableString()
  reader.readNullableString()
  reader.readInt32()
  strictEqual(reader.readUnsignedVarInt(), 0)
  strictEqual(reader.readNullableString(), null)
  strictEqual(reader.readNullableString(), null)
  strictEqual(reader.readUnsignedVarInt(), topicPartitions.length + 1)
  for (const topicPartition of topicPartitions) {
    strictEqual(reader.readUUID(), topicPartition.topicId)
    strictEqual(reader.readUnsignedVarInt(), topicPartition.partitions.length + 1)
    for (const partition of topicPartition.partitions) {
      strictEqual(reader.readInt32(), partition)
    }
    reader.readTaggedFields()
  }
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
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

  const reader = Reader.from(writer)
  const response = parseResponse(1, 68, 1, reader)

  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    memberId: 'test-member-1',
    memberEpoch: 5,
    heartbeatIntervalMs: 3000,
    assignment: null
  })
  strictEqual(reader.remaining, 0)
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
