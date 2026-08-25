import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { offsetFetchV7, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetFetchV7

interface ResponsePartition {
  partitionIndex: number
  committedOffset: bigint
  committedLeaderEpoch: number
  metadata: string | null
  errorCode: number
}

interface ResponseTopic {
  name: string
  partitions: ResponsePartition[]
}

function writeResponse (topics: ResponseTopic[], errorCode = 0, unknownRootTag = false): Writer {
  const writer = Writer.create()
    .appendInt32(12)
    .appendArray(topics, (writer, topic) => {
      writer.appendString(topic.name).appendArray(topic.partitions, (writer, partition) => writer
        .appendInt32(partition.partitionIndex)
        .appendInt64(partition.committedOffset)
        .appendInt32(partition.committedLeaderEpoch)
        .appendString(partition.metadata)
        .appendInt16(partition.errorCode))
    })
    .appendInt16(errorCode)

  return unknownRootTag
    ? writer.appendUnsignedVarInt(1).appendUnsignedVarInt(42).appendUnsignedVarInt(2).append(Buffer.from([1, 2]))
    : writer.appendTaggedFields()
}

test('createRequest serializes basic parameters and nested tagged fields using the v7 wire format', () => {
  const writer = createRequest([
    {
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [0, 1] }]
    }
  ], false)

  ok(writer instanceof Writer)
  const reader = Reader.from(writer)
  deepStrictEqual(reader.readString(), 'test-group')
  deepStrictEqual(reader.readArray(
    reader => ({ name: reader.readString(), partitionIndexes: reader.readArray(reader => reader.readInt32(), true, false) })
  ), [{ name: 'test-topic', partitionIndexes: [0, 1] }])
  deepStrictEqual(reader.readBoolean(), false)
  deepStrictEqual(reader.readUnsignedVarInt(), 0)
  deepStrictEqual(reader.remaining, 0)
})

test('createRequest serializes requireStable in v7', () => {
  const groups = [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0] }] }]
  ok(!createRequest(groups, true).buffer.equals(createRequest(groups, false).buffer))
})

test('createRequest supports null topics', () => {
  const writer = createRequest([{ groupId: 'group-1', topics: null }], true)
  const reader = Reader.from(writer)
  deepStrictEqual(reader.readString(), 'group-1')
  deepStrictEqual(reader.readUnsignedVarInt(), 0)
  deepStrictEqual(reader.readBoolean(), true)
  deepStrictEqual(reader.readUnsignedVarInt(), 0)
  deepStrictEqual(reader.remaining, 0)
})

test('createRequest downconverts to the first group and defaults an empty group', () => {
  const groups = [{ groupId: 'group-1', topics: null }, { groupId: 'group-2', topics: null }]
  deepStrictEqual(createRequest(groups, false).buffer, createRequest([groups[0]], false).buffer)
  deepStrictEqual(createRequest([], false).buffer, Buffer.from('01000000', 'hex'))
})

test('parseResponse processes a successful response and automatic nested tag buffers', () => {
  const reader = Reader.from(writeResponse([
    {
      name: 'test-topic',
      partitions: [{ partitionIndex: 0, committedOffset: 100n, committedLeaderEpoch: 5, metadata: 'metadata', errorCode: 0 }]
    }
  ]))
  const response = parseResponse(1, 9, 7, reader)

  deepStrictEqual(response, {
    throttleTimeMs: 12,
    topics: [{
      name: 'test-topic',
      partitions: [{ partitionIndex: 0, committedOffset: 100n, committedLeaderEpoch: 5, metadata: 'metadata', errorCode: 0 }]
    }],
    errorCode: 0,
    groups: []
  })
  deepStrictEqual(reader.remaining, 0)
})

test('parseResponse handles a top-level error code', () => {
  throws(() => parseResponse(1, 9, 7, Reader.from(writeResponse([], 16))), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, { throttleTimeMs: 12, topics: [], errorCode: 16, groups: [] })
    return true
  })
})

test('parseResponse handles a partition-level error code', () => {
  throws(() => parseResponse(1, 9, 7, Reader.from(writeResponse([
    {
      name: 'test-topic',
      partitions: [{ partitionIndex: 0, committedOffset: -1n, committedLeaderEpoch: -1, metadata: null, errorCode: 3 }]
    }
  ]))), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, {
      throttleTimeMs: 12,
      topics: [{
        name: 'test-topic',
        partitions: [{ partitionIndex: 0, committedOffset: -1n, committedLeaderEpoch: -1, metadata: null, errorCode: 3 }]
      }],
      errorCode: 0,
      groups: []
    })
    return true
  })
})

test('parseResponse handles multiple topics and partitions and consumes unknown root tags', () => {
  const reader = Reader.from(writeResponse([
    {
      name: 'topic-1',
      partitions: [
        { partitionIndex: 0, committedOffset: 100n, committedLeaderEpoch: 5, metadata: 'metadata-1', errorCode: 0 },
        { partitionIndex: 1, committedOffset: 200n, committedLeaderEpoch: 6, metadata: 'metadata-2', errorCode: 0 }
      ]
    },
    {
      name: 'topic-2',
      partitions: [{ partitionIndex: 0, committedOffset: 300n, committedLeaderEpoch: 7, metadata: 'metadata-3', errorCode: 0 }]
    }
  ], 0, true))
  const response = parseResponse(1, 9, 7, reader)

  deepStrictEqual(response, {
    throttleTimeMs: 12,
    topics: [
      {
        name: 'topic-1',
        partitions: [
          { partitionIndex: 0, committedOffset: 100n, committedLeaderEpoch: 5, metadata: 'metadata-1', errorCode: 0 },
          { partitionIndex: 1, committedOffset: 200n, committedLeaderEpoch: 6, metadata: 'metadata-2', errorCode: 0 }
        ]
      },
      {
        name: 'topic-2',
        partitions: [{ partitionIndex: 0, committedOffset: 300n, committedLeaderEpoch: 7, metadata: 'metadata-3', errorCode: 0 }]
      }
    ],
    errorCode: 0,
    groups: []
  })
  deepStrictEqual(reader.remaining, 0)
})
