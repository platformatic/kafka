import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { offsetFetchV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetFetchV3

interface ResponsePartition {
  partitionIndex: number
  committedOffset: bigint
  metadata: string | null
  errorCode: number
}

interface ResponseTopic {
  name: string
  partitions: ResponsePartition[]
}

function writeResponse (topics: ResponseTopic[], errorCode = 0): Writer {
  return Writer.create()
    .appendInt32(12)
    .appendArray(topics, (writer, topic) => {
      writer.appendString(topic.name, false).appendArray(
        topic.partitions,
        (writer, partition) => writer
          .appendInt32(partition.partitionIndex)
          .appendInt64(partition.committedOffset)
          .appendString(partition.metadata, false)
          .appendInt16(partition.errorCode),
        false,
        false
      )
    }, false, false)
    .appendInt16(errorCode)
}

test('createRequest serializes basic parameters using the v3 wire format', () => {
  const writer = createRequest([
    {
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [0, 1] }]
    }
  ], false)

  ok(writer instanceof Writer)
  const reader = Reader.from(writer)
  deepStrictEqual(reader.readString(false), 'test-group')
  deepStrictEqual(reader.readArray(
    reader => ({ name: reader.readString(false), partitionIndexes: reader.readArray(reader => reader.readInt32(), false, false) }),
    false,
    false
  ), [{ name: 'test-topic', partitionIndexes: [0, 1] }])
  deepStrictEqual(reader.remaining, 0)
})

test('createRequest ignores requireStable in v3', () => {
  const groups = [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0] }] }]
  deepStrictEqual(createRequest(groups, true).buffer, createRequest(groups, false).buffer)
})

test('createRequest supports null topics', () => {
  const writer = createRequest([{ groupId: 'group-1', topics: null }], true)
  const reader = Reader.from(writer)
  deepStrictEqual(reader.readString(false), 'group-1')
  deepStrictEqual(reader.readInt32(), -1)
  deepStrictEqual(reader.remaining, 0)
})

test('createRequest downconverts to the first group and defaults an empty group', () => {
  const groups = [{ groupId: 'group-1', topics: null }, { groupId: 'group-2', topics: null }]
  deepStrictEqual(createRequest(groups, false).buffer, createRequest([groups[0]], false).buffer)
  deepStrictEqual(createRequest([], false).buffer, Buffer.from('0000ffffffff', 'hex'))
})

test('parseResponse correctly processes a successful response', () => {
  const response = parseResponse(1, 9, 3, Reader.from(writeResponse([
    { name: 'test-topic', partitions: [{ partitionIndex: 0, committedOffset: 100n, metadata: 'metadata', errorCode: 0 }] }
  ])))

  deepStrictEqual(response, {
    throttleTimeMs: 12,
    topics: [{
      name: 'test-topic',
      partitions: [{ partitionIndex: 0, committedOffset: 100n, committedLeaderEpoch: -1, metadata: 'metadata', errorCode: 0 }]
    }],
    errorCode: 0,
    groups: []
  })
})

test('parseResponse handles a top-level error code', () => {
  throws(() => parseResponse(1, 9, 3, Reader.from(writeResponse([], 16))), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, { throttleTimeMs: 12, topics: [], errorCode: 16, groups: [] })
    return true
  })
})

test('parseResponse handles a partition-level error code', () => {
  throws(() => parseResponse(1, 9, 3, Reader.from(writeResponse([
    { name: 'test-topic', partitions: [{ partitionIndex: 0, committedOffset: -1n, metadata: null, errorCode: 3 }] }
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

test('parseResponse handles multiple topics and partitions', () => {
  const response = parseResponse(1, 9, 3, Reader.from(writeResponse([
    {
      name: 'topic-1',
      partitions: [
        { partitionIndex: 0, committedOffset: 100n, metadata: 'metadata-1', errorCode: 0 },
        { partitionIndex: 1, committedOffset: 200n, metadata: 'metadata-2', errorCode: 0 }
      ]
    },
    { name: 'topic-2', partitions: [{ partitionIndex: 0, committedOffset: 300n, metadata: 'metadata-3', errorCode: 0 }] }
  ])))

  deepStrictEqual(response, {
    throttleTimeMs: 12,
    topics: [
      {
        name: 'topic-1',
        partitions: [
          { partitionIndex: 0, committedOffset: 100n, committedLeaderEpoch: -1, metadata: 'metadata-1', errorCode: 0 },
          { partitionIndex: 1, committedOffset: 200n, committedLeaderEpoch: -1, metadata: 'metadata-2', errorCode: 0 }
        ]
      },
      {
        name: 'topic-2',
        partitions: [{ partitionIndex: 0, committedOffset: 300n, committedLeaderEpoch: -1, metadata: 'metadata-3', errorCode: 0 }]
      }
    ],
    errorCode: 0,
    groups: []
  })
})
