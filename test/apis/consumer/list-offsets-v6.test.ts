import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { listOffsetsV6, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = listOffsetsV6

test('createRequest serializes basic parameters correctly', () => {
  const replica = -1
  const isolationLevel = 0
  const topics = [
    {
      name: 'test-topic',
      partitions: [{ partitionIndex: 0, currentLeaderEpoch: 0, timestamp: -1n }]
    }
  ]

  const writer = createRequest(replica, isolationLevel, topics)
  ok(writer instanceof Writer)

  const reader = Reader.from(writer)
  deepStrictEqual(reader.readInt32(), replica)
  deepStrictEqual(reader.readInt8(), isolationLevel)

  const topicsArray = reader.readArray(() => {
    const topicName = reader.readString()
    const partitions = reader.readArray(() => {
      return {
        partitionIndex: reader.readInt32(),
        currentLeaderEpoch: reader.readInt32(),
        timestamp: reader.readInt64()
      }
    })
    return { topicName, partitions }
  })

  deepStrictEqual(topicsArray, [
    { topicName: 'test-topic', partitions: [{ partitionIndex: 0, currentLeaderEpoch: 0, timestamp: -1n }] }
  ])
  deepStrictEqual(reader.readUnsignedVarInt(), 0)
})

test('createRequest with multiple topics and partitions', () => {
  const replica = -1
  const isolationLevel = 0
  const topics = [
    {
      name: 'topic-1',
      partitions: [
        { partitionIndex: 0, currentLeaderEpoch: 0, timestamp: -1n },
        { partitionIndex: 1, currentLeaderEpoch: 0, timestamp: -1n }
      ]
    },
    {
      name: 'topic-2',
      partitions: [{ partitionIndex: 0, currentLeaderEpoch: 0, timestamp: -1n }]
    }
  ]

  const writer = createRequest(replica, isolationLevel, topics)
  ok(writer instanceof Writer)

  const reader = Reader.from(writer)
  deepStrictEqual(reader.readInt32(), replica)
  deepStrictEqual(reader.readInt8(), isolationLevel)

  const topicsRead = reader.readArray(() => {
    const topicName = reader.readString()
    const partitions = reader.readArray(() => {
      return {
        partitionIndex: reader.readInt32(),
        currentLeaderEpoch: reader.readInt32(),
        timestamp: reader.readInt64()
      }
    })
    return { topicName, partitions }
  })

  deepStrictEqual(topicsRead.length, 2)
  deepStrictEqual(topicsRead[0].topicName, 'topic-1')
  deepStrictEqual(topicsRead[0].partitions.length, 2)
  deepStrictEqual(topicsRead[1].topicName, 'topic-2')
  deepStrictEqual(topicsRead[1].partitions.length, 1)
})

test('parseResponse correctly processes a successful response', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendArray(
      [{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: 0, timestamp: 1617234567890n, offset: 100n, leaderEpoch: 5 }] }],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode).appendInt64(partition.timestamp).appendInt64(partition.offset).appendInt32(partition.leaderEpoch)
        })
      }
    )
    .appendInt8(0)

  const response = parseResponse(1, 2, 6, Reader.from(writer))

  deepStrictEqual(response, {
    throttleTimeMs: 0,
    topics: [{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: 0, timestamp: 1617234567890n, offset: 100n, leaderEpoch: 5 }] }]
  })
})

test('parseResponse handles partition-level error code', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendArray(
      [{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: 1, timestamp: 0n, offset: -1n, leaderEpoch: -1 }] }],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode).appendInt64(partition.timestamp).appendInt64(partition.offset).appendInt32(partition.leaderEpoch)
        })
      }
    )
    .appendInt8(0)

  throws(
    () => { parseResponse(1, 2, 6, Reader.from(writer)) },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))
      ok(err.errors && typeof err.errors === 'object')
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        topics: [{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: 1, timestamp: 0n, offset: -1n, leaderEpoch: -1 }] }]
      })
      return true
    }
  )
})
