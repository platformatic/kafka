import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { offsetForLeaderEpochV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetForLeaderEpochV3

test('createRequest writes the v3 replica ID and current leader epoch', () => {
  const reader = Reader.from(
    createRequest(-1, [{ name: 'orders', partitions: [{ partitionIndex: 2, currentLeaderEpoch: 4, leaderEpoch: 5 }] }])
  )

  deepStrictEqual(reader.readInt32(), -1)
  deepStrictEqual(
    reader.readArray(
      r => ({
        topic: r.readString(false),
        partitions: r.readArray(
          r => ({ partition: r.readInt32(), currentLeaderEpoch: r.readInt32(), leaderEpoch: r.readInt32() }),
          false,
          false
        )
      }),
      false,
      false
    ),
    [{ topic: 'orders', partitions: [{ partition: 2, currentLeaderEpoch: 4, leaderEpoch: 5 }] }]
  )
})

test('parseResponse reads the v3 response schema', () => {
  const response = parseResponse(
    1,
    23,
    3,
    Reader.from(
      Writer.create().appendInt32(100).appendArray(
        [{ topic: 'orders', partitions: [{ errorCode: 0, partition: 2, leaderEpoch: 5, endOffset: 42n }] }],
        (w, topic) => {
          w.appendString(topic.topic, false).appendArray(
            topic.partitions,
            (w, partition) =>
              w.appendInt16(partition.errorCode).appendInt32(partition.partition).appendInt32(partition.leaderEpoch).appendInt64(partition.endOffset),
            false,
            false
          )
        },
        false,
        false
      )
    )
  )

  deepStrictEqual(response, {
    throttleTimeMs: 100,
    topics: [{ topic: 'orders', partitions: [{ errorCode: 0, partition: 2, leaderEpoch: 5, endOffset: 42n }] }]
  })
})

test('parseResponse reports v3 partition errors', () => {
  const response = Writer.create().appendInt32(0).appendArray(
    [{ topic: 'orders', partitions: [{ errorCode: 6, partition: 2, leaderEpoch: -1, endOffset: -1n }] }],
    (w, topic) => {
      w.appendString(topic.topic, false).appendArray(
        topic.partitions,
        (w, partition) =>
          w.appendInt16(partition.errorCode).appendInt32(partition.partition).appendInt32(partition.leaderEpoch).appendInt64(partition.endOffset),
        false,
        false
      )
    },
    false,
    false
  )

  throws(() => parseResponse(1, 23, 3, Reader.from(response)), ResponseError)
})
