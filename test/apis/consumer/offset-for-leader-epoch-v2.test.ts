import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { offsetForLeaderEpochV2, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetForLeaderEpochV2

test('createRequest writes the v2 current leader epoch', () => {
  const reader = Reader.from(
    createRequest(-1, [{ name: 'orders', partitions: [{ partitionIndex: 2, currentLeaderEpoch: 4, leaderEpoch: 5 }] }])
  )

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

test('parseResponse reads the v2 throttle time', () => {
  const response = parseResponse(
    1,
    23,
    2,
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

test('parseResponse reports v2 partition errors', () => {
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

  throws(() => parseResponse(1, 23, 2, Reader.from(response)), ResponseError)
})
