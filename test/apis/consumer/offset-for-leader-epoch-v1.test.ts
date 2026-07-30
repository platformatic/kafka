import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { offsetForLeaderEpochV1, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetForLeaderEpochV1

test('createRequest writes the v1 request schema', () => {
  const reader = Reader.from(createRequest(-1, [{ name: 'orders', partitions: [{ partitionIndex: 2, currentLeaderEpoch: -1, leaderEpoch: 5 }] }]))

  deepStrictEqual(
    reader.readArray(
      r => ({
        topic: r.readString(false),
        partitions: r.readArray(r => ({ partition: r.readInt32(), leaderEpoch: r.readInt32() }), false, false)
      }),
      false,
      false
    ),
    [{ topic: 'orders', partitions: [{ partition: 2, leaderEpoch: 5 }] }]
  )
})

test('parseResponse reads the v1 response leader epoch', () => {
  const response = parseResponse(
    1,
    23,
    1,
    Reader.from(
      Writer.create().appendArray(
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
    throttleTimeMs: 0, topics: [{ topic: 'orders', partitions: [{ errorCode: 0, partition: 2, leaderEpoch: 5, endOffset: 42n }] }]
  })
})

test('parseResponse reports v1 partition errors', () => {
  const response = Writer.create().appendArray(
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

  throws(() => parseResponse(1, 23, 1, Reader.from(response)), ResponseError)
})
