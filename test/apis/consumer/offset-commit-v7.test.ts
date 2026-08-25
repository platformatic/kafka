import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { offsetCommitV7, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetCommitV7

function request (metadata: string | null = null, groupInstanceId: string | null = null) {
  return createRequest('test-group', 1, 'test-member', groupInstanceId, [
    {
      name: 'test-topic',
      partitions: [{ partitionIndex: 0, committedOffset: 100n, committedLeaderEpoch: 7, committedMetadata: metadata }]
    }
  ])
}
function response (topics = [{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: 0 }] }]) {
  return Writer.create()
    .appendInt32(0)
    .appendArray(
      topics,
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
          },
          false,
          false
        )
      },
      false,
      false
    )
}

test('createRequest serializes basic parameters and the committed leader epoch', () => {
  const writer = request()
  ok(writer instanceof Writer)
  const reader = Reader.from(writer)
  strictEqual(reader.readString(false), 'test-group')
  strictEqual(reader.readInt32(), 1)
  strictEqual(reader.readString(false), 'test-member')
  strictEqual(reader.readNullableString(false), null)
  deepStrictEqual(
    reader.readArray(
      r => ({
        name: r.readString(false),
        partitions: r.readArray(
          r => ({
            partitionIndex: r.readInt32(),
            committedOffset: r.readInt64(),
            committedLeaderEpoch: r.readInt32(),
            committedMetadata: r.readNullableString(false)
          }),
          false,
          false
        )
      }),
      false,
      false
    ),
    [
      {
        name: 'test-topic',
        partitions: [{ partitionIndex: 0, committedOffset: 100n, committedLeaderEpoch: 7, committedMetadata: null }]
      }
    ]
  )
})
test('createRequest serializes committed metadata', () => {
  const reader = Reader.from(request('test-metadata'))
  reader.readString(false)
  reader.readInt32()
  reader.readString(false)
  reader.readNullableString(false)
  strictEqual(
    reader.readArray(
      r => {
        r.readString(false)
        return r.readArray(
          r => {
            r.readInt32()
            r.readInt64()
            r.readInt32()
            return r.readString(false)
          },
          false,
          false
        )
      },
      false,
      false
    )[0][0],
    'test-metadata'
  )
})
test('createRequest serializes the group instance ID', () => {
  const reader = Reader.from(request(null, 'test-instance'))
  reader.readString(false)
  reader.readInt32()
  reader.readString(false)
  strictEqual(reader.readString(false), 'test-instance')
})
test('createRequest serializes multiple topics and partitions', () => {
  const reader = Reader.from(
    createRequest('group', 1, 'member', 'instance', [
      {
        name: 'topic-1',
        partitions: [
          { partitionIndex: 0, committedOffset: 100n, committedLeaderEpoch: 1, committedMetadata: null },
          { partitionIndex: 1, committedOffset: 200n, committedLeaderEpoch: 2, committedMetadata: null }
        ]
      },
      {
        name: 'topic-2',
        partitions: [{ partitionIndex: 0, committedOffset: 300n, committedLeaderEpoch: 3, committedMetadata: null }]
      }
    ])
  )
  reader.readString(false)
  reader.readInt32()
  reader.readString(false)
  reader.readString(false)
  deepStrictEqual(
    reader.readArray(
      r => ({
        name: r.readString(false),
        partitions: r.readArray(
          r => ({
            index: r.readInt32(),
            offset: r.readInt64(),
            epoch: r.readInt32(),
            metadata: r.readNullableString(false)
          }),
          false,
          false
        )
      }),
      false,
      false
    ),
    [
      {
        name: 'topic-1',
        partitions: [
          { index: 0, offset: 100n, epoch: 1, metadata: null },
          { index: 1, offset: 200n, epoch: 2, metadata: null }
        ]
      },
      { name: 'topic-2', partitions: [{ index: 0, offset: 300n, epoch: 3, metadata: null }] }
    ]
  )
})
test('parseResponse processes a successful response', () => {
  deepStrictEqual(parseResponse(1, 8, 7, Reader.from(response())), {
    throttleTimeMs: 0,
    topics: [{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: 0 }] }]
  })
})
test('parseResponse preserves a partition-level error response', () => {
  throws(
    () =>
      parseResponse(
        1,
        8,
        7,
        Reader.from(response([{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: 22 }] }]))
      ),
    error => {
      ok(error instanceof ResponseError)
      deepStrictEqual(error.response, {
        throttleTimeMs: 0,
        topics: [{ name: 'test-topic', partitions: [{ partitionIndex: 0, errorCode: 22 }] }]
      })
      return true
    }
  )
})
test('parseResponse processes multiple topics and partitions', () => {
  const topics = [
    {
      name: 'topic-1',
      partitions: [
        { partitionIndex: 0, errorCode: 0 },
        { partitionIndex: 1, errorCode: 0 }
      ]
    },
    { name: 'topic-2', partitions: [{ partitionIndex: 0, errorCode: 0 }] }
  ]
  deepStrictEqual(parseResponse(1, 8, 7, Reader.from(response(topics))), { throttleTimeMs: 0, topics })
})
