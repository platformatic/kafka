import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { offsetCommitV5, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetCommitV5

function request (metadata: string | null = null) {
  return createRequest('test-group', 1, 'test-member', 'ignored-instance', [
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

test('createRequest serializes basic parameters', () => {
  const writer = request()
  ok(writer instanceof Writer)
  const reader = Reader.from(writer)
  strictEqual(reader.readString(false), 'test-group')
  strictEqual(reader.readInt32(), 1)
  strictEqual(reader.readString(false), 'test-member')
  deepStrictEqual(
    reader.readArray(
      r => ({
        name: r.readString(false),
        partitions: r.readArray(
          r => ({
            partitionIndex: r.readInt32(),
            committedOffset: r.readInt64(),
            committedMetadata: r.readNullableString(false)
          }),
          false,
          false
        )
      }),
      false,
      false
    ),
    [{ name: 'test-topic', partitions: [{ partitionIndex: 0, committedOffset: 100n, committedMetadata: null }] }]
  )
})
test('createRequest serializes committed metadata', () => {
  const reader = Reader.from(request('test-metadata'))
  reader.readString(false)
  reader.readInt32()
  reader.readString(false)
  strictEqual(
    reader.readArray(
      r => {
        r.readString(false)
        return r.readArray(
          r => {
            r.readInt32()
            r.readInt64()
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
test('createRequest ignores the group instance ID while preserving its positional argument', () => {
  const reader = Reader.from(request())
  reader.readString(false)
  reader.readInt32()
  strictEqual(reader.readString(false), 'test-member')
  strictEqual(reader.readArray(r => r.readString(false), false, false)[0], 'test-topic')
})
test('createRequest serializes multiple topics and partitions', () => {
  const reader = Reader.from(
    createRequest('group', 1, 'member', 'ignored', [
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
  deepStrictEqual(
    reader.readArray(
      r => ({
        name: r.readString(false),
        partitions: r.readArray(
          r => ({ index: r.readInt32(), offset: r.readInt64(), metadata: r.readNullableString(false) }),
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
          { index: 0, offset: 100n, metadata: null },
          { index: 1, offset: 200n, metadata: null }
        ]
      },
      { name: 'topic-2', partitions: [{ index: 0, offset: 300n, metadata: null }] }
    ]
  )
})
test('parseResponse processes a successful response', () => {
  deepStrictEqual(parseResponse(1, 8, 5, Reader.from(response())), {
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
        5,
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
  deepStrictEqual(parseResponse(1, 8, 5, Reader.from(response(topics))), { throttleTimeMs: 0, topics })
})
