import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { offsetForLeaderEpochV4, Reader, ResponseError } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetForLeaderEpochV4

test('createRequest serializes the flexible request', () => {
  const reader = Reader.from(createRequest(-1, [{ name: 'topic', partitions: [{ partitionIndex: 0, currentLeaderEpoch: 1, leaderEpoch: 2 }] }]))

  strictEqual(reader.readInt32(), -1)
  deepStrictEqual(reader.readArray(r => ({
    name: r.readString(),
    partitions: r.readArray(r => ({
      partitionIndex: r.readInt32(),
      currentLeaderEpoch: r.readInt32(),
      leaderEpoch: r.readInt32()
    }))
  })), [{ name: 'topic', partitions: [{ partitionIndex: 0, currentLeaderEpoch: 1, leaderEpoch: 2 }] }])
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('parseResponse reads error code before partition from a wire fixture', () => {
  const reader = Reader.from(Buffer.from('00000000020b746573742d746f70696302000000000000000000010000000000000064000000', 'hex'))

  deepStrictEqual(parseResponse(1, 23, 4, reader), {
    throttleTimeMs: 0,
    topics: [{ topic: 'test-topic', partitions: [{ errorCode: 0, partition: 0, leaderEpoch: 1, endOffset: 100n }] }]
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse reports partition errors from a wire fixture', () => {
  const reader = Reader.from(Buffer.from('000000000206746f70696302000600000000000000010000000000000064000000', 'hex'))

  throws(() => parseResponse(1, 23, 4, reader), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, {
      throttleTimeMs: 0,
      topics: [{ topic: 'topic', partitions: [{ errorCode: 6, partition: 0, leaderEpoch: 1, endOffset: 100n }] }]
    })
    strictEqual(reader.remaining, 0)
    return true
  })
})
