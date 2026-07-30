import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import * as codec from '../../../src/apis/consumer/offset-commit-v1.ts'
import { Reader } from '../../../src/protocol/reader.ts'
test('OffsetCommit v1 has the expected API version', () => {
  strictEqual(codec.api.version, 1)
})

test('OffsetCommit v1 includes generation and member identifiers', () => {
  const request = Reader.from(
    codec.createRequest('group', 1, 'member', null, [])
  )

  deepStrictEqual([request.readString(false), request.readInt32(), request.readString(false)], ['group', 1, 'member'])
})

test('OffsetCommit v1 writes the per-partition commit timestamp', () => {
  const request = Reader.from(codec.createRequest('group', 1, 'member', null, [{ name: 'topic', partitions: [{ partitionIndex: 0, committedOffset: 1n, committedLeaderEpoch: -1 }] }]))
  request.readString(false)
  request.readInt32()
  request.readString(false)
  request.readArray(r => {
    r.readString(false)
    r.readArray(r => {
      r.readInt32()
      r.readInt64()
      deepStrictEqual(r.readInt64(), -1n)
    }, false, false)
  }, false, false)
})
