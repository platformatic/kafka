import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import * as codec from '../../../src/apis/consumer/offset-commit-v2.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'
test('OffsetCommit v2 writes the legacy retention sentinel', () => {
  const reader = Reader.from(codec.createRequest('group', 1, 'member', null, []))
  reader.readString(false)
  reader.readInt32()
  reader.readString(false)
  deepStrictEqual(reader.readInt64(), -1n)
})

test('OffsetCommit v2 has no throttle time in the response', () => {
  deepStrictEqual(codec.parseResponse(1, 8, 2, Reader.from(Writer.create().appendArray([], () => {}, false, false))), {
    throttleTimeMs: 0,
    topics: []
  })
})
