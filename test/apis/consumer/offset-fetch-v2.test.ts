import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import * as codec from '../../../src/apis/consumer/offset-fetch-v2.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'
test('OffsetFetch v2 reads its top-level error code without throttle time', () => {
  deepStrictEqual(
    codec.parseResponse(
      1,
      9,
      2,
      Reader.from(
        Writer.create()
          .appendArray([], () => {}, false, false)
          .appendInt16(0)
      )
    ),
    { throttleTimeMs: 0, topics: [], errorCode: 0, groups: [] }
  )
})

test('OffsetFetch v2 encodes null topics for a fetch-all request', () => {
  const reader = Reader.from(codec.createRequest([{ groupId: 'group', topics: null }], false))
  strictEqual(reader.readString(false), 'group')
  strictEqual(reader.readNullableArray(() => undefined, false, false), null)
  strictEqual(reader.remaining, 0)
})

test('OffsetFetch v2 downconverts to the first group and defaults an empty group', () => {
  const groups = [{ groupId: 'group-1', topics: null }, { groupId: 'group-2', topics: null }]
  deepStrictEqual(codec.createRequest(groups, false).buffer, codec.createRequest([groups[0]], false).buffer)
  deepStrictEqual(codec.createRequest([], false).buffer, Buffer.from('0000ffffffff', 'hex'))
})
