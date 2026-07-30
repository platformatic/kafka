import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import * as codec from '../../../src/apis/consumer/offset-fetch-v0.ts'
import { UserError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'
test('OffsetFetch v0 normalizes omitted response fields', () => {
  const response = codec.parseResponse(1, 9, 0, Reader.from(Writer.create().appendArray([], () => {}, false, false)))
  deepStrictEqual(response, { throttleTimeMs: 0, topics: [], errorCode: 0, groups: [] })
})

test('OffsetFetch v0 encodes an empty topics array rather than nullable topics', () => {
  const writer = codec.createRequest([{ groupId: 'group', topics: [] }], false)
  const reader = Reader.from(writer)
  strictEqual(reader.readString(false), 'group')
  strictEqual(reader.readInt32(), 0)
  strictEqual(reader.remaining, 0)
})

test('OffsetFetch v0 rejects null or omitted topics because fetch-all is unsupported', () => {
  for (const topics of [null, undefined]) {
    throws(
      () => codec.createRequest([{ groupId: 'group', topics }], false),
      error => error instanceof UserError && error.message === 'OffsetFetch v0 does not support fetching all offsets.'
    )
  }
})

test('OffsetFetch v0 downconverts to the first group and defaults an empty group', () => {
  const groups = [{ groupId: 'group-1', topics: [] }, { groupId: 'group-2', topics: [] }]
  deepStrictEqual(codec.createRequest(groups, false).buffer, codec.createRequest([groups[0]], false).buffer)
  deepStrictEqual(codec.createRequest([], false).buffer, Buffer.from('000000000000', 'hex'))
})
