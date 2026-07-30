import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { ConsumerGroupStates, GroupTypes } from '../../../src/apis/enumerations.ts'
import { listGroupsV2, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = listGroupsV2

test('createRequest ignores legacy filters and writes an empty classic body', () => {
  const writer = createRequest([...ConsumerGroupStates], [...GroupTypes])

  ok(writer instanceof Writer)
  strictEqual(writer.length, 0)
})

test('uses classic request and response headers without tags', () => {
  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, [], [])

  deepStrictEqual(
    { key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] },
    { key: 16, version: 2, requestTags: false, responseTags: false }
  )
})

test('parseResponse processes groups with classic fields', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendArray(
      ['test-group-1', 'test-group-2'],
      (w, groupId) => w.appendString(groupId, false).appendString(groupId === 'test-group-1' ? 'consumer' : 'connect', false),
      false,
      false
    )
  const reader = Reader.from(writer)

  deepStrictEqual(parseResponse(1, 16, 2, reader), {
    throttleTimeMs: 0,
    errorCode: 0,
    groups: [
      { groupId: 'test-group-1', protocolType: 'consumer', groupState: '', groupType: '' },
      { groupId: 'test-group-2', protocolType: 'connect', groupState: '', groupType: '' }
    ]
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse processes an empty classic groups array', () => {
  const reader = Reader.from(Writer.create().appendInt32(0).appendInt16(0).appendArray([], () => {}, false, false))

  deepStrictEqual(parseResponse(1, 16, 2, reader), { throttleTimeMs: 0, errorCode: 0, groups: [] })
  strictEqual(reader.remaining, 0)
})

test('parseResponse preserves a throttle time', () => {
  const reader = Reader.from(Writer.create().appendInt32(100).appendInt16(0).appendArray([], () => {}, false, false))

  strictEqual(parseResponse(1, 16, 2, reader).throttleTimeMs, 100)
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws a ResponseError and preserves an error response', () => {
  const reader = Reader.from(Writer.create().appendInt32(0).appendInt16(41).appendArray([], () => {}, false, false))

  throws(() => parseResponse(1, 16, 2, reader), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.errors.map(({ apiCode, apiId, path }) => ({ apiCode, apiId, path })), [
      { apiCode: 41, apiId: 'NOT_CONTROLLER', path: '/' }
    ])
    deepStrictEqual(error.response, { throttleTimeMs: 0, errorCode: 41, groups: [] })
    strictEqual(reader.remaining, 0)
    return true
  })
})
