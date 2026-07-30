import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { ConsumerGroupStates, GroupTypes } from '../../../src/apis/enumerations.ts'
import { listGroupsV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = listGroupsV3

test('createRequest ignores legacy filters and writes the flexible empty body', () => {
  const writer = createRequest([...ConsumerGroupStates], [...GroupTypes])

  ok(writer instanceof Writer)
  deepStrictEqual(writer.buffer, Buffer.from('00', 'hex'))
})

test('uses flexible request and response headers with tags', () => {
  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, [], [])

  deepStrictEqual(
    { key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] },
    { key: 16, version: 3, requestTags: true, responseTags: true }
  )
})

test('parseResponse processes groups with compact fields and tagged fields', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendArray(['test-group-1', 'test-group-2'], (w, groupId) => {
      w.appendString(groupId).appendString(groupId === 'test-group-1' ? 'consumer' : 'connect')
    })
    .appendTaggedFields()
  const reader = Reader.from(writer)

  deepStrictEqual(parseResponse(1, 16, 3, reader), {
    throttleTimeMs: 0,
    errorCode: 0,
    groups: [
      { groupId: 'test-group-1', protocolType: 'consumer', groupState: '', groupType: '' },
      { groupId: 'test-group-2', protocolType: 'connect', groupState: '', groupType: '' }
    ]
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse processes an empty compact groups array and tagged fields', () => {
  const reader = Reader.from(Writer.create().appendInt32(0).appendInt16(0).appendArray([], () => {}).appendTaggedFields())

  deepStrictEqual(parseResponse(1, 16, 3, reader), { throttleTimeMs: 0, errorCode: 0, groups: [] })
  strictEqual(reader.remaining, 0)
})

test('parseResponse preserves a throttle time', () => {
  const reader = Reader.from(Writer.create().appendInt32(100).appendInt16(0).appendArray([], () => {}).appendTaggedFields())

  strictEqual(parseResponse(1, 16, 3, reader).throttleTimeMs, 100)
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws a ResponseError and preserves an error response', () => {
  const reader = Reader.from(Writer.create().appendInt32(0).appendInt16(41).appendArray([], () => {}).appendTaggedFields())

  throws(() => parseResponse(1, 16, 3, reader), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.errors.map(({ apiCode, apiId, path }) => ({ apiCode, apiId, path })), [
      { apiCode: 41, apiId: 'NOT_CONTROLLER', path: '/' }
    ])
    deepStrictEqual(error.response, { throttleTimeMs: 0, errorCode: 41, groups: [] })
    strictEqual(reader.remaining, 0)
    return true
  })
})
