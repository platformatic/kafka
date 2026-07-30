import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { deleteGroupsV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = deleteGroupsV0

test('createRequest serializes group names with classic strings', () => {
  const reader = Reader.from(createRequest(['group-1', 'group-2']))

  deepStrictEqual(reader.readArray(r => r.readString(false), false, false), ['group-1', 'group-2'])
  strictEqual(reader.remaining, 0)
})

test('createRequest serializes an empty classic group names array', () => {
  const reader = Reader.from(createRequest([]))

  deepStrictEqual(reader.readArray(() => '', false, false), [])
  strictEqual(reader.remaining, 0)
})

test('uses classic request and response headers without tags', () => {
  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, [])

  deepStrictEqual(
    { key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] },
    { key: 42, version: 0, requestTags: false, responseTags: false }
  )
})

test('parseResponse processes successful classic results', () => {
  const reader = Reader.from(
    Writer.create().appendInt32(0).appendArray(
      [
        { groupId: 'group-1', errorCode: 0 },
        { groupId: 'group-2', errorCode: 0 }
      ],
      (writer, group) => writer.appendString(group.groupId, false).appendInt16(group.errorCode),
      false,
      false
    )
  )

  deepStrictEqual(parseResponse(1, 42, 0, reader), {
    throttleTimeMs: 0,
    results: [
      { groupId: 'group-1', errorCode: 0 },
      { groupId: 'group-2', errorCode: 0 }
    ]
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse preserves throttle time and an empty classic results array', () => {
  const reader = Reader.from(Writer.create().appendInt32(100).appendArray([], () => {}, false, false))

  deepStrictEqual(parseResponse(1, 42, 0, reader), { throttleTimeMs: 100, results: [] })
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws a ResponseError and preserves failed classic results', () => {
  const reader = Reader.from(
    Writer.create().appendInt32(0).appendArray(
      [
        { groupId: 'group-1', errorCode: 0 },
        { groupId: 'group-2', errorCode: 15 }
      ],
      (writer, group) => writer.appendString(group.groupId, false).appendInt16(group.errorCode),
      false,
      false
    )
  )

  throws(() => parseResponse(1, 42, 0, reader), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.errors.map(({ apiCode, apiId, path }) => ({ apiCode, apiId, path })), [
      { apiCode: 15, apiId: 'COORDINATOR_NOT_AVAILABLE', path: '/results/1' }
    ])
    deepStrictEqual(error.response, {
      throttleTimeMs: 0,
      results: [
        { groupId: 'group-1', errorCode: 0 },
        { groupId: 'group-2', errorCode: 15 }
      ]
    })
    strictEqual(reader.remaining, 0)
    return true
  })
})
