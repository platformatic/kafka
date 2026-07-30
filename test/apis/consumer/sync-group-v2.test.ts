import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, syncGroupV2, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = syncGroupV2

test('createRequest serializes basic parameters correctly', () => {
  const writer = createRequest('test-group', 5, 'test-member-1', null, 'consumer', 'range', [])
  const reader = Reader.from(writer)

  ok(writer instanceof Writer)
  deepStrictEqual({ groupId: reader.readString(false), generationId: reader.readInt32(), memberId: reader.readString(false) }, {
    groupId: 'test-group', generationId: 5, memberId: 'test-member-1'
  })
  strictEqual(reader.readInt32(), 0)
  strictEqual(reader.remaining, 0)
})

test('createRequest with assignments', () => {
  const writer = createRequest('test-group', 5, 'test-member-1', null, 'consumer', 'range', [
    { memberId: 'member-1', assignment: Buffer.from('assignment-data-1') },
    { memberId: 'member-2', assignment: Buffer.from('assignment-data-2') }
  ])
  const reader = Reader.from(writer)

  reader.readString(false)
  reader.readInt32()
  reader.readString(false)
  deepStrictEqual(reader.readArray(r => ({ memberId: r.readString(false), assignment: r.readBytes(false) }), false, false), [
    { memberId: 'member-1', assignment: Buffer.from('assignment-data-1') },
    { memberId: 'member-2', assignment: Buffer.from('assignment-data-2') }
  ])
  strictEqual(reader.remaining, 0)
})

test('createRequest ignores group instance and protocol fields', () => {
  const writer = createRequest('test-group', 5, 'test-member-1', 'test-instance-id', 'consumer', 'range', [])
  const reader = Reader.from(writer)

  deepStrictEqual({ groupId: reader.readString(false), generationId: reader.readInt32(), memberId: reader.readString(false) }, {
    groupId: 'test-group', generationId: 5, memberId: 'test-member-1'
  })
  strictEqual(reader.readInt32(), 0)
  strictEqual(reader.remaining, 0)
})

test('uses classic request and response headers without tags', () => {
  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, 'test-group', 5, 'test-member-1', null, 'consumer', 'range', [])

  deepStrictEqual({ key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] }, {
    key: 14, version: 2, requestTags: false, responseTags: false
  })
})

test('parseResponse correctly processes a successful response', () => {
  const reader = Reader.from(Writer.create().appendInt32(0).appendInt16(0).appendBytes(Buffer.from('test-assignment-data'), false))

  deepStrictEqual(parseResponse(1, 14, 2, reader), {
    throttleTimeMs: 0, errorCode: 0, protocolType: null, protocolName: null, assignment: Buffer.from('test-assignment-data')
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse handles throttling', () => {
  const reader = Reader.from(Writer.create().appendInt32(100).appendInt16(0).appendBytes(Buffer.from('test-assignment-data'), false))

  deepStrictEqual(parseResponse(1, 14, 2, reader), {
    throttleTimeMs: 100, errorCode: 0, protocolType: null, protocolName: null, assignment: Buffer.from('test-assignment-data')
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws error on non-zero error code', () => {
  const reader = Reader.from(Writer.create().appendInt32(0).appendInt16(16).appendBytes(Buffer.alloc(0), false))

  throws(() => parseResponse(1, 14, 2, reader), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, { throttleTimeMs: 0, errorCode: 16, protocolType: null, protocolName: null, assignment: Buffer.alloc(0) })
    strictEqual(reader.remaining, 0)
    return true
  })
})
