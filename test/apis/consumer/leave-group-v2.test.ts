import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { leaveGroupV2, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = leaveGroupV2

test('createRequest serializes basic parameters correctly', () => {
  const writer = createRequest('test-group', [{ memberId: 'test-member-1' }])
  ok(writer instanceof Writer)
  const reader = Reader.from(writer)
  deepStrictEqual({ groupId: reader.readString(false), memberId: reader.readString(false) }, { groupId: 'test-group', memberId: 'test-member-1' })
  deepStrictEqual(reader.remaining, 0)

  let headers: unknown[] = []
  api({ send: (...args: unknown[]) => { headers = args } } as never, 'test-group', [{ memberId: 'test-member-1' }])
  deepStrictEqual(headers.slice(4, 6), [false, false])
})

test('createRequest ignores group instance ID', () => {
  const reader = Reader.from(createRequest('test-group', [{ memberId: 'test-member-1', groupInstanceId: 'test-instance-id' }]))
  reader.readString(false)
  deepStrictEqual(reader.readString(false), 'test-member-1')
  deepStrictEqual(reader.remaining, 0)
})

test('createRequest ignores reason', () => {
  const reader = Reader.from(createRequest('test-group', [{ memberId: 'test-member-1', reason: 'Shutting down' }]))
  reader.readString(false)
  deepStrictEqual(reader.readString(false), 'test-member-1')
  deepStrictEqual(reader.remaining, 0)
})

test('createRequest uses the first member', () => {
  const reader = Reader.from(createRequest('test-group', [{ memberId: 'first-member' }, { memberId: 'second-member' }]))
  reader.readString(false)
  deepStrictEqual(reader.readString(false), 'first-member')
  deepStrictEqual(reader.remaining, 0)
})

test('parseResponse correctly processes a successful response', () => {
  deepStrictEqual(parseResponse(1, 13, 2, Reader.from(Writer.create().appendInt32(0).appendInt16(0))), {
    throttleTimeMs: 0,
    errorCode: 0,
    members: []
  })
})

test('parseResponse handles throttling', () => {
  deepStrictEqual(parseResponse(1, 13, 2, Reader.from(Writer.create().appendInt32(100).appendInt16(0))), {
    throttleTimeMs: 100,
    errorCode: 0,
    members: []
  })
})

test('parseResponse handles error code', () => {
  throws(() => {
    parseResponse(1, 13, 2, Reader.from(Writer.create().appendInt32(0).appendInt16(16)))
  }, error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, { throttleTimeMs: 0, errorCode: 16, members: [] })
    return true
  })
})
