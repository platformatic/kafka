import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { leaveGroupV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = leaveGroupV3

test('createRequest serializes basic parameters correctly', () => {
  const writer = createRequest('test-group', [{ memberId: 'test-member-1', groupInstanceId: null }])
  ok(writer instanceof Writer)

  const reader = Reader.from(writer)
  deepStrictEqual(reader.readString(false), 'test-group')
  deepStrictEqual(reader.readArray(r => ({ memberId: r.readString(false), groupInstanceId: r.readNullableString(false) }), false, false), [
    { memberId: 'test-member-1', groupInstanceId: null }
  ])
  deepStrictEqual(reader.remaining, 0)

  let headers: unknown[] = []
  api({ send: (...args: unknown[]) => { headers = args } } as never, 'test-group', [{ memberId: 'test-member-1' }])
  deepStrictEqual(headers.slice(4, 6), [false, false])
})

test('createRequest serializes group instance ID', () => {
  const reader = Reader.from(createRequest('test-group', [{ memberId: 'test-member-1', groupInstanceId: 'test-instance-id' }]))
  reader.readString(false)
  deepStrictEqual(reader.readArray(r => ({ memberId: r.readString(false), groupInstanceId: r.readNullableString(false) }), false, false), [
    { memberId: 'test-member-1', groupInstanceId: 'test-instance-id' }
  ])
  deepStrictEqual(reader.remaining, 0)
})

test('createRequest ignores reason', () => {
  const reader = Reader.from(createRequest('test-group', [{ memberId: 'test-member-1', groupInstanceId: null, reason: 'Shutting down' }]))
  reader.readString(false)
  deepStrictEqual(reader.readArray(r => ({ memberId: r.readString(false), groupInstanceId: r.readNullableString(false) }), false, false), [
    { memberId: 'test-member-1', groupInstanceId: null }
  ])
  deepStrictEqual(reader.remaining, 0)
})

test('createRequest serializes multiple members', () => {
  const reader = Reader.from(createRequest('test-group', [
    { memberId: 'test-member-1', groupInstanceId: null },
    { memberId: 'test-member-2', groupInstanceId: 'test-instance-id' }
  ]))
  reader.readString(false)
  deepStrictEqual(reader.readArray(r => ({ memberId: r.readString(false), groupInstanceId: r.readNullableString(false) }), false, false), [
    { memberId: 'test-member-1', groupInstanceId: null },
    { memberId: 'test-member-2', groupInstanceId: 'test-instance-id' }
  ])
  deepStrictEqual(reader.remaining, 0)
})

test('parseResponse reads a non-null member ID', () => {
  const writer = Writer.create().appendInt32(0).appendInt16(0).appendArray([
    { memberId: 'test-member-1', groupInstanceId: null, errorCode: 0 }
  ], (w, member) => {
    w.appendString(member.memberId, false).appendString(member.groupInstanceId, false).appendInt16(member.errorCode)
  }, false, false)

  deepStrictEqual(parseResponse(1, 13, 3, Reader.from(writer)), {
    throttleTimeMs: 0,
    errorCode: 0,
    members: [{ memberId: 'test-member-1', groupInstanceId: null, errorCode: 0 }]
  })
})

test('parseResponse handles top-level error code', () => {
  const writer = Writer.create().appendInt32(0).appendInt16(15).appendArray([], () => {}, false, false)
  throws(() => {
    parseResponse(1, 13, 3, Reader.from(writer))
  }, error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, { throttleTimeMs: 0, errorCode: 15, members: [] })
    return true
  })
})

test('parseResponse handles member-level error code', () => {
  const writer = Writer.create().appendInt32(0).appendInt16(0).appendArray([
    { memberId: 'test-member-1', groupInstanceId: null, errorCode: 16 }
  ], (w, member) => {
    w.appendString(member.memberId, false).appendString(member.groupInstanceId, false).appendInt16(member.errorCode)
  }, false, false)

  throws(() => {
    parseResponse(1, 13, 3, Reader.from(writer))
  }, error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.errors[0].path, '/members/0')
    deepStrictEqual(error.response, {
      throttleTimeMs: 0,
      errorCode: 0,
      members: [{ memberId: 'test-member-1', groupInstanceId: null, errorCode: 16 }]
    })
    return true
  })
})
