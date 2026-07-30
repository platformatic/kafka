import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { describeGroupsV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = describeGroupsV3

test('createRequest serializes groups and authorized operations', () => {
  const reader = Reader.from(createRequest(['group-1', 'group-2'], true))
  deepStrictEqual(
    reader.readArray(r => r.readString(false), false, false),
    ['group-1', 'group-2']
  )
  strictEqual(reader.readBoolean(), true)
  strictEqual(reader.remaining, 0)
})

test('createRequest serializes false for authorized operations', () => {
  const reader = Reader.from(createRequest(['group-1'], false))
  deepStrictEqual(
    reader.readArray(r => r.readString(false), false, false),
    ['group-1']
  )
  strictEqual(reader.readBoolean(), false)
  strictEqual(reader.remaining, 0)
})

test('createRequest serializes empty and special-character groups', () => {
  const reader = Reader.from(createRequest(['', 'group/1', 'group.with-dots'], false))
  deepStrictEqual(
    reader.readArray(r => r.readString(false), false, false),
    ['', 'group/1', 'group.with-dots']
  )
  strictEqual(reader.readBoolean(), false)
  strictEqual(reader.remaining, 0)
})

test('uses classic request and response headers without tags', () => {
  let sent: unknown[] = []
  api(
    {
      send: (...args: unknown[]) => {
        sent = args
      }
    } as never,
    ['group-1'],
    false
  )
  deepStrictEqual(
    { key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] },
    { key: 15, version: 3, requestTags: false, responseTags: false }
  )
})

test('parseResponse normalizes missing group instance IDs and parses authorized operations', () => {
  const writer = Writer.create().appendArray(
    ['member-1', 'member-2'],
    (w, memberId) =>
      w
        .appendString(memberId, false)
        .appendString('client', false)
        .appendString('host', false)
        .appendBytes(Buffer.from('metadata'), false)
        .appendBytes(Buffer.from('assignment'), false),
    false,
    false
  )
  const members = Reader.from(writer).readArray(
    r => ({
      memberId: r.readString(false),
      clientId: r.readString(false),
      clientHost: r.readString(false),
      memberMetadata: r.readBytes(false),
      memberAssignment: r.readBytes(false)
    }),
    false,
    false
  )
  const responseWriter = Writer.create()
    .appendInt32(0)
    .appendArray(
      [{ id: 'group-1', members }],
      (w, group) =>
        w
          .appendInt16(0)
          .appendString(group.id, false)
          .appendString('Stable', false)
          .appendString('consumer', false)
          .appendString('range', false)
          .appendArray(
            group.members,
            (w, member) =>
              w
                .appendString(member.memberId, false)
                .appendString(member.clientId, false)
                .appendString(member.clientHost, false)
                .appendBytes(member.memberMetadata, false)
                .appendBytes(member.memberAssignment, false),
            false,
            false
          )
          .appendInt32(3),
      false,
      false
    )
  const reader = Reader.from(responseWriter)
  const response = parseResponse(1, 15, 3, reader)
  deepStrictEqual(
    response.groups[0].members.map(member => member.groupInstanceId),
    [null, null]
  )
  strictEqual(response.groups[0].authorizedOperations, 3)
  strictEqual(reader.remaining, 0)
})

test('parseResponse processes multiple groups and empty members', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendArray(
      ['group-1', 'group-2'],
      (w, id) =>
        w
          .appendInt16(0)
          .appendString(id, false)
          .appendString('Empty', false)
          .appendString('', false)
          .appendString('', false)
          .appendArray([], () => {}, false, false)
          .appendInt32(0),
      false,
      false
    )
  deepStrictEqual(
    parseResponse(1, 15, 3, Reader.from(writer)).groups.map(group => group.groupId),
    ['group-1', 'group-2']
  )
})

test('parseResponse processes an empty groups response', () => {
  const reader = Reader.from(
    Writer.create()
      .appendInt32(0)
      .appendArray([], () => {}, false, false)
  )
  deepStrictEqual(parseResponse(1, 15, 3, reader), { throttleTimeMs: 0, groups: [] })
  strictEqual(reader.remaining, 0)
})

test('parseResponse preserves throttle time', () => {
  strictEqual(
    parseResponse(
      1,
      15,
      3,
      Reader.from(
        Writer.create()
          .appendInt32(100)
          .appendArray([], () => {}, false, false)
      )
    ).throttleTimeMs,
    100
  )
})

test('parseResponse reports group errors and preserves their response', () => {
  const reader = Reader.from(
    Writer.create()
      .appendInt32(0)
      .appendArray(
        ['missing'],
        (w, id) =>
          w
            .appendInt16(15)
            .appendString(id, false)
            .appendString('', false)
            .appendString('', false)
            .appendString('', false)
            .appendArray([], () => {}, false, false)
            .appendInt32(0),
        false,
        false
      )
  )
  throws(
    () => parseResponse(1, 15, 3, reader),
    error => {
      ok(error instanceof ResponseError)
      deepStrictEqual(
        error.errors.map(({ path, apiCode }) => ({ path, apiCode })),
        [{ path: '/groups/0', apiCode: 15 }]
      )
      deepStrictEqual(error.response.groups[0].authorizedOperations, 0)
      strictEqual(reader.remaining, 0)
      return true
    }
  )
})
