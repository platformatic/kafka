import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { describeGroupsV1, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = describeGroupsV1

test('createRequest serializes multiple groups and omits authorized operations from the wire', () => {
  const reader = Reader.from(createRequest(['group-1', 'group-2'], true))
  deepStrictEqual(
    reader.readArray(r => r.readString(false), false, false),
    ['group-1', 'group-2']
  )
  strictEqual(reader.remaining, 0)
})

test('createRequest accepts false and its omitted authorized operations default', () => {
  const falseReader = Reader.from(createRequest(['group-1'], false))
  deepStrictEqual(
    falseReader.readArray(r => r.readString(false), false, false),
    ['group-1']
  )
  strictEqual(falseReader.remaining, 0)
  const defaultReader = Reader.from(createRequest(['group-1']))
  deepStrictEqual(
    defaultReader.readArray(r => r.readString(false), false, false),
    ['group-1']
  )
  strictEqual(defaultReader.remaining, 0)
})

test('createRequest serializes empty and special-character groups', () => {
  const reader = Reader.from(createRequest(['', 'group/1', 'group.with-dots'], true))
  deepStrictEqual(
    reader.readArray(r => r.readString(false), false, false),
    ['', 'group/1', 'group.with-dots']
  )
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
    ['group-1']
  )
  deepStrictEqual(
    { key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] },
    { key: 15, version: 1, requestTags: false, responseTags: false }
  )
})

test('parseResponse normalizes legacy members and unavailable authorized operations', () => {
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
          ),
      false,
      false
    )
  const reader = Reader.from(responseWriter)
  const response = parseResponse(1, 15, 1, reader)
  deepStrictEqual(
    response.groups[0].members.map(member => member.groupInstanceId),
    [null, null]
  )
  strictEqual(response.groups[0].authorizedOperations, -2147483648)
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
          .appendArray([], () => {}, false, false),
      false,
      false
    )
  const response = parseResponse(1, 15, 1, Reader.from(writer))
  deepStrictEqual(
    response.groups.map(group => ({
      groupId: group.groupId,
      members: group.members,
      authorizedOperations: group.authorizedOperations
    })),
    [
      { groupId: 'group-1', members: [], authorizedOperations: -2147483648 },
      { groupId: 'group-2', members: [], authorizedOperations: -2147483648 }
    ]
  )
})

test('parseResponse processes an empty groups response', () => {
  const reader = Reader.from(
    Writer.create()
      .appendInt32(0)
      .appendArray([], () => {}, false, false)
  )
  deepStrictEqual(parseResponse(1, 15, 1, reader), { throttleTimeMs: 0, groups: [] })
  strictEqual(reader.remaining, 0)
})

test('parseResponse preserves throttle time', () => {
  strictEqual(
    parseResponse(
      1,
      15,
      1,
      Reader.from(
        Writer.create()
          .appendInt32(100)
          .appendArray([], () => {}, false, false)
      )
    ).throttleTimeMs,
    100
  )
})

test('parseResponse reports group errors and preserves normalized fields', () => {
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
            .appendArray([], () => {}, false, false),
        false,
        false
      )
  )
  throws(
    () => parseResponse(1, 15, 1, reader),
    error => {
      ok(error instanceof ResponseError)
      deepStrictEqual(
        error.errors.map(({ path, apiCode }) => ({ path, apiCode })),
        [{ path: '/groups/0', apiCode: 15 }]
      )
      strictEqual(error.response.groups[0].authorizedOperations, -2147483648)
      strictEqual(reader.remaining, 0)
      return true
    }
  )
})
