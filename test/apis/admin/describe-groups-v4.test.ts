import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { describeGroupsV4, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = describeGroupsV4

test('createRequest serializes groups and authorized operations', () => {
  const writer = createRequest(['group-1', 'group-2'], true)
  ok(writer instanceof Writer)
  const reader = Reader.from(writer)
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
    { key: 15, version: 4, requestTags: false, responseTags: false }
  )
})

test('parseResponse processes members, static instances, and authorized operations', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendArray(
      [
        {
          id: 'group-1',
          members: [
            { id: 'member-1', instance: null },
            { id: 'member-2', instance: 'instance-2' }
          ]
        }
      ],
      (w, group) => {
        w.appendInt16(0)
          .appendString(group.id, false)
          .appendString('Stable', false)
          .appendString('consumer', false)
          .appendString('range', false)
          .appendArray(
            group.members,
            (w, member) => {
              w.appendString(member.id, false)
                .appendString(member.instance, false)
                .appendString('client', false)
                .appendString('host', false)
                .appendBytes(Buffer.from('metadata'), false)
                .appendBytes(Buffer.from('assignment'), false)
            },
            false,
            false
          )
          .appendInt32(3)
      },
      false,
      false
    )
  const reader = Reader.from(writer)
  deepStrictEqual(parseResponse(1, 15, 4, reader), {
    throttleTimeMs: 0,
    groups: [
      {
        errorCode: 0,
        groupId: 'group-1',
        groupState: 'Stable',
        protocolType: 'consumer',
        protocolData: 'range',
        members: [
          {
            memberId: 'member-1',
            groupInstanceId: null,
            clientId: 'client',
            clientHost: 'host',
            memberMetadata: Buffer.from('metadata'),
            memberAssignment: Buffer.from('assignment')
          },
          {
            memberId: 'member-2',
            groupInstanceId: 'instance-2',
            clientId: 'client',
            clientHost: 'host',
            memberMetadata: Buffer.from('metadata'),
            memberAssignment: Buffer.from('assignment')
          }
        ],
        authorizedOperations: 3
      }
    ]
  })
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
  const response = parseResponse(1, 15, 4, Reader.from(writer))
  deepStrictEqual(
    response.groups.map(group => ({
      groupId: group.groupId,
      members: group.members,
      authorizedOperations: group.authorizedOperations
    })),
    [
      { groupId: 'group-1', members: [], authorizedOperations: 0 },
      { groupId: 'group-2', members: [], authorizedOperations: 0 }
    ]
  )
})

test('parseResponse processes an empty groups response', () => {
  const reader = Reader.from(
    Writer.create()
      .appendInt32(0)
      .appendArray([], () => {}, false, false)
  )
  deepStrictEqual(parseResponse(1, 15, 4, reader), { throttleTimeMs: 0, groups: [] })
  strictEqual(reader.remaining, 0)
})

test('parseResponse preserves throttle time', () => {
  const writer = Writer.create()
    .appendInt32(100)
    .appendArray([], () => {}, false, false)
  strictEqual(parseResponse(1, 15, 4, Reader.from(writer)).throttleTimeMs, 100)
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
    () => parseResponse(1, 15, 4, reader),
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
