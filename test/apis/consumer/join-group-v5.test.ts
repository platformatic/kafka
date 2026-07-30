import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { joinGroupV5, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = joinGroupV5

test('createRequest serializes basic parameters correctly', () => {
  const groupId = 'test-group'
  const sessionTimeoutMs = 30000
  const rebalanceTimeoutMs = 60000
  const memberId = '' // Empty for new members
  const groupInstanceId = null
  const protocolType = 'consumer'
  const protocols = [
    { name: 'range', metadata: Buffer.from('metadata-1') },
    { name: 'roundrobin', metadata: null }
  ]

  const writer = createRequest(
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType,
    protocols
  )
  ok(writer instanceof Writer)

  const reader = Reader.from(writer)
  const serializedData = {
    groupId: reader.readString(false),
    sessionTimeoutMs: reader.readInt32(),
    rebalanceTimeoutMs: reader.readInt32(),
    memberId: reader.readString(false),
    groupInstanceId: reader.readNullableString(false),
    protocolType: reader.readString(false)
  }

  deepStrictEqual(serializedData, {
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType
  })

  const protocolsArray = reader.readArray(
    r => ({ name: r.readString(false), metadata: r.readBytes(false) }),
    false,
    false
  )
  deepStrictEqual(protocolsArray, [
    { name: 'range', metadata: Buffer.from('metadata-1') },
    { name: 'roundrobin', metadata: Buffer.alloc(0) }
  ])
  ok(Buffer.isBuffer(protocolsArray[1].metadata))
  deepStrictEqual(reader.remaining, 0) // Legacy versions have no tagged fields

  let headers: unknown[] = []
  api(
    {
      send: (...args: unknown[]) => {
        headers = args
      }
    } as never,
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType,
    []
  )
  deepStrictEqual(headers.slice(4, 6), [false, false])
})

test('createRequest with existing member ID', () => {
  const reader = Reader.from(
    createRequest('test-group', 30000, 60000, 'existing-member-id', null, 'consumer', [
      { name: 'range', metadata: Buffer.from('metadata-1') }
    ])
  )
  reader.readString(false)
  reader.readInt32()
  reader.readInt32()
  deepStrictEqual(reader.readString(false), 'existing-member-id')
})

test('createRequest with group instance ID', () => {
  const reader = Reader.from(
    createRequest('test-group', 30000, 60000, '', 'test-instance-id', 'consumer', [
      { name: 'range', metadata: Buffer.from('metadata-1') }
    ])
  )
  reader.readString(false)
  reader.readInt32()
  reader.readInt32()
  reader.readString(false)
  deepStrictEqual(reader.readNullableString(false), 'test-instance-id')
})

test('createRequest with multiple protocols', () => {
  const reader = Reader.from(
    createRequest('test-group', 30000, 60000, '', null, 'consumer', [
      { name: 'range', metadata: Buffer.from('metadata-1') },
      { name: 'roundrobin', metadata: Buffer.from('metadata-2') }
    ])
  )
  reader.readString(false)
  reader.readInt32()
  reader.readInt32()
  reader.readString(false)
  reader.readNullableString(false)
  reader.readString(false)
  deepStrictEqual(
    reader.readArray(r => ({ name: r.readString(false), metadata: r.readBytes(false) }), false, false),
    [
      { name: 'range', metadata: Buffer.from('metadata-1') },
      { name: 'roundrobin', metadata: Buffer.from('metadata-2') }
    ]
  )
})

test('parseResponse correctly processes a successful response for a follower', () => {
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt32(5) // generationId
    .appendString('range', false) // protocolName
    .appendString('leader-member-id', false) // leader
    .appendString('follower-member-id', false) // memberId
    .appendArray([], () => {}, false, false) // Followers have no member info

  deepStrictEqual(parseResponse(1, 11, 5, Reader.from(writer)), {
    throttleTimeMs: 0,
    errorCode: 0,
    generationId: 5,
    protocolName: 'range',
    protocolType: null,
    leader: 'leader-member-id',
    skipAssignment: false,
    memberId: 'follower-member-id',
    members: []
  })
})

test('parseResponse correctly processes a successful response for a leader', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendInt32(5)
    .appendString('range', false)
    .appendString('leader-member-id', false)
    .appendString('leader-member-id', false)
    .appendArray(
      [
        {
          memberId: 'leader-member-id',
          groupInstanceId: 'leader-instance-id',
          metadata: Buffer.from('leader-metadata')
        },
        { memberId: 'follower-member-id', groupInstanceId: null, metadata: Buffer.from('follower-metadata') }
      ],
      (w, member) =>
        w
          .appendString(member.memberId, false)
          .appendString(member.groupInstanceId, false)
          .appendBytes(member.metadata, false),
      false,
      false
    )

  const response = parseResponse(1, 11, 5, Reader.from(writer))
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    generationId: 5,
    protocolName: 'range',
    protocolType: null,
    leader: 'leader-member-id',
    skipAssignment: false,
    memberId: 'leader-member-id',
    members: [
      { memberId: 'leader-member-id', groupInstanceId: 'leader-instance-id', metadata: Buffer.from('leader-metadata') },
      { memberId: 'follower-member-id', groupInstanceId: null, metadata: Buffer.from('follower-metadata') }
    ]
  })
  ok(Buffer.isBuffer(response.members[0].metadata))
  ok(Buffer.isBuffer(response.members[1].metadata))
})

test('parseResponse throws error on non-zero error code', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(15) // COORDINATOR_NOT_AVAILABLE
    .appendInt32(-1)
    .appendString(null, false)
    .appendString('', false)
    .appendString('', false)
    .appendArray([], () => {}, false, false)

  throws(
    () => parseResponse(1, 11, 5, Reader.from(writer)),
    (err: unknown) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))
      ok(err.errors && typeof err.errors === 'object')
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        errorCode: 15,
        generationId: -1,
        protocolName: '',
        protocolType: null,
        leader: '',
        skipAssignment: false,
        memberId: '',
        members: []
      })
      return true
    }
  )
})
