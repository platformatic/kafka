import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { consumerGroupHeartbeatV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = consumerGroupHeartbeatV0

test('createRequest serializes member subscription fields correctly', () => {
  const groupId = 'test-group'
  const memberId = 'test-member-1'
  const memberEpoch = 5
  const instanceId = 'test-instance-id'
  const rackId = 'test-rack-id'
  const rebalanceTimeoutMs = 30000
  const subscribedTopicNames = ['topic1', 'topic2']
  const serverAssignor = 'range'
  const topicPartitions: NonNullable<Parameters<typeof createRequest>[9]> = []

  const writer = createRequest(
    groupId,
    memberId,
    memberEpoch,
    instanceId,
    rackId,
    rebalanceTimeoutMs,
    subscribedTopicNames,
    null,
    serverAssignor,
    topicPartitions
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  deepStrictEqual(
    {
      groupId: reader.readString(),
      memberId: reader.readString(),
      memberEpoch: reader.readInt32(),
      instanceId: reader.readNullableString(),
      rackId: reader.readNullableString(),
      rebalanceTimeoutMs: reader.readInt32()
    },
    {
      groupId,
      memberId,
      memberEpoch,
      instanceId,
      rackId,
      rebalanceTimeoutMs
    }
  )

  const topicsArrayLength = reader.readUnsignedVarInt() - 1
  const topics = []
  for (let i = 0; i < topicsArrayLength; i++) {
    topics.push(reader.readString())
  }
  deepStrictEqual(topics, ['topic1', 'topic2'])

  strictEqual(reader.readNullableString(), serverAssignor)
  strictEqual(reader.readUnsignedVarInt(), 1)
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('createRequest with topic partitions', () => {
  const groupId = 'test-group'
  const memberId = 'test-member-1'
  const memberEpoch = 5
  const instanceId = null
  const rackId = null
  const rebalanceTimeoutMs = 30000
  const subscribedTopicNames = null
  const serverAssignor = null
  const topicPartitions = [
    {
      topicId: '12345678-1234-1234-1234-123456789012',
      partitions: [0, 1, 2]
    },
    {
      topicId: '87654321-4321-4321-4321-210987654321',
      partitions: [3, 4]
    }
  ]

  const writer = createRequest(
    groupId,
    memberId,
    memberEpoch,
    instanceId,
    rackId,
    rebalanceTimeoutMs,
    subscribedTopicNames,
    null,
    serverAssignor,
    topicPartitions
  )

  const reader = Reader.from(writer)

  reader.readString()
  reader.readString()
  reader.readInt32()
  reader.readNullableString()
  reader.readNullableString()
  reader.readInt32()
  strictEqual(reader.readUnsignedVarInt(), 0)
  strictEqual(reader.readNullableString(), null)

  strictEqual(reader.readUnsignedVarInt(), topicPartitions.length + 1)
  for (const topicPartition of topicPartitions) {
    strictEqual(reader.readUUID(), topicPartition.topicId)
    strictEqual(reader.readUnsignedVarInt(), topicPartition.partitions.length + 1)
    for (const partition of topicPartition.partitions) {
      strictEqual(reader.readInt32(), partition)
    }
    reader.readTaggedFields()
  }
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('createRequest preserves null topic partitions distinctly from an empty array', () => {
  const cases: Array<[Parameters<typeof createRequest>[9], number]> = [[null, 0], [[], 1]]
  for (const [topicPartitions, expectedLength] of cases) {
    const reader = Reader.from(createRequest('group', 'member', 1, null, null, 1, null, null, null, topicPartitions))
    reader.readString()
    reader.readString()
    reader.readInt32()
    reader.readNullableString()
    reader.readNullableString()
    reader.readInt32()
    reader.readArray(() => null)
    reader.readNullableString()
    strictEqual(reader.readUnsignedVarInt(), expectedLength)
    reader.readTaggedFields()
    strictEqual(reader.remaining, 0)
  }
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage
    .appendString('test-member-1') // memberId
    .appendInt32(5) // memberEpoch
    .appendInt32(3000) // heartbeatIntervalMs
    .appendInt8(-1) // Assignment non-present (nullable struct indicator)
    .appendInt8(0) // Root tagged fields

  const reader = Reader.from(writer)
  const response = parseResponse(1, 68, 0, reader)

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    memberId: 'test-member-1',
    memberEpoch: 5,
    heartbeatIntervalMs: 3000,
    assignment: null
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse with assignment', () => {
  // Create a response with assignment
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage
    .appendString('test-member-1') // memberId
    .appendInt32(5) // memberEpoch
    .appendInt32(3000) // heartbeatIntervalMs
    .appendInt8(1) // Assignment present (nullable struct indicator)
    .appendArray(
      [
        {
          topicId: '12345678-1234-1234-1234-123456789012',
          partitions: [0, 1, 2]
        },
        {
          topicId: '87654321-4321-4321-4321-210987654321',
          partitions: [3, 4]
        }
      ],
      (w, tp) => {
        w.appendUUID(tp.topicId).appendArray(tp.partitions, (w, p) => w.appendInt32(p), true, false)
      }
    )
    .appendInt8(0) // Assignment tagged fields
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 68, 0, Reader.from(writer))

  // Verify assignment structure
  deepStrictEqual(response.assignment, {
    topicPartitions: [
      {
        topicId: '12345678-1234-1234-1234-123456789012',
        partitions: [0, 1, 2]
      },
      {
        topicId: '87654321-4321-4321-4321-210987654321',
        partitions: [3, 4]
      }
    ]
  })
})

test('parseResponse handles throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage
    .appendString('test-member-1') // memberId
    .appendInt32(5) // memberEpoch
    .appendInt32(3000) // heartbeatIntervalMs
    .appendInt8(1) // Assignment present (nullable struct indicator)
    .appendArray([], () => {}) // Empty topic partitions array
    .appendInt8(0) // Assignment tagged fields
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 68, 0, Reader.from(writer))

  // Verify response structure with throttling
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    errorCode: 0,
    errorMessage: null,
    memberId: 'test-member-1',
    memberEpoch: 5,
    heartbeatIntervalMs: 3000,
    assignment: { topicPartitions: [] }
  })
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(16) // errorCode (e.g., UNKNOWN_MEMBER_ID)
    .appendString('Member ID is not valid') // errorMessage
    .appendString(null) // memberId
    .appendInt32(-1) // memberEpoch
    .appendInt32(3000) // heartbeatIntervalMs
    .appendInt8(-1) // Assignment non-present (nullable struct indicator)
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 68, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        errorCode: 16,
        errorMessage: 'Member ID is not valid',
        memberId: null,
        memberEpoch: -1,
        heartbeatIntervalMs: 3000,
        assignment: null
      })

      return true
    }
  )
})
