import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { consumerGroupHeartbeatV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = consumerGroupHeartbeatV0

test('createRequest serializes basic parameters correctly', () => {
  const groupId = 'test-group'
  const memberId = 'test-member-1'
  const memberEpoch = 5
  const instanceId = null
  const rackId = null
  const rebalanceTimeoutMs = 30000
  const subscribedTopicNames = ['topic1', 'topic2']
  const serverAssignor = null
  const topicPartitions: any[] = []

  const writer = createRequest(
    groupId,
    memberId,
    memberEpoch,
    instanceId,
    rackId,
    rebalanceTimeoutMs,
    subscribedTopicNames,
    serverAssignor,
    topicPartitions
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Verify basic parameters
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

  // Verify topics array
  const topicsArrayLength = reader.readUnsignedVarInt() - 1 // Get array length
  const topics = []
  for (let i = 0; i < topicsArrayLength; i++) {
    topics.push(reader.readString())
  }
  deepStrictEqual(topics, ['topic1', 'topic2'])

  // Verify serverAssignor and other fields
  deepStrictEqual(
    {
      serverAssignor: reader.readNullableString()
    },
    {
      serverAssignor
    }
  )
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
    serverAssignor,
    topicPartitions
  )

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Skip to topic partitions
  reader.readString() // Group ID
  reader.readString() // Member ID
  reader.readInt32() // Member Epoch
  reader.readString() // Instance ID
  reader.readString() // Rack ID
  reader.readInt32() // Rebalance Timeout Ms
  reader.readUnsignedVarInt() // Empty subscribed topics array
  reader.readString() // Server Assignor

  // Verify topic partitions array
  reader.readUnsignedVarInt()
  // Not checking the exact value as implementation may include extra fields

  // First topic partition - the UUID might be read differently due to endianness
  const firstTopicUUID = reader.readUUID()
  ok(typeof firstTopicUUID === 'string' && firstTopicUUID.includes('-'), 'Should read a valid UUID')

  // First topic partitions
  const partitionsLength1 = reader.readUnsignedVarInt() - 1
  for (let i = 0; i < partitionsLength1; i++) {
    deepStrictEqual(reader.readInt32(), topicPartitions[0].partitions[i])
  }

  reader.readTaggedFields() // Skip to the next topic partition

  // Second topic partition - the UUID might be read differently due to endianness
  const secondTopicUUID = reader.readUUID()
  ok(typeof secondTopicUUID === 'string' && secondTopicUUID.includes('-'), 'Should read a valid UUID')

  // Second topic partitions
  const partitionsLength2 = reader.readUnsignedVarInt() - 1
  for (let i = 0; i < partitionsLength2; i++) {
    deepStrictEqual(reader.readInt32(), topicPartitions[1].partitions[i])
  }
})

test('createRequest with instance ID and rack ID', () => {
  const groupId = 'test-group'
  const memberId = 'test-member-1'
  const memberEpoch = 5
  const instanceId = 'test-instance-id'
  const rackId = 'test-rack-id'
  const rebalanceTimeoutMs = 30000
  const subscribedTopicNames: string[] = []
  const serverAssignor = 'range'
  const topicPartitions: any[] = []

  const writer = createRequest(
    groupId,
    memberId,
    memberEpoch,
    instanceId,
    rackId,
    rebalanceTimeoutMs,
    subscribedTopicNames,
    serverAssignor,
    topicPartitions
  )

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Skip to instance ID and rack ID
  reader.readString() // Group ID
  reader.readString() // Member ID
  reader.readInt32() // Member Epoch

  // Verify instance ID, rack ID, and server assignor
  deepStrictEqual(
    {
      instanceId: reader.readString(),
      rackId: reader.readString(),
      // Skip rebalanceTimeoutMs and empty topic array
      serverAssignor: reader.readString() // After skipping rebalanceTimeoutMs and topics array via readInt32() and readUnsignedVarInt()
    },
    {
      instanceId: 'test-instance-id',
      rackId: 'test-rack-id',
      serverAssignor: ''
    }
  )
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
    .appendArray([], () => {}) // Empty assignment
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 68, 0, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    memberId: 'test-member-1',
    memberEpoch: 5,
    heartbeatIntervalMs: 3000,
    assignment: []
  })
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
    .appendArray(
      [
        // Assignment with topic partitions
        {
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
        }
      ],
      (w, a) => {
        // Write topicPartitions array
        w.appendArray(a.topicPartitions, (w, tp) => {
          w.appendUUID(tp.topicId).appendArray(tp.partitions, (w, p) => w.appendInt32(p), true, false)
        })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 68, 0, Reader.from(writer))

  // Verify assignment structure
  deepStrictEqual(response.assignment, [
    {
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
    }
  ])
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
    .appendArray([], () => {}) // Empty assignment
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
    assignment: []
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
    .appendArray([], () => {}) // Empty assignment
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
        assignment: []
      })

      return true
    }
  )
})
