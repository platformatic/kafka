import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { joinGroupV9, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = joinGroupV9

test('createRequest serializes basic parameters correctly', () => {
  const groupId = 'test-group'
  const sessionTimeoutMs = 30000
  const rebalanceTimeoutMs = 60000
  const memberId = '' // Empty for new members
  const groupInstanceId = null
  const protocolType = 'consumer'
  const protocols = [
    {
      name: 'range',
      metadata: Buffer.from('metadata-1')
    },
    {
      name: 'roundrobin',
      metadata: null
    }
  ]
  const reason = null

  const writer = createRequest(
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType,
    protocols,
    reason
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read and store basic parameters
  const serializedData = {
    groupId: reader.readString(),
    sessionTimeoutMs: reader.readInt32(),
    rebalanceTimeoutMs: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readNullableString(),
    protocolType: reader.readString()
  }

  // Verify basic parameters match expected values
  deepStrictEqual(serializedData, {
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType
  })

  // Read protocols array
  const protocolsArray = reader.readArray(() => {
    const name = reader.readString()
    const metadata = reader.readBytes()
    return { name, metadata }
  })

  // Verify protocols, reason, and tagged fields
  deepStrictEqual(
    {
      protocols: [
        {
          name: protocolsArray[0].name,
          metadata: protocolsArray[0].metadata?.toString()
        },
        {
          name: protocolsArray[1].name,
          metadataLength: protocolsArray[1].metadata?.length
        }
      ],
      reason: reader.readString()
    },
    {
      protocols: [
        {
          name: 'range',
          metadata: 'metadata-1'
        },
        {
          name: 'roundrobin',
          metadataLength: 0
        }
      ],
      reason: '' // Reason (null)
    }
  )

  // Verify second protocol metadata is a Buffer
  ok(Buffer.isBuffer(protocolsArray[1].metadata))
})

test('createRequest with existing member ID', () => {
  const groupId = 'test-group'
  const sessionTimeoutMs = 30000
  const rebalanceTimeoutMs = 60000
  const memberId = 'existing-member-id' // Non-empty for existing members
  const groupInstanceId = null
  const protocolType = 'consumer'
  const protocols = [
    {
      name: 'range',
      metadata: Buffer.from('metadata-1')
    }
  ]
  const reason = null

  const writer = createRequest(
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType,
    protocols,
    reason
  )

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all basic parameters
  const serializedData = {
    groupId: reader.readString(),
    sessionTimeoutMs: reader.readInt32(),
    rebalanceTimeoutMs: reader.readInt32(),
    memberId: reader.readString()
  }

  // Verify all parameters including existing member ID
  deepStrictEqual(serializedData, {
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId: 'existing-member-id'
  })
})

test('createRequest with group instance ID', () => {
  const groupId = 'test-group'
  const sessionTimeoutMs = 30000
  const rebalanceTimeoutMs = 60000
  const memberId = ''
  const groupInstanceId = 'test-instance-id' // Static group membership
  const protocolType = 'consumer'
  const protocols = [
    {
      name: 'range',
      metadata: Buffer.from('metadata-1')
    }
  ]
  const reason = null

  const writer = createRequest(
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType,
    protocols,
    reason
  )

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all basic parameters
  const serializedData = {
    groupId: reader.readString(),
    sessionTimeoutMs: reader.readInt32(),
    rebalanceTimeoutMs: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readString()
  }

  // Verify all parameters including group instance ID
  deepStrictEqual(serializedData, {
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId: 'test-instance-id'
  })
})

test('createRequest with multiple protocols', () => {
  const groupId = 'test-group'
  const sessionTimeoutMs = 30000
  const rebalanceTimeoutMs = 60000
  const memberId = ''
  const groupInstanceId = null
  const protocolType = 'consumer'
  const protocols = [
    {
      name: 'range',
      metadata: Buffer.from('metadata-1')
    },
    {
      name: 'roundrobin',
      metadata: Buffer.from('metadata-2')
    }
  ]
  const reason = null

  const writer = createRequest(
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType,
    protocols,
    reason
  )

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all basic parameters
  const serializedData = {
    groupId: reader.readString(),
    sessionTimeoutMs: reader.readInt32(),
    rebalanceTimeoutMs: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readNullableString(),
    protocolType: reader.readString()
  }

  // Verify all basic parameters
  deepStrictEqual(serializedData, {
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType
  })

  // Read protocols array
  const protocolsArray = reader.readArray(() => {
    const name = reader.readString()
    const metadata = reader.readBytes()
    return { name, metadata }
  })

  // Verify protocols
  deepStrictEqual(protocolsArray, [
    {
      name: 'range',
      metadata: Buffer.from('metadata-1')
    },
    {
      name: 'roundrobin',
      metadata: Buffer.from('metadata-2')
    }
  ])
})

test('createRequest with reason', () => {
  const groupId = 'test-group'
  const sessionTimeoutMs = 30000
  const rebalanceTimeoutMs = 60000
  const memberId = ''
  const groupInstanceId = null
  const protocolType = 'consumer'
  const protocols = [
    {
      name: 'range',
      metadata: Buffer.from('metadata-1')
    }
  ]
  const reason = 'Joining after rebalance'

  const writer = createRequest(
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType,
    protocols,
    reason
  )

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all basic parameters
  const serializedData = {
    groupId: reader.readString(),
    sessionTimeoutMs: reader.readInt32(),
    rebalanceTimeoutMs: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readNullableString(),
    protocolType: reader.readString()
  }

  // Verify all basic parameters
  deepStrictEqual(serializedData, {
    groupId,
    sessionTimeoutMs,
    rebalanceTimeoutMs,
    memberId,
    groupInstanceId,
    protocolType
  })

  // Skip protocols
  reader.readArray(() => {
    reader.readString() // name
    reader.readBytes() // metadata
  })

  // Read and verify reason
  const serializedReason = reader.readString()
  deepStrictEqual(serializedReason, 'Joining after rebalance')

  // Verify tags count
  deepStrictEqual(reader.readUnsignedVarInt(), 0)
})

test('parseResponse correctly processes a successful response for a follower', () => {
  // Create a successful response for a follower (not the leader)
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt32(5) // generationId
    .appendString('consumer') // protocolType
    .appendString('range') // protocolName
    .appendString('leader-member-id') // leader
    .appendBoolean(false) // skipAssignment
    .appendString('follower-member-id') // memberId (this member)
    // Empty members array (followers don't get member info)
    .appendArray([], () => {})
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 11, 9, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    generationId: 5,
    protocolType: 'consumer',
    protocolName: 'range',
    leader: 'leader-member-id',
    skipAssignment: false,
    memberId: 'follower-member-id',
    members: []
  })
})

test('parseResponse correctly processes a successful response for a leader', () => {
  // Create a successful response for the leader (includes member metadata)
  const leaderMetadata = Buffer.from('leader-metadata')
  const followerMetadata = Buffer.from('follower-metadata')

  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt32(5) // generationId
    .appendString('consumer') // protocolType
    .appendString('range') // protocolName
    .appendString('leader-member-id') // leader
    .appendBoolean(false) // skipAssignment
    .appendString('leader-member-id') // memberId (same as leader)
    // Members array with multiple members
    .appendArray(
      [
        {
          memberId: 'leader-member-id',
          groupInstanceId: 'leader-instance-id',
          metadata: leaderMetadata
        },
        {
          memberId: 'follower-member-id',
          groupInstanceId: null,
          metadata: followerMetadata
        }
      ],
      (w, member) => {
        // For each member in the array, serialize its fields
        w.appendString(member.memberId)
          .appendString(member.groupInstanceId)
          .appendBytes(member.metadata || Buffer.alloc(0))
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 11, 9, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    generationId: 5,
    protocolType: 'consumer',
    protocolName: 'range',
    leader: 'leader-member-id',
    skipAssignment: false,
    memberId: 'leader-member-id',
    members: [
      {
        memberId: 'leader-member-id',
        groupInstanceId: 'leader-instance-id',
        metadata: Buffer.from('leader-metadata')
      },
      {
        memberId: 'follower-member-id',
        groupInstanceId: null,
        metadata: Buffer.from('follower-metadata')
      }
    ]
  })

  // Verify metadata are Buffers with correct content
  ok(Buffer.isBuffer(response.members[0].metadata))
  ok(Buffer.isBuffer(response.members[1].metadata))
  deepStrictEqual(Buffer.compare(response.members[0].metadata, Buffer.from('leader-metadata')), 0)
  deepStrictEqual(Buffer.compare(response.members[1].metadata, Buffer.from('follower-metadata')), 0)
})

test('parseResponse with skip assignment flag', () => {
  // Create a response with skipAssignment = true
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt32(5) // generationId
    .appendString('consumer') // protocolType
    .appendString('range') // protocolName
    .appendString('leader-member-id') // leader
    .appendBoolean(true) // skipAssignment = true
    .appendString('follower-member-id') // memberId
    // Empty members array
    .appendArray([], () => {})
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 11, 9, Reader.from(writer))

  // Verify skipAssignment flag
  ok(response.skipAssignment)
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(15) // errorCode (e.g., COORDINATOR_NOT_AVAILABLE)
    .appendInt32(-1) // generationId (invalid for error)
    .appendString(null) // protocolType (null for error)
    .appendString(null) // protocolName (null for error)
    .appendString('') // leader (empty for error)
    .appendBoolean(false) // skipAssignment
    .appendString('') // memberId (empty for error)
    // Empty members array
    .appendArray([], () => {})
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 11, 9, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        errorCode: 15,
        generationId: -1,
        protocolType: null,
        protocolName: null,
        leader: '',
        skipAssignment: false,
        memberId: '',
        members: []
      })

      return true
    }
  )
})
