import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { leaveGroupV5, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = leaveGroupV5

test('createRequest serializes basic parameters correctly', () => {
  const groupId = 'test-group'
  const members = [
    {
      memberId: 'test-member-1',
      groupInstanceId: null,
      reason: null
    }
  ]

  const writer = createRequest(groupId, members)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Verify group ID
  deepStrictEqual(reader.readString(), groupId)

  // Read and verify members array
  const membersArray = reader.readArray(() => {
    const memberId = reader.readString()
    const groupInstanceId = reader.readNullableString()
    const reason = reader.readNullableString()

    return { memberId, groupInstanceId, reason }
  })

  // Verify the members details and tagged fields
  deepStrictEqual(
    {
      members: membersArray
    },
    {
      members: [
        {
          memberId: 'test-member-1',
          groupInstanceId: null,
          reason: null
        }
      ]
    }
  )
})

test('createRequest with group instance ID', () => {
  const groupId = 'test-group'
  const members = [
    {
      memberId: 'test-member-1',
      groupInstanceId: 'test-instance-id',
      reason: null
    }
  ]

  const writer = createRequest(groupId, members)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Skip to members
  reader.readString() // Group ID

  // Read members array
  const membersArray = reader.readArray(() => {
    const memberId = reader.readString()
    const groupInstanceId = reader.readString()
    const reason = reader.readString()

    return { memberId, groupInstanceId, reason }
  })

  // Verify the group instance ID
  deepStrictEqual(membersArray[0].groupInstanceId, 'test-instance-id')
})

test('createRequest with reason', () => {
  const groupId = 'test-group'
  const members = [
    {
      memberId: 'test-member-1',
      groupInstanceId: null,
      reason: 'Rebalancing'
    }
  ]

  const writer = createRequest(groupId, members)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Skip to members
  reader.readString() // Group ID

  // Read members array
  const membersArray = reader.readArray(() => {
    const memberId = reader.readString()
    const groupInstanceId = reader.readString()
    const reason = reader.readString()

    return { memberId, groupInstanceId, reason }
  })

  // Verify the reason
  deepStrictEqual(membersArray[0].reason, 'Rebalancing')
})

test('createRequest with multiple members', () => {
  const groupId = 'test-group'
  const members = [
    {
      memberId: 'test-member-1',
      groupInstanceId: null,
      reason: null
    },
    {
      memberId: 'test-member-2',
      groupInstanceId: 'test-instance-id',
      reason: 'Shutting down'
    }
  ]

  const writer = createRequest(groupId, members)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Skip to members
  reader.readString() // Group ID

  // Read members array
  const membersArray = reader.readArray(() => {
    const memberId = reader.readString()
    const groupInstanceId = reader.readString()
    const reason = reader.readString()

    return { memberId, groupInstanceId, reason }
  })

  // Verify multiple members
  deepStrictEqual(membersArray, [
    {
      memberId: 'test-member-1',
      groupInstanceId: '', // Note: readString() without nullable would return '' not null
      reason: ''
    },
    {
      memberId: 'test-member-2',
      groupInstanceId: 'test-instance-id',
      reason: 'Shutting down'
    }
  ])
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    // Members array - using tagged fields format
    .appendArray(
      [
        {
          memberId: 'test-member-1',
          groupInstanceId: null,
          errorCode: 0
        }
      ],
      (w, member) => {
        w.appendString(member.memberId).appendString(member.groupInstanceId).appendInt16(member.errorCode)
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 13, 5, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    members: [
      {
        memberId: 'test-member-1',
        groupInstanceId: null,
        errorCode: 0
      }
    ]
  })
})

test('parseResponse handles top-level error code', () => {
  // Create a response with a top-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(15) // errorCode (e.g., COORDINATOR_NOT_AVAILABLE)
    // Empty members array
    .appendArray(
      [], // Empty array
      () => {}, // No members to process
      true // Append tagged fields
    )
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 13, 5, Reader.from(writer))
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
        members: []
      })

      return true
    }
  )
})

test('parseResponse handles member-level error code', () => {
  // Create a response with a member-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success at top level)
    // Members array with member-level error
    .appendArray(
      [
        {
          memberId: 'test-member-1',
          groupInstanceId: null,
          errorCode: 16 // error code (e.g., UNKNOWN_MEMBER_ID)
        }
      ],
      (w, member) => {
        w.appendString(member.memberId).appendString(member.groupInstanceId).appendInt16(member.errorCode)
      }
    )
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 13, 5, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        errorCode: 0,
        members: [
          {
            memberId: 'test-member-1',
            groupInstanceId: null,
            errorCode: 16
          }
        ]
      })

      return true
    }
  )
})

test('parseResponse handles multiple members', () => {
  // Create a response with multiple members
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    // Members array - using tagged fields format
    .appendArray(
      [
        {
          memberId: 'test-member-1',
          groupInstanceId: null,
          errorCode: 0
        },
        {
          memberId: 'test-member-2',
          groupInstanceId: 'test-instance-id',
          errorCode: 0
        }
      ],
      (w, member) => {
        w.appendString(member.memberId).appendString(member.groupInstanceId).appendInt16(member.errorCode)
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 13, 5, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    members: [
      {
        memberId: 'test-member-1',
        groupInstanceId: null,
        errorCode: 0
      },
      {
        memberId: 'test-member-2',
        groupInstanceId: 'test-instance-id',
        errorCode: 0
      }
    ]
  })
})
