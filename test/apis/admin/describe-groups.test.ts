import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { describeGroupsV5, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeGroupsV5

test('createRequest serializes group names correctly', () => {
  const groups = ['group-1', 'group-2', 'group-3']
  const includeAuthorizedOperations = true

  const writer = createRequest(groups, includeAuthorizedOperations)

  // Verify it returns a Writer instance
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read group names array
  const serializedGroups = reader.readArray(() => reader.readString(), true, false)

  // Read includeAuthorizedOperations flag and tagged fields count
  const includeAuthOps = reader.readBoolean()

  // Verify the complete structure
  deepStrictEqual(
    {
      groups: serializedGroups,
      includeAuthorizedOperations: includeAuthOps
    },
    {
      groups: ['group-1', 'group-2', 'group-3'],
      includeAuthorizedOperations: true
    },
    'Serialized data should match expected structure'
  )
})

test('createRequest with includeAuthorizedOperations false', () => {
  const groups = ['group-1']
  const includeAuthorizedOperations = false

  const writer = createRequest(groups, includeAuthorizedOperations)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read group names array
  const serializedGroups = reader.readArray(() => reader.readString(), true, false)

  // Read includeAuthorizedOperations flag and tagged fields count
  const includeAuthOps = reader.readBoolean()

  // Verify the complete structure
  deepStrictEqual(
    {
      groups: serializedGroups,
      includeAuthorizedOperations: includeAuthOps
    },
    {
      groups: ['group-1'],
      includeAuthorizedOperations: false
    },
    'Serialized data with includeAuthorizedOperations=false should match expected structure'
  )
})

test('createRequest serializes empty groups array correctly', () => {
  const groups: string[] = []
  const includeAuthorizedOperations = true

  const writer = createRequest(groups, includeAuthorizedOperations)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read group names array
  const serializedGroups = reader.readArray(() => reader.readString(), true, false)

  // Read includeAuthorizedOperations flag and tagged fields count
  const includeAuthOps = reader.readBoolean()

  // Verify the complete structure
  deepStrictEqual(
    {
      groups: serializedGroups,
      includeAuthorizedOperations: includeAuthOps
    },
    {
      groups: [],
      includeAuthorizedOperations: true
    },
    'Serialized data with empty groups should match expected structure'
  )
})

test('createRequest serializes special characters in group names', () => {
  const groups = ['group/1', 'group-with-hyphen', 'group.with.dots']
  const includeAuthorizedOperations = true

  const writer = createRequest(groups, includeAuthorizedOperations)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read group names array
  const serializedGroups = reader.readArray(() => reader.readString(), true, false)

  // Read includeAuthorizedOperations flag and tagged fields count
  const includeAuthOps = reader.readBoolean()

  // Verify the complete structure
  deepStrictEqual(
    {
      groups: serializedGroups,
      includeAuthorizedOperations: includeAuthOps
    },
    {
      groups: ['group/1', 'group-with-hyphen', 'group.with.dots'],
      includeAuthorizedOperations: true
    },
    'Group names with special characters should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with minimal member data
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Groups array
    .appendArray(
      [
        {
          errorCode: 0, // Success
          groupId: 'test-group',
          groupState: 'Stable',
          protocolType: 'consumer',
          protocolData: 'range',
          members: [
            {
              memberId: 'consumer-1-123',
              groupInstanceId: null,
              clientId: 'client-1',
              clientHost: '127.0.0.1',
              memberMetadata: Buffer.from('test-metadata'),
              memberAssignment: Buffer.from('test-assignment')
            }
          ],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendString(group.protocolType)
          .appendString(group.protocolData)
          // Members array
          .appendArray(group.members, (w, member) => {
            w.appendString(member.memberId)
              .appendString(member.groupInstanceId)
              .appendString(member.clientId)
              .appendString(member.clientHost)
              .appendBytes(member.memberMetadata)
              .appendBytes(member.memberAssignment)
          })
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 15, 5, writer.bufferList)

  // Verify the main response structure
  deepStrictEqual(
    {
      throttleTimeMs: response.throttleTimeMs,
      groupsLength: response.groups.length,
      group: {
        errorCode: response.groups[0].errorCode,
        groupId: response.groups[0].groupId,
        groupState: response.groups[0].groupState,
        protocolType: response.groups[0].protocolType,
        protocolData: response.groups[0].protocolData,
        membersLength: response.groups[0].members.length,
        authorizedOperations: response.groups[0].authorizedOperations
      }
    },
    {
      throttleTimeMs: 0,
      groupsLength: 1,
      group: {
        errorCode: 0,
        groupId: 'test-group',
        groupState: 'Stable',
        protocolType: 'consumer',
        protocolData: 'range',
        membersLength: 1,
        authorizedOperations: 0
      }
    },
    'Response structure should match expected values'
  )

  // Verify the member data
  const member = response.groups[0].members[0]
  deepStrictEqual(
    {
      memberId: member.memberId,
      groupInstanceId: member.groupInstanceId,
      clientId: member.clientId,
      clientHost: member.clientHost,
      // Use buffer equality checks for binary data
      metadataString: member.memberMetadata.toString(),
      assignmentString: member.memberAssignment.toString()
    },
    {
      memberId: 'consumer-1-123',
      groupInstanceId: null,
      clientId: 'client-1',
      clientHost: '127.0.0.1',
      metadataString: 'test-metadata',
      assignmentString: 'test-assignment'
    },
    'Member data should match expected values'
  )
})

test('parseResponse with multiple members', () => {
  // Create a response with multiple members
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Groups array
    .appendArray(
      [
        {
          errorCode: 0, // Success
          groupId: 'test-group',
          groupState: 'Stable',
          protocolType: 'consumer',
          protocolData: 'range',
          members: [
            {
              memberId: 'consumer-1-123',
              groupInstanceId: null,
              clientId: 'client-1',
              clientHost: '127.0.0.1',
              memberMetadata: Buffer.from('metadata-1'),
              memberAssignment: Buffer.from('assignment-1')
            },
            {
              memberId: 'consumer-2-456',
              groupInstanceId: 'static-instance-1',
              clientId: 'client-2',
              clientHost: '127.0.0.2',
              memberMetadata: Buffer.from('metadata-2'),
              memberAssignment: Buffer.from('assignment-2')
            }
          ],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendString(group.protocolType)
          .appendString(group.protocolData)
          // Members array
          .appendArray(group.members, (w, member) => {
            w.appendString(member.memberId)
              .appendString(member.groupInstanceId)
              .appendString(member.clientId)
              .appendString(member.clientHost)
              .appendBytes(member.memberMetadata)
              .appendBytes(member.memberAssignment)
          })
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 15, 5, writer.bufferList)

  // Verify multiple members
  deepStrictEqual(response.groups[0].members.length, 2, 'Response should have two members')

  // Verify the second member with static group instance ID
  const member2 = response.groups[0].members[1]
  deepStrictEqual(
    {
      memberId: member2.memberId,
      groupInstanceId: member2.groupInstanceId,
      clientId: member2.clientId,
      clientHost: member2.clientHost,
      metadataString: member2.memberMetadata.toString(),
      assignmentString: member2.memberAssignment.toString()
    },
    {
      memberId: 'consumer-2-456',
      groupInstanceId: 'static-instance-1',
      clientId: 'client-2',
      clientHost: '127.0.0.2',
      metadataString: 'metadata-2',
      assignmentString: 'assignment-2'
    },
    'Second member data should match expected values'
  )
})

test('parseResponse with multiple groups', () => {
  // Create a response with multiple groups
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Groups array
    .appendArray(
      [
        {
          errorCode: 0,
          groupId: 'group-1',
          groupState: 'Stable',
          protocolType: 'consumer',
          protocolData: 'range',
          members: [],
          authorizedOperations: 0
        },
        {
          errorCode: 0,
          groupId: 'group-2',
          groupState: 'PreparingRebalance',
          protocolType: 'consumer',
          protocolData: 'round_robin',
          members: [],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendString(group.protocolType)
          .appendString(group.protocolData)
          // Empty members array
          .appendArray(group.members, () => {})
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 15, 5, writer.bufferList)

  // Verify multiple groups
  deepStrictEqual(response.groups.length, 2, 'Response should have two groups')

  // Verify each group's data
  deepStrictEqual(
    {
      group1: {
        groupId: response.groups[0].groupId,
        groupState: response.groups[0].groupState,
        protocolData: response.groups[0].protocolData
      },
      group2: {
        groupId: response.groups[1].groupId,
        groupState: response.groups[1].groupState,
        protocolData: response.groups[1].protocolData
      }
    },
    {
      group1: {
        groupId: 'group-1',
        groupState: 'Stable',
        protocolData: 'range'
      },
      group2: {
        groupId: 'group-2',
        groupState: 'PreparingRebalance',
        protocolData: 'round_robin'
      }
    },
    'Group data should match expected values'
  )
})

test('parseResponse with authorized operations', () => {
  // Create a response with authorized operations set
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Groups array
    .appendArray(
      [
        {
          errorCode: 0,
          groupId: 'test-group',
          groupState: 'Stable',
          protocolType: 'consumer',
          protocolData: 'range',
          members: [],
          authorizedOperations: 3 // Some bit flags for operations
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendString(group.protocolType)
          .appendString(group.protocolData)
          // Empty members array
          .appendArray(group.members, () => {})
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 15, 5, writer.bufferList)

  // Verify authorized operations are parsed correctly
  deepStrictEqual(response.groups[0].authorizedOperations, 3, 'Authorized operations should be correctly parsed')
})

test('parseResponse handles throttling correctly', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero for throttling)
    // Groups array - just one for simplicity
    .appendArray(
      [
        {
          errorCode: 0,
          groupId: 'test-group',
          groupState: 'Stable',
          protocolType: 'consumer',
          protocolData: 'range',
          members: [],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendString(group.protocolType)
          .appendString(group.protocolData)
          // Empty members array
          .appendArray(group.members, () => {})
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 15, 5, writer.bufferList)

  // Verify throttling is processed correctly
  deepStrictEqual(response.throttleTimeMs, 100, 'Throttle time should be correctly parsed')
})

test('parseResponse handles group error correctly', () => {
  // Create a response with group having an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Groups array with error
    .appendArray(
      [
        {
          errorCode: 15, // GROUP_ID_NOT_FOUND
          groupId: 'test-group',
          groupState: '', // Empty for error
          protocolType: '', // Empty for error
          protocolData: '', // Empty for error
          members: [],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendString(group.protocolType)
          .appendString(group.protocolData)
          // Empty members array
          .appendArray(group.members, () => {})
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 15, 5, writer.bufferList)
    },
    (err: any) => {
      // Verify error is a ResponseError
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify the error object has the expected properties
      ok(Array.isArray(err.errors) && err.errors.length === 1, 'Should have an array with 1 error for the failed group')

      // Verify the response structure is preserved
      deepStrictEqual(
        err.response,
        {
          throttleTimeMs: 0,
          groups: [
            {
              errorCode: 15,
              groupId: 'test-group',
              groupState: '',
              protocolType: '',
              protocolData: '',
              members: [],
              authorizedOperations: 0
            }
          ]
        },
        'Error response should preserve the original response structure'
      )

      return true
    }
  )
})

test('parseResponse handles multiple groups with mixed errors', () => {
  // Create a response with multiple groups and mixed results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Groups array with mixed results
    .appendArray(
      [
        {
          errorCode: 0, // Success
          groupId: 'group-1',
          groupState: 'Stable',
          protocolType: 'consumer',
          protocolData: 'range',
          members: [],
          authorizedOperations: 0
        },
        {
          errorCode: 15, // GROUP_ID_NOT_FOUND
          groupId: 'group-2',
          groupState: '',
          protocolType: '',
          protocolData: '',
          members: [],
          authorizedOperations: 0
        },
        {
          errorCode: 41, // GROUP_AUTHORIZATION_FAILED
          groupId: 'group-3',
          groupState: '',
          protocolType: '',
          protocolData: '',
          members: [],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendString(group.protocolType)
          .appendString(group.protocolData)
          .appendArray(group.members, () => {})
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 15, 5, writer.bufferList)
    },
    (err: any) => {
      // Verify error is a ResponseError
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify there are multiple errors
      ok(
        Array.isArray(err.errors) && err.errors.length === 2,
        'Should have an array with 2 errors for the failed groups'
      )

      // Get error codes from the response
      const errorCodes = err.response.groups
        .filter((g: Record<string, number>) => g.errorCode !== 0)
        .map((g: Record<string, number>) => g.errorCode)

      // Verify we have both expected error codes
      ok(
        errorCodes.includes(15) && errorCodes.includes(41),
        'Response should contain groups with expected error codes (15 and 41)'
      )

      // Verify all groups are preserved in the response
      deepStrictEqual(err.response.groups.length, 3, 'Response should contain all 3 groups')

      // Verify the successful group data is preserved
      deepStrictEqual(
        err.response.groups.find((g: Record<string, number>) => g.errorCode === 0)?.groupId,
        'group-1',
        'Successful group should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse with empty groups array', () => {
  // Create a response with an empty groups array
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Empty groups array
    .appendArray([], () => {})
    .appendTaggedFields()

  const response = parseResponse(1, 15, 5, writer.bufferList)

  // Verify response with empty groups
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      groups: []
    },
    'Response with empty groups should be parsed correctly'
  )
})
