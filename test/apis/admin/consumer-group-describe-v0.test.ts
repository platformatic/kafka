import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { consumerGroupDescribeV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = consumerGroupDescribeV0

test('createRequest serializes basic parameters correctly', () => {
  const groupIds = ['test-group']
  const includeAuthorizedOperations = false

  const writer = createRequest(groupIds, includeAuthorizedOperations)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read group IDs array
  const groupIdsArray = reader.readArray(() => reader.readString(), true, false)

  // Read includeAuthorizedOperations boolean
  const includeAuthorizedOpsValue = reader.readBoolean()

  // Verify serialized data
  deepStrictEqual(
    {
      groupIds: groupIdsArray,
      includeAuthorizedOperations: includeAuthorizedOpsValue
    },
    {
      groupIds: ['test-group'],
      includeAuthorizedOperations: false
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple group IDs correctly', () => {
  const groupIds = ['group-1', 'group-2', 'group-3']
  const includeAuthorizedOperations = false

  const writer = createRequest(groupIds, includeAuthorizedOperations)
  const reader = Reader.from(writer)

  // Read group IDs array
  const groupIdsArray = reader.readArray(() => reader.readString(), true, false)

  // Skip includeAuthorizedOperations boolean
  reader.readBoolean()

  // Verify multiple group IDs
  deepStrictEqual(groupIdsArray, ['group-1', 'group-2', 'group-3'], 'Should correctly serialize multiple group IDs')
})

test('createRequest serializes includeAuthorizedOperations flag correctly', () => {
  const groupIds = ['test-group']
  const includeAuthorizedOperations = true

  const writer = createRequest(groupIds, includeAuthorizedOperations)
  const reader = Reader.from(writer)

  // Skip group IDs array
  reader.readArray(() => reader.readString(), true, false)

  // Read includeAuthorizedOperations boolean
  const includeAuthorizedOpsValue = reader.readBoolean()

  // Verify includeAuthorizedOperations flag
  ok(includeAuthorizedOpsValue === true, 'includeAuthorizedOperations flag should be set to true')
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no groups
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}) // Empty groups array
    .appendTaggedFields()

  const response = parseResponse(1, 69, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      groups: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a successful response with a group', () => {
  // Create a successful response with a basic group
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          groupId: 'test-group',
          groupState: 'Stable',
          groupEpoch: 1,
          assignmentEpoch: 1,
          assignorName: 'range',
          members: [],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.errorMessage)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendInt32(group.groupEpoch)
          .appendInt32(group.assignmentEpoch)
          .appendString(group.assignorName)
          .appendArray(group.members, () => {}) // Empty members array
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 69, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      groups: [
        {
          errorCode: 0,
          errorMessage: null,
          groupId: 'test-group',
          groupState: 'Stable',
          groupEpoch: 1,
          assignmentEpoch: 1,
          assignorName: 'range',
          members: [],
          authorizedOperations: 0
        }
      ]
    },
    'Response with a group should match expected structure'
  )
})

test('parseResponse correctly processes a response with group members', () => {
  // Create a response with group members
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          groupId: 'test-group',
          groupState: 'Stable',
          groupEpoch: 1,
          assignmentEpoch: 1,
          assignorName: 'range',
          members: [
            {
              memberId: 'consumer-1-uuid',
              instanceId: 'instance-1',
              rackId: null,
              memberEpoch: 1,
              clientId: 'consumer-1',
              clientHost: '172.17.0.1',
              subscribedTopicNames: 'test-topic',
              subscribedTopicRegex: null,
              assignment: {
                topicPartitions: [
                  {
                    topicId: '12345678-1234-1234-1234-123456789abc',
                    topicName: 'test-topic',
                    partitions: [0, 1]
                  }
                ]
              },
              targetAssignment: {
                topicPartitions: [
                  {
                    topicId: '12345678-1234-1234-1234-123456789abc',
                    topicName: 'test-topic',
                    partitions: [0, 1]
                  }
                ]
              }
            }
          ],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.errorMessage)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendInt32(group.groupEpoch)
          .appendInt32(group.assignmentEpoch)
          .appendString(group.assignorName)
          .appendArray(group.members, (w, member) => {
            w.appendString(member.memberId)
              .appendString(member.instanceId)
              .appendString(member.rackId)
              .appendInt32(member.memberEpoch)
              .appendString(member.clientId)
              .appendString(member.clientHost)
              .appendString(member.subscribedTopicNames)
              .appendString(member.subscribedTopicRegex)
              // Assignment
              .appendArray(member.assignment.topicPartitions, (w, tp) => {
                w.appendUUID(tp.topicId)
                  .appendString(tp.topicName)
                  .appendArray(tp.partitions, (w, p) => w.appendInt32(p), true, false)
              })
              // Target assignment
              .appendArray(member.targetAssignment.topicPartitions, (w, tp) => {
                w.appendUUID(tp.topicId)
                  .appendString(tp.topicName)
                  .appendArray(tp.partitions, (w, p) => w.appendInt32(p), true, false)
              })
          })
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 69, 0, Reader.from(writer))

  // Verify member structure
  deepStrictEqual(response.groups[0].members[0].memberId, 'consumer-1-uuid', 'Member ID should be parsed correctly')

  deepStrictEqual(response.groups[0].members[0].clientId, 'consumer-1', 'Client ID should be parsed correctly')

  deepStrictEqual(
    response.groups[0].members[0].assignment.topicPartitions[0].topicName,
    'test-topic',
    'Topic name in assignment should be parsed correctly'
  )

  deepStrictEqual(
    response.groups[0].members[0].assignment.topicPartitions[0].partitions,
    [0, 1],
    'Partitions in assignment should be parsed correctly'
  )
})

test('parseResponse handles group level errors correctly', () => {
  // Create a response with a group-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 16, // GROUP_ID_NOT_FOUND
          errorMessage: 'Group not found',
          groupId: 'nonexistent-group',
          groupState: '',
          groupEpoch: -1,
          assignmentEpoch: -1,
          assignorName: '',
          members: [],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.errorMessage)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendInt32(group.groupEpoch)
          .appendInt32(group.assignmentEpoch)
          .appendString(group.assignorName)
          .appendArray(group.members, () => {}) // Empty members array
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 69, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(err.response.groups[0].errorCode, 16, 'Error code should be preserved in the response')

      deepStrictEqual(
        err.response.groups[0].errorMessage,
        'Group not found',
        'Error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse handles multiple groups with mixed errors correctly', () => {
  // Create a response with multiple groups and mixed errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0, // Success
          errorMessage: null,
          groupId: 'good-group',
          groupState: 'Stable',
          groupEpoch: 1,
          assignmentEpoch: 1,
          assignorName: 'range',
          members: [],
          authorizedOperations: 0
        },
        {
          errorCode: 16, // GROUP_ID_NOT_FOUND
          errorMessage: 'Group not found',
          groupId: 'bad-group',
          groupState: '',
          groupEpoch: -1,
          assignmentEpoch: -1,
          assignorName: '',
          members: [],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.errorMessage)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendInt32(group.groupEpoch)
          .appendInt32(group.assignmentEpoch)
          .appendString(group.assignorName)
          .appendArray(group.members, () => {}) // Empty members array
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 69, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify response contains both successful and error groups
      deepStrictEqual(
        err.response.groups.map((g: Record<string, number>) => ({ groupId: g.groupId, errorCode: g.errorCode })),
        [
          { groupId: 'good-group', errorCode: 0 },
          { groupId: 'bad-group', errorCode: 16 }
        ],
        'Response should contain both successful and error groups'
      )

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs)
    .appendArray(
      [
        {
          errorCode: 0,
          errorMessage: null,
          groupId: 'test-group',
          groupState: 'Stable',
          groupEpoch: 1,
          assignmentEpoch: 1,
          assignorName: 'range',
          members: [],
          authorizedOperations: 0
        }
      ],
      (w, group) => {
        w.appendInt16(group.errorCode)
          .appendString(group.errorMessage)
          .appendString(group.groupId)
          .appendString(group.groupState)
          .appendInt32(group.groupEpoch)
          .appendInt32(group.assignmentEpoch)
          .appendString(group.assignorName)
          .appendArray(group.members, () => {}) // Empty members array
          .appendInt32(group.authorizedOperations)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 69, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
