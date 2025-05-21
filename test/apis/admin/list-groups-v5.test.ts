import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { ConsumerGroupStates } from '../../../src/apis/enumerations.ts'
import { listGroupsV5, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = listGroupsV5

test('createRequest serializes states filter and types filter correctly', () => {
  const typesFilter = ['consumer', 'static-consumer']

  const writer = createRequest(['STABLE', 'EMPTY'], typesFilter)

  // Verify it returns a Writer instance
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read states filter array
  const serializedStates = reader.readArray(() => reader.readString(), true, false)

  // Read types filter array
  const serializedTypes = reader.readArray(() => reader.readString(), true, false)

  // Read tagged fields count

  // Verify the complete structure
  deepStrictEqual(
    {
      statesFilter: serializedStates,
      typesFilter: serializedTypes
    },
    {
      statesFilter: ['STABLE', 'EMPTY'],
      typesFilter: ['consumer', 'static-consumer']
    },
    'Serialized data should match expected structure'
  )
})

test('createRequest with empty states and types filters', () => {
  const writer = createRequest([], [])

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read states filter array
  const serializedStates = reader.readArray(() => reader.readString(), true, false)

  // Read types filter array
  const serializedTypes = reader.readArray(() => reader.readString(), true, false)

  // Read tagged fields count

  // Verify the complete structure
  deepStrictEqual(
    {
      statesFilter: serializedStates,
      typesFilter: serializedTypes
    },
    {
      statesFilter: [],
      typesFilter: []
    },
    'Serialized data with empty filter arrays should match expected structure'
  )
})

test('createRequest with all possible consumer group states', () => {
  const statesFilter = [...ConsumerGroupStates]
  const typesFilter: string[] = []

  const writer = createRequest(statesFilter, typesFilter)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read states filter array
  const serializedStates = reader.readArray(() => reader.readString(), true, false)

  // Read types filter array
  const serializedTypes = reader.readArray(() => reader.readString(), true, false)

  // Read tagged fields count

  // Verify the complete structure
  deepStrictEqual(
    {
      statesFilter: serializedStates,
      typesFilter: serializedTypes
    },
    {
      statesFilter: ['PREPARING_REBALANCE', 'COMPLETING_REBALANCE', 'STABLE', 'DEAD', 'EMPTY'],
      typesFilter: []
    },
    'All consumer group states should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with groups data
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    // Groups array
    .appendArray(
      [
        {
          groupId: 'test-group-1',
          protocolType: 'consumer',
          groupState: 'STABLE',
          groupType: 'CLASSIC'
        },
        {
          groupId: 'test-group-2',
          protocolType: 'consumer',
          groupState: 'EMPTY',
          groupType: 'CLASSIC'
        }
      ],
      (w, group) => {
        w.appendString(group.groupId)
          .appendString(group.protocolType)
          .appendString(group.groupState)
          .appendString(group.groupType)
      }
    )

  const response = parseResponse(1, 16, 5, Reader.from(writer))

  // Verify the main response structure
  deepStrictEqual(
    {
      throttleTimeMs: response.throttleTimeMs,
      errorCode: response.errorCode,
      groupsLength: response.groups.length
    },
    {
      throttleTimeMs: 0,
      errorCode: 0,
      groupsLength: 2
    },
    'Response structure should match expected values'
  )

  // Verify the first group data
  deepStrictEqual(
    {
      groupId: response.groups[0].groupId,
      protocolType: response.groups[0].protocolType,
      groupState: response.groups[0].groupState,
      groupType: response.groups[0].groupType
    },
    {
      groupId: 'test-group-1',
      protocolType: 'consumer',
      groupState: 'STABLE',
      groupType: 'CLASSIC'
    },
    'First group data should match expected values'
  )

  // Verify the second group data
  deepStrictEqual(
    {
      groupId: response.groups[1].groupId,
      protocolType: response.groups[1].protocolType,
      groupState: response.groups[1].groupState,
      groupType: response.groups[1].groupType
    },
    {
      groupId: 'test-group-2',
      protocolType: 'consumer',
      groupState: 'EMPTY',
      groupType: 'CLASSIC'
    },
    'Second group data should match expected values'
  )
})

test('parseResponse with empty groups array', () => {
  // Create a response with an empty groups array
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    // Empty groups array
    .appendArray([], () => {})

  const response = parseResponse(1, 16, 5, Reader.from(writer))

  // Verify response with empty groups
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      groups: []
    },
    'Response with empty groups should be parsed correctly'
  )
})

test('parseResponse handles throttling correctly', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero for throttling)
    .appendInt16(0) // errorCode
    // Empty groups array for simplicity
    .appendArray([], () => {})

  const response = parseResponse(1, 16, 5, Reader.from(writer))

  // Verify throttling is processed correctly
  deepStrictEqual(response.throttleTimeMs, 100, 'Throttle time should be correctly parsed')
})

test('parseResponse throws on error response', () => {
  // Create an error response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(41) // NOT_CONTROLLER (example error)
    // Empty groups array
    .appendArray([], () => {})

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 16, 5, Reader.from(writer))
    },
    (err: any) => {
      // Verify error is a ResponseError
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify it contains a ProtocolError
      const protocolError = err.errors[0]
      ok(protocolError, 'Should have at least one error')
      deepStrictEqual(protocolError.apiCode, 41, 'Error code should be correctly captured')
      deepStrictEqual(protocolError.apiId, 'NOT_CONTROLLER', 'Error ID should be correctly captured')

      // Verify the response structure is preserved
      deepStrictEqual(
        err.response,
        {
          throttleTimeMs: 0,
          errorCode: 41,
          groups: []
        },
        'Error response should preserve the original response structure'
      )

      return true
    }
  )
})

test('parseResponse with different group types and states', () => {
  // Create a response with different group types and states
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    // Groups array with different types and states
    .appendArray(
      [
        {
          groupId: 'classic-group',
          protocolType: 'consumer',
          groupState: 'STABLE',
          groupType: 'CLASSIC'
        },
        {
          groupId: 'high-level-group',
          protocolType: 'consumer',
          groupState: 'PREPARING_REBALANCE',
          groupType: 'HIGH_LEVEL'
        },
        {
          groupId: 'consumer-group',
          protocolType: 'consumer',
          groupState: 'DEAD',
          groupType: 'CONSUMER'
        }
      ],
      (w, group) => {
        w.appendString(group.groupId)
          .appendString(group.protocolType)
          .appendString(group.groupState)
          .appendString(group.groupType)
      }
    )

  const response = parseResponse(1, 16, 5, Reader.from(writer))

  // Verify number of groups
  deepStrictEqual(response.groups.length, 3, 'Response should have 3 groups')

  // Verify each group has distinct type and state
  const groupsData = response.groups.map(g => ({
    groupId: g.groupId,
    protocolType: g.protocolType,
    groupState: g.groupState,
    groupType: g.groupType
  }))

  deepStrictEqual(
    groupsData,
    [
      {
        groupId: 'classic-group',
        protocolType: 'consumer',
        groupState: 'STABLE',
        groupType: 'CLASSIC'
      },
      {
        groupId: 'high-level-group',
        protocolType: 'consumer',
        groupState: 'PREPARING_REBALANCE',
        groupType: 'HIGH_LEVEL'
      },
      {
        groupId: 'consumer-group',
        protocolType: 'consumer',
        groupState: 'DEAD',
        groupType: 'CONSUMER'
      }
    ],
    'Group data with different types and states should be parsed correctly'
  )
})
