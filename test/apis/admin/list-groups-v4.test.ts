import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { ConsumerGroupStates } from '../../../src/apis/enumerations.ts'
import { listGroupsV4, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = listGroupsV4

test('createRequest serializes states filter and accepts the compatibility types filter argument', () => {
  const writer = createRequest(['Stable', 'Empty'], ['consumer'])

  // Verify it returns a Writer instance
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read states filter array
  const serializedStates = reader.readArray(() => reader.readString(), true, false)
  reader.readTaggedFields()

  // Verify the complete structure
  deepStrictEqual(
    {
      statesFilter: serializedStates
    },
    {
      statesFilter: ['Stable', 'Empty']
    },
    'Serialized data should match expected structure'
  )
  strictEqual(reader.remaining, 0)
})

test('createRequest with empty states and types filters', () => {
  const writer = createRequest([], [])

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read states filter array
  const serializedStates = reader.readArray(() => reader.readString(), true, false)
  reader.readTaggedFields()

  // Verify the complete structure
  deepStrictEqual(
    {
      statesFilter: serializedStates
    },
    {
      statesFilter: []
    },
    'Serialized data with empty filter arrays should match expected structure'
  )
  strictEqual(reader.remaining, 0)
})

test('createRequest with all possible consumer group states', () => {
  const statesFilter = [...ConsumerGroupStates]

  const writer = createRequest(statesFilter, [])

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read states filter array
  const serializedStates = reader.readArray(() => reader.readString(), true, false)
  reader.readTaggedFields()

  // Verify the complete structure
  deepStrictEqual(
    {
      statesFilter: serializedStates
    },
    {
      statesFilter: ['Unknown', 'PreparingRebalance', 'CompletingRebalance', 'Stable', 'Dead', 'Empty', 'Assigning', 'Reconciling', 'NotReady']
    },
    'All consumer group states should be serialized correctly'
  )
  strictEqual(reader.remaining, 0)
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
          groupState: 'Stable'
        },
        {
          groupId: 'test-group-2',
          protocolType: 'consumer',
          groupState: 'Empty'
        }
      ],
      (w, group) => {
        w.appendString(group.groupId).appendString(group.protocolType).appendString(group.groupState)
      }
    )

  const reader = Reader.from(writer.appendTaggedFields())
  const response = parseResponse(1, 16, 4, reader)

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
  strictEqual(reader.remaining, 0)

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
      groupState: 'Stable',
      groupType: ''
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
      groupState: 'Empty',
      groupType: ''
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

  const reader = Reader.from(writer.appendTaggedFields())
  const response = parseResponse(1, 16, 4, reader)

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
  strictEqual(reader.remaining, 0)
})

test('parseResponse handles throttling correctly', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero for throttling)
    .appendInt16(0) // errorCode
    // Empty groups array for simplicity
    .appendArray([], () => {})

  const reader = Reader.from(writer.appendTaggedFields())
  const response = parseResponse(1, 16, 4, reader)

  // Verify throttling is processed correctly
  deepStrictEqual(response.throttleTimeMs, 100, 'Throttle time should be correctly parsed')
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws on error response', () => {
  // Create an error response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(41) // NOT_CONTROLLER (example error)
    // Empty groups array
    .appendArray([], () => {})

  const reader = Reader.from(writer.appendTaggedFields())

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 16, 4, reader)
    },
    (err: any) => {
      // Verify error is a ResponseError
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify it contains a ProtocolError
      const protocolError = err.errors[0]
      ok(protocolError, 'Should have at least one error')
      deepStrictEqual(protocolError.apiCode, 41, 'Error code should be correctly captured')
      deepStrictEqual(protocolError.apiId, 'NOT_CONTROLLER', 'Error ID should be correctly captured')
      ok(err.message.includes('ListGroups(v4)'), 'API version should be correctly captured')
      strictEqual(reader.remaining, 0)

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
          groupState: 'Stable'
        },
        {
          groupId: 'high-level-group',
          protocolType: 'consumer',
          groupState: 'PreparingRebalance'
        },
        {
          groupId: 'consumer-group',
          protocolType: 'consumer',
          groupState: 'Dead'
        }
      ],
      (w, group) => {
        w.appendString(group.groupId).appendString(group.protocolType).appendString(group.groupState)
      }
    )

  const reader = Reader.from(writer.appendTaggedFields())
  const response = parseResponse(1, 16, 4, reader)

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
        groupState: 'Stable',
        groupType: ''
      },
      {
        groupId: 'high-level-group',
        protocolType: 'consumer',
        groupState: 'PreparingRebalance',
        groupType: ''
      },
      {
        groupId: 'consumer-group',
        protocolType: 'consumer',
        groupState: 'Dead',
        groupType: ''
      }
    ],
    'Group data with different types and states should be parsed correctly'
  )
  strictEqual(reader.remaining, 0)
})
