import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, syncGroupV4, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = syncGroupV4

test('createRequest serializes basic parameters correctly', () => {
  const groupId = 'test-group'
  const generationId = 5
  const memberId = 'test-member-1'
  const groupInstanceId = null
  const assignments: any[] = []

  const writer = createRequest(groupId, generationId, memberId, groupInstanceId, assignments)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read and collect all basic parameters in a single object
  const serializedParams = {
    groupId: reader.readString(),
    generationId: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readNullableString()
  }

  // Verify all parameters in a single assertion with descriptive message
  deepStrictEqual(
    serializedParams,
    {
      groupId,
      generationId,
      memberId,
      groupInstanceId
    },
    'Serialized basic parameters should match input values'
  )

  // Get assignments array length
  const assignmentsArrayLength = reader.readUnsignedVarInt()
  deepStrictEqual(assignmentsArrayLength, 1, 'Empty assignments array should have length 1 for compact arrays')
})

test('createRequest with assignments', () => {
  const groupId = 'test-group'
  const generationId = 5
  const memberId = 'test-member-1'
  const groupInstanceId = null

  // Define assignment data with clear test values
  const member1Id = 'member-1'
  const member2Id = 'member-2'
  const assignment1Data = 'assignment-data-1'
  const assignment2Data = 'assignment-data-2'

  const assignments = [
    {
      memberId: member1Id,
      assignment: Buffer.from(assignment1Data)
    },
    {
      memberId: member2Id,
      assignment: Buffer.from(assignment2Data)
    }
  ]

  const writer = createRequest(groupId, generationId, memberId, groupInstanceId, assignments)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read and collect all basic parameters in a single object
  const serializedParams = {
    groupId: reader.readString(),
    generationId: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readNullableString()
  }

  // Verify all basic parameters in a single assertion with descriptive message
  deepStrictEqual(
    serializedParams,
    {
      groupId,
      generationId,
      memberId,
      groupInstanceId
    },
    'Serialized basic parameters should match input values'
  )

  const serializedAssignments = reader.readArray(r => {
    return {
      memberId: r.readString(),
      assignmentData: r.readBytes()
    }
  })

  // The valid assignments we expected must be present
  const member1Assignment = serializedAssignments.find(a => a.memberId === member1Id)!
  const member2Assignment = serializedAssignments.find(a => a.memberId === member2Id)!

  ok(
    member1Assignment.assignmentData.toString('utf-8') === assignment1Data,
    'Member 1 assignment data should match expected value'
  )
  ok(
    member2Assignment.assignmentData.toString('utf-8') === assignment2Data,
    'Member 2 assignment data should match expected value'
  )
})

test('createRequest with group instance ID', () => {
  const groupId = 'test-group'
  const generationId = 5
  const memberId = 'test-member-1'
  const groupInstanceId = 'test-instance-id'
  const assignments: any[] = []

  const writer = createRequest(groupId, generationId, memberId, groupInstanceId, assignments)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all parameters into a single object
  const serializedData = {
    groupId: reader.readString(),
    generationId: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readString() // Read as string since we know it's not null
  }

  // Verify all parameters including group instance ID in one assertion
  deepStrictEqual(
    serializedData,
    {
      groupId,
      generationId,
      memberId,
      groupInstanceId
    },
    'Serialized parameters with group instance ID should match input values'
  )

  // Get assignments array length
  const assignmentsArrayLength = reader.readUnsignedVarInt()
  deepStrictEqual(assignmentsArrayLength, 1, 'Empty assignments array should have length 1 for compact arrays')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const assignment = Buffer.from('test-assignment-data')
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendBytes(assignment) // assignment
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 14, 4, Reader.from(writer))

  // Verify complete response structure in a single assertion
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      protocolType: null,
      protocolName: null,
      assignment
    },
    'Response object should match expected structure'
  )
})

test('parseResponse handles throttling', () => {
  // Create a response with throttling
  const assignment = Buffer.from('test-assignment-data')
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    .appendInt16(0) // errorCode (success)
    .appendBytes(assignment) // assignment
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 14, 4, Reader.from(writer))

  // Verify response structure with throttling in a single assertion
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 100,
      errorCode: 0,
      protocolType: null,
      protocolName: null,
      assignment
    },
    'Response with throttling should match expected structure'
  )
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(16) // errorCode (e.g., UNKNOWN_MEMBER_ID)
    .appendBytes(Buffer.from('')) // empty assignment
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 14, 4, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Error should be a ResponseError instance')
      ok(
        err.message.includes('Received response with error while executing API'),
        'Error message should include API execution error description'
      )

      // Check that errors object exists and has the correct type
      ok(err.errors && typeof err.errors === 'object', 'Error should have errors object')

      // Verify that the response structure is preserved in one assertion
      deepStrictEqual(
        err.response,
        {
          throttleTimeMs: 0,
          errorCode: 16,
          protocolType: null,
          protocolName: null,
          assignment: Buffer.from('')
        },
        'Error response should preserve the original response structure'
      )

      return true
    }
  )
})
