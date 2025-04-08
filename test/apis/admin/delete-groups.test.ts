import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { deleteGroupsV2, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = deleteGroupsV2

test('createRequest serializes group names correctly', () => {
  const groupNames = ['group-1', 'group-2', 'group-3']

  const writer = createRequest(groupNames)

  // Verify it returns a Writer instance
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read group names array
  const serializedGroupNames = reader.readArray(() => reader.readString(), true, false)

  // Read tagged fields count

  // Verify the complete structure
  deepStrictEqual(
    {
      groupNames: serializedGroupNames
    },
    {
      groupNames: ['group-1', 'group-2', 'group-3']
    },
    'Serialized data should match expected structure'
  )
})

test('createRequest serializes empty group names array correctly', () => {
  const groupNames: string[] = []

  const writer = createRequest(groupNames)

  // Verify it returns a Writer instance
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read group names array
  const serializedGroupNames = reader.readArray(() => reader.readString(), true, false)

  // Read tagged fields count

  // Verify the complete structure
  deepStrictEqual(
    {
      groupNames: serializedGroupNames
    },
    {
      groupNames: []
    },
    'Empty group names array should be serialized correctly'
  )
})

test('createRequest serializes special characters in group names', () => {
  const groupNames = ['group/1', 'group-with-hyphen', 'group.with.dots']

  const writer = createRequest(groupNames)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read group names array
  const serializedGroupNames = reader.readArray(() => reader.readString(), true, false)

  // Read tagged fields count

  // Verify the complete structure
  deepStrictEqual(
    {
      groupNames: serializedGroupNames
    },
    {
      groupNames: ['group/1', 'group-with-hyphen', 'group.with.dots']
    },
    'Group names with special characters should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Results array
    .appendArray(
      [
        {
          groupId: 'group-1',
          errorCode: 0 // Success
        },
        {
          groupId: 'group-2',
          errorCode: 0 // Success
        }
      ],
      (w, result) => {
        w.appendString(result.groupId).appendInt16(result.errorCode)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 42, 2, writer.bufferList)

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      results: [
        {
          groupId: 'group-1',
          errorCode: 0
        },
        {
          groupId: 'group-2',
          errorCode: 0
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse handles throttling correctly', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero for throttling)
    // Results array - just one for simplicity
    .appendArray(
      [
        {
          groupId: 'group-1',
          errorCode: 0 // Success
        }
      ],
      (w, result) => {
        w.appendString(result.groupId).appendInt16(result.errorCode)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 42, 2, writer.bufferList)

  // Verify throttling is processed correctly
  deepStrictEqual(response.throttleTimeMs, 100, 'Throttle time should be correctly parsed')

  // Verify the rest of the response is still correct
  deepStrictEqual(
    response.results,
    [
      {
        groupId: 'group-1',
        errorCode: 0
      }
    ],
    'Results should be correctly parsed even with throttling'
  )
})

test('parseResponse handles single group error correctly', () => {
  // Create a response with one group having an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Results array with one error
    .appendArray(
      [
        {
          groupId: 'group-1',
          errorCode: 15 // NON_EXISTENT_GROUP
        }
      ],
      (w, result) => {
        w.appendString(result.groupId).appendInt16(result.errorCode)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 42, 2, writer.bufferList)
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
          results: [
            {
              groupId: 'group-1',
              errorCode: 15
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
    // Results array with mixed results
    .appendArray(
      [
        {
          groupId: 'group-1',
          errorCode: 0 // Success
        },
        {
          groupId: 'group-2',
          errorCode: 15 // NON_EXISTENT_GROUP
        },
        {
          groupId: 'group-3',
          errorCode: 41 // GROUP_AUTHORIZATION_FAILED
        }
      ],
      (w, result) => {
        w.appendString(result.groupId).appendInt16(result.errorCode)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 42, 2, writer.bufferList)
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
      const errorCodes = err.response.results
        .filter((r: Record<string, number>) => r.errorCode !== 0)
        .map((r: Record<string, number>) => r.errorCode)

      // Verify we have both expected error codes
      ok(
        errorCodes.includes(15) && errorCodes.includes(41),
        'Response should contain groups with expected error codes (15 and 41)'
      )

      // Verify all groups are preserved in the response
      deepStrictEqual(err.response.results.length, 3, 'Response should contain all 3 groups')

      // Verify the successful group data is preserved
      deepStrictEqual(
        err.response.results.find((r: Record<string, number>) => r.errorCode === 0),
        {
          groupId: 'group-1',
          errorCode: 0
        },
        'Successful group should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse with empty results array', () => {
  // Create a response with an empty results array
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Empty results array
    .appendArray([], () => {})
    .appendTaggedFields()

  const response = parseResponse(1, 42, 2, writer.bufferList)

  // Verify response with empty results
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      results: []
    },
    'Response with empty results should be parsed correctly'
  )
})
