import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { deleteTopicsV6, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = deleteTopicsV6

test('createRequest serializes topic names correctly', () => {
  const topics = [{ name: 'topic-1' }, { name: 'topic-2' }]
  const timeoutMs = 30000

  const writer = createRequest(topics, timeoutMs)

  // Verify it returns a Writer instance
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read topics array
  const serializedTopics = reader.readArray(() => {
    const name = reader.readString()
    const topicId = reader.readUUID()
    return { name, topicId }
  })

  // Read timeout and tagged fields
  const timeout = reader.readInt32()

  // Verify the complete structure
  deepStrictEqual(
    {
      topics: serializedTopics.map(t => ({
        name: t.name,
        hasNullTopicId: t.topicId === '00000000-0000-0000-0000-000000000000'
      })),
      timeout
    },
    {
      topics: [
        { name: 'topic-1', hasNullTopicId: true },
        { name: 'topic-2', hasNullTopicId: true }
      ],
      timeout: 30000
    },
    'Serialized data should match expected structure'
  )
})

test('createRequest serializes topic names and topic IDs correctly', () => {
  const topics = [
    {
      name: 'topic-1',
      topicId: '12345678-1234-1234-1234-123456789abc'
    },
    {
      name: 'topic-2',
      topicId: '87654321-4321-4321-4321-cba987654321'
    }
  ]
  const timeoutMs = 30000

  const writer = createRequest(topics, timeoutMs)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read topics array
  const serializedTopics = reader.readArray(() => {
    const name = reader.readString()
    const topicId = reader.readUUID()
    return { name, topicId }
  })

  // Read timeout and tagged fields
  const timeout = reader.readInt32()

  // Verify the complete structure
  deepStrictEqual(
    {
      topics: serializedTopics,
      timeout
    },
    {
      topics: [
        { name: 'topic-1', topicId: '12345678-1234-1234-1234-123456789abc' },
        { name: 'topic-2', topicId: '87654321-4321-4321-4321-cba987654321' }
      ],
      timeout: 30000
    },
    'Serialized data with topic IDs should match expected structure'
  )
})

test('createRequest serializes empty topics array correctly', () => {
  const topics: Array<{ name: string; topicId?: string }> = []
  const timeoutMs = 30000

  const writer = createRequest(topics, timeoutMs)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read topics array
  const serializedTopics = reader.readArray(() => {
    const name = reader.readString()
    const topicId = reader.readUUID()
    return { name, topicId }
  })

  // Read timeout and tagged fields
  const timeout = reader.readInt32()

  // Verify the complete structure
  deepStrictEqual(
    {
      topics: serializedTopics,
      timeout
    },
    {
      topics: [],
      timeout: 30000
    },
    'Serialized data with empty topics should match expected structure'
  )
})

test('createRequest serializes different timeout values correctly', () => {
  const topics = [{ name: 'topic-1' }]
  const timeoutMs = 5000 // Different timeout value

  const writer = createRequest(topics, timeoutMs)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Skip topics array
  reader.readArray(() => {
    reader.readString()
    reader.readUUID()
    return {}
  })

  // Read timeout and tagged fields
  const timeout = reader.readInt32()

  // Verify timeout and tagged fields
  deepStrictEqual(
    {
      timeout
    },
    {
      timeout: 5000
    },
    'Timeout value should be correctly serialized'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Responses array
    .appendArray(
      [
        {
          name: 'topic-1',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0, // Success
          errorMessage: null
        },
        {
          name: 'topic-2',
          topicId: '87654321-4321-4321-4321-cba987654321',
          errorCode: 0, // Success
          errorMessage: null
        }
      ],
      (w, response) => {
        w.appendString(response.name)
          .appendUUID(response.topicId)
          .appendInt16(response.errorCode)
          .appendString(response.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 20, 6, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      responses: [
        {
          name: 'topic-1',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0,
          errorMessage: null
        },
        {
          name: 'topic-2',
          topicId: '87654321-4321-4321-4321-cba987654321',
          errorCode: 0,
          errorMessage: null
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
    // Responses array - just one for simplicity
    .appendArray(
      [
        {
          name: 'topic-1',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0, // Success
          errorMessage: null
        }
      ],
      (w, response) => {
        w.appendString(response.name)
          .appendUUID(response.topicId)
          .appendInt16(response.errorCode)
          .appendString(response.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 20, 6, Reader.from(writer))

  // Verify throttling is processed correctly
  deepStrictEqual(response.throttleTimeMs, 100, 'Throttle time should be correctly parsed')

  // Verify the rest of the response is still correct
  deepStrictEqual(
    response.responses,
    [
      {
        name: 'topic-1',
        topicId: '12345678-1234-1234-1234-123456789abc',
        errorCode: 0,
        errorMessage: null
      }
    ],
    'Responses should be correctly parsed even with throttling'
  )
})

test('parseResponse handles single topic error correctly', () => {
  // Create a response with one topic having an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Responses array with one error
    .appendArray(
      [
        {
          name: 'topic-1',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
          errorMessage: 'The topic does not exist'
        }
      ],
      (w, response) => {
        w.appendString(response.name)
          .appendUUID(response.topicId)
          .appendInt16(response.errorCode)
          .appendString(response.errorMessage)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 20, 6, Reader.from(writer))
    },
    (err: any) => {
      // Verify error is a ResponseError
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify the error object has the expected properties
      ok(Array.isArray(err.errors) && err.errors.length === 1, 'Should have an array with 1 error for the failed topic')

      // Verify the response structure is preserved
      deepStrictEqual(
        err.response,
        {
          throttleTimeMs: 0,
          responses: [
            {
              name: 'topic-1',
              topicId: '12345678-1234-1234-1234-123456789abc',
              errorCode: 3,
              errorMessage: 'The topic does not exist'
            }
          ]
        },
        'Error response should preserve the original response structure'
      )

      return true
    }
  )
})

test('parseResponse handles multiple topics with mixed errors', () => {
  // Create a response with multiple topics and mixed results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Responses array with mixed results
    .appendArray(
      [
        {
          name: 'topic-1',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0, // Success
          errorMessage: null
        },
        {
          name: 'topic-2',
          topicId: '87654321-4321-4321-4321-cba987654321',
          errorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
          errorMessage: 'The topic does not exist'
        },
        {
          name: 'topic-3',
          topicId: 'abcdef12-3456-7890-abcd-ef1234567890',
          errorCode: 41, // TOPIC_AUTHORIZATION_FAILED
          errorMessage: 'Authorization failed'
        }
      ],
      (w, response) => {
        w.appendString(response.name)
          .appendUUID(response.topicId)
          .appendInt16(response.errorCode)
          .appendString(response.errorMessage)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 20, 6, Reader.from(writer))
    },
    (err: any) => {
      // Verify error is a ResponseError
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify there are multiple errors
      ok(
        Array.isArray(err.errors) && err.errors.length === 2,
        'Should have an array with 2 errors for the failed topics'
      )

      // Get error codes from the response
      const errorCodes = err.response.responses
        .filter((r: Record<string, number>) => r.errorCode !== 0)
        .map((r: Record<string, number>) => r.errorCode)

      // Verify we have both expected error codes
      ok(
        errorCodes.includes(3) && errorCodes.includes(41),
        'Response should contain topics with expected error codes (3 and 41)'
      )

      // Verify all topics are preserved in the response
      deepStrictEqual(err.response.responses.length, 3, 'Response should contain all 3 topics')

      // Verify the successful topic data is preserved
      deepStrictEqual(
        err.response.responses.find((r: Record<string, number>) => r.errorCode === 0),
        {
          name: 'topic-1',
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0,
          errorMessage: null
        },
        'Successful topic should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse handles response with null topic names', () => {
  // Create a response with a null topic name (using topicId for deletion)
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Responses array with null name
    .appendArray(
      [
        {
          name: null,
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0, // Success
          errorMessage: null
        }
      ],
      (w, response) => {
        w.appendString(response.name)
          .appendUUID(response.topicId)
          .appendInt16(response.errorCode)
          .appendString(response.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 20, 6, Reader.from(writer))

  // Verify null name is handled correctly
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      responses: [
        {
          name: null,
          topicId: '12345678-1234-1234-1234-123456789abc',
          errorCode: 0,
          errorMessage: null
        }
      ]
    },
    'Response with null topic name should be parsed correctly'
  )
})

test('parseResponse with empty responses array', () => {
  // Create a response with an empty responses array
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Empty responses array
    .appendArray([], () => {})
    .appendTaggedFields()

  const response = parseResponse(1, 20, 6, Reader.from(writer))

  // Verify response with empty results
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      responses: []
    },
    'Response with empty responses should be parsed correctly'
  )
})
