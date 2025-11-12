import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { createPartitionsV2, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = createPartitionsV2

test('createRequest serializes basic parameters correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      count: 3,
      assignments: []
    }
  ]
  const timeoutMs = 30000
  const validateOnly = false

  const writer = createRequest(topics, timeoutMs, validateOnly)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const count = reader.readInt32()

    // Read assignments array (empty in this case)
    const assignments = reader.readArray(() => {
      const brokerIds = reader.readArray(() => reader.readInt32(), true, false)
      return { brokerIds }
    })

    return { name, count, assignments }
  })

  // Read timeoutMs and validateOnly
  const serializedTimeoutMs = reader.readInt32()
  const serializedValidateOnly = reader.readBoolean()

  // Verify serialized data
  deepStrictEqual(
    {
      topics: topicsArray,
      timeoutMs: serializedTimeoutMs,
      validateOnly: serializedValidateOnly
    },
    {
      topics: [
        {
          name: 'test-topic',
          count: 3,
          assignments: []
        }
      ],
      timeoutMs: 30000,
      validateOnly: false
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes topic with assignments correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      count: 3,
      assignments: [
        {
          brokerIds: [1, 2, 3]
        }
      ]
    }
  ]
  const timeoutMs = 30000
  const validateOnly = false

  const writer = createRequest(topics, timeoutMs, validateOnly)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const count = reader.readInt32()

    // Read assignments array
    const assignments = reader.readArray(() => {
      const brokerIds = reader.readArray(() => reader.readInt32(), true, false)
      return { brokerIds }
    })

    return { name, count, assignments }
  })

  // Skip timeoutMs and validateOnly
  reader.readInt32()
  reader.readBoolean()

  // Verify broker IDs in assignments
  deepStrictEqual(
    topicsArray[0].assignments[0].brokerIds,
    [1, 2, 3],
    'Broker IDs in assignments should be serialized correctly'
  )
})

test('createRequest serializes multiple topics correctly', () => {
  const topics = [
    {
      name: 'test-topic-1',
      count: 3,
      assignments: []
    },
    {
      name: 'test-topic-2',
      count: 6,
      assignments: []
    }
  ]
  const timeoutMs = 30000
  const validateOnly = false

  const writer = createRequest(topics, timeoutMs, validateOnly)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const count = reader.readInt32()

    // Skip assignments details
    reader.readArray(() => {
      reader.readArray(() => reader.readInt32(), true, false)
      return {}
    })

    return { name, count }
  })

  // Skip timeoutMs and validateOnly
  reader.readInt32()
  reader.readBoolean()

  // Verify multiple topics
  deepStrictEqual(
    topicsArray,
    [
      {
        name: 'test-topic-1',
        count: 3
      },
      {
        name: 'test-topic-2',
        count: 6
      }
    ],
    'Multiple topics should be serialized correctly'
  )
})

test('createRequest serializes multiple assignments correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      count: 3,
      assignments: [
        {
          brokerIds: [1, 2, 3]
        },
        {
          brokerIds: [2, 3, 4]
        }
      ]
    }
  ]
  const timeoutMs = 30000
  const validateOnly = false

  const writer = createRequest(topics, timeoutMs, validateOnly)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const count = reader.readInt32()

    // Read assignments array
    const assignments = reader.readArray(() => {
      const brokerIds = reader.readArray(() => reader.readInt32(), true, false)
      return { brokerIds }
    })

    return { name, count, assignments }
  })

  // Skip timeoutMs and validateOnly
  reader.readInt32()
  reader.readBoolean()

  // Verify multiple assignments
  deepStrictEqual(
    topicsArray[0].assignments,
    [
      {
        brokerIds: [1, 2, 3]
      },
      {
        brokerIds: [2, 3, 4]
      }
    ],
    'Multiple assignments should be serialized correctly'
  )
})

test('createRequest serializes validateOnly flag correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      count: 3,
      assignments: []
    }
  ]
  const timeoutMs = 30000
  const validateOnly = true

  const writer = createRequest(topics, timeoutMs, validateOnly)
  const reader = Reader.from(writer)

  // Skip topics array
  reader.readArray(() => {
    reader.readString()
    reader.readInt32()
    reader.readArray(() => {
      reader.readArray(() => reader.readInt32(), true, false)
      return {}
    })
    return {}
  })

  // Skip timeoutMs
  reader.readInt32()

  // Read validateOnly flag
  const serializedValidateOnly = reader.readBoolean()

  // Verify validateOnly flag
  ok(serializedValidateOnly === true, 'validateOnly flag should be set to true')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendString(result.name).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 37, 2, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      results: [
        {
          name: 'test-topic',
          errorCode: 0,
          errorMessage: null
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse correctly processes multiple results', () => {
  // Create a response with multiple results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic-1',
          errorCode: 0,
          errorMessage: null
        },
        {
          name: 'test-topic-2',
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendString(result.name).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 37, 2, Reader.from(writer))

  // Verify multiple results
  deepStrictEqual(
    response.results.map(r => r.name),
    ['test-topic-1', 'test-topic-2'],
    'Response should correctly parse multiple results'
  )
})

test('parseResponse handles error responses', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          errorCode: 37, // INVALID_PARTITIONS
          errorMessage: 'Invalid partition count'
        }
      ],
      (w, result) => {
        w.appendString(result.name).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 37, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(
        err.response.results[0],
        {
          name: 'test-topic',
          errorCode: 37,
          errorMessage: 'Invalid partition count'
        },
        'Error response should preserve the original error details'
      )

      return true
    }
  )
})

test('parseResponse handles mixed results with success and errors', () => {
  // Create a response with mixed results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'good-topic',
          errorCode: 0,
          errorMessage: null
        },
        {
          name: 'bad-topic',
          errorCode: 37, // INVALID_PARTITIONS
          errorMessage: 'Invalid partition count'
        }
      ],
      (w, result) => {
        w.appendString(result.name).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 37, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify response contains both successful and error results
      deepStrictEqual(
        err.response.results.map((r: Record<string, number>) => ({ name: r.name, errorCode: r.errorCode })),
        [
          { name: 'good-topic', errorCode: 0 },
          { name: 'bad-topic', errorCode: 37 }
        ],
        'Response should contain both successful and error results'
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
          name: 'test-topic',
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendString(result.name).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 37, 2, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
