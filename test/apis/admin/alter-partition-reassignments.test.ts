import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { alterPartitionReassignmentsV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = alterPartitionReassignmentsV0

test('createRequest serializes basic parameters correctly', () => {
  const timeoutMs = 30000
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          replicas: [1, 2, 3]
        }
      ]
    }
  ]

  const writer = createRequest(timeoutMs, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read timeoutMs
  const timeout = reader.readInt32()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const replicas = reader.readArray(() => reader.readInt32(), true, false)
      return { partitionIndex, replicas }
    })

    return { name, partitions }
  })

  // Verify serialized data
  deepStrictEqual(
    {
      timeout,
      topics: topicsArray
    },
    {
      timeout: 30000,
      topics: [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              replicas: [1, 2, 3]
            }
          ]
        }
      ]
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple topics correctly', () => {
  const timeoutMs = 30000
  const topics = [
    {
      name: 'test-topic-1',
      partitions: [
        {
          partitionIndex: 0,
          replicas: [1, 2, 3]
        }
      ]
    },
    {
      name: 'test-topic-2',
      partitions: [
        {
          partitionIndex: 0,
          replicas: [2, 3, 4]
        }
      ]
    }
  ]

  const writer = createRequest(timeoutMs, topics)
  const reader = Reader.from(writer)

  // Skip timeoutMs
  reader.readInt32()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const replicas = reader.readArray(() => reader.readInt32(), true, false)
      return { partitionIndex, replicas }
    })

    return { name, partitions }
  })

  // Verify structure with multiple topics
  deepStrictEqual(
    topicsArray,
    [
      {
        name: 'test-topic-1',
        partitions: [
          {
            partitionIndex: 0,
            replicas: [1, 2, 3]
          }
        ]
      },
      {
        name: 'test-topic-2',
        partitions: [
          {
            partitionIndex: 0,
            replicas: [2, 3, 4]
          }
        ]
      }
    ],
    'Should correctly serialize multiple topics'
  )
})

test('createRequest serializes multiple partitions correctly', () => {
  const timeoutMs = 30000
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          replicas: [1, 2, 3]
        },
        {
          partitionIndex: 1,
          replicas: [2, 3, 4]
        },
        {
          partitionIndex: 2,
          replicas: [3, 4, 5]
        }
      ]
    }
  ]

  const writer = createRequest(timeoutMs, topics)
  const reader = Reader.from(writer)

  // Skip timeoutMs
  reader.readInt32()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const replicas = reader.readArray(() => reader.readInt32(), true, false)
      return { partitionIndex, replicas }
    })

    return { name, partitions }
  })

  // Verify multiple partitions structure
  deepStrictEqual(
    topicsArray[0].partitions,
    [
      {
        partitionIndex: 0,
        replicas: [1, 2, 3]
      },
      {
        partitionIndex: 1,
        replicas: [2, 3, 4]
      },
      {
        partitionIndex: 2,
        replicas: [3, 4, 5]
      }
    ],
    'Should correctly serialize multiple partitions'
  )
})

test('createRequest serializes empty replicas array correctly', () => {
  const timeoutMs = 30000
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          replicas: [] // Empty array to cancel reassignment
        }
      ]
    }
  ]

  const writer = createRequest(timeoutMs, topics)
  const reader = Reader.from(writer)

  // Skip timeoutMs
  reader.readInt32()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const replicas = reader.readArray(() => reader.readInt32(), true, false)
      return { partitionIndex, replicas }
    })

    return { name, partitions }
  })

  // Verify empty replicas array is correctly serialized
  deepStrictEqual(topicsArray[0].partitions[0].replicas, [], 'Should correctly serialize empty replicas array')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null
            }
          ]
        }
      ],
      (w, response) => {
        w.appendString(response.name).appendArray(response.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode).appendString(partition.errorMessage)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 45, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      errorMessage: null,
      responses: [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null
            }
          ]
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse handles global error correctly', () => {
  // Create a response with a global error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(41) // errorCode - CLUSTER_AUTHORIZATION_FAILED
    .appendString('Not authorized to alter partition reassignments') // errorMessage
    .appendArray([], () => {}) // Empty responses array
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 45, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(
        err.response,
        {
          throttleTimeMs: 0,
          errorCode: 41,
          errorMessage: 'Not authorized to alter partition reassignments',
          responses: []
        },
        'Error response should preserve the original response structure'
      )

      return true
    }
  )
})

test('parseResponse handles partition level errors correctly', () => {
  // Create a response with partition level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 9, // INVALID_REPLICA_ASSIGNMENT
              errorMessage: 'Invalid replica assignment'
            }
          ]
        }
      ],
      (w, response) => {
        w.appendString(response.name).appendArray(response.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode).appendString(partition.errorMessage)
        })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 45, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response contains the partition error
      deepStrictEqual(
        err.response.responses[0].partitions[0],
        {
          partitionIndex: 0,
          errorCode: 9,
          errorMessage: 'Invalid replica assignment'
        },
        'Error response should contain partition level error'
      )

      return true
    }
  )
})

test('parseResponse handles mixed partition successes and errors', () => {
  // Create a response with mixed partition results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null
            },
            {
              partitionIndex: 1,
              errorCode: 9, // INVALID_REPLICA_ASSIGNMENT
              errorMessage: 'Invalid replica assignment'
            }
          ]
        }
      ],
      (w, response) => {
        w.appendString(response.name).appendArray(response.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode).appendString(partition.errorMessage)
        })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 45, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify response contains both successful and error partitions
      deepStrictEqual(
        err.response.responses[0].partitions,
        [
          {
            partitionIndex: 0,
            errorCode: 0,
            errorMessage: null
          },
          {
            partitionIndex: 1,
            errorCode: 9,
            errorMessage: 'Invalid replica assignment'
          }
        ],
        'Response should contain both successful and error partitions'
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
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null
            }
          ]
        }
      ],
      (w, response) => {
        w.appendString(response.name).appendArray(response.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode).appendString(partition.errorMessage)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 45, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
