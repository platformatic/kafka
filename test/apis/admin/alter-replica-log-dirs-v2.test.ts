import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { alterReplicaLogDirsV2, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = alterReplicaLogDirsV2

test('createRequest serializes basic parameters correctly', () => {
  const dirs = [
    {
      path: '/var/lib/kafka/data-1',
      topics: [
        {
          name: 'test-topic',
          partitions: [0]
        }
      ]
    }
  ]

  const writer = createRequest(dirs)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read dirs array
  const dirsArray = reader.readArray(() => {
    const path = reader.readString()

    // Read topics array
    const topics = reader.readArray(() => {
      const name = reader.readString()

      // Read partitions array
      const partitions = reader.readArray(() => reader.readInt32(), true, false)

      return { name, partitions }
    })

    return { path, topics }
  })

  // Verify serialized data
  deepStrictEqual(
    dirsArray,
    [
      {
        path: '/var/lib/kafka/data-1',
        topics: [
          {
            name: 'test-topic',
            partitions: [0]
          }
        ]
      }
    ],
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple directories correctly', () => {
  const dirs = [
    {
      path: '/var/lib/kafka/data-1',
      topics: [
        {
          name: 'test-topic-1',
          partitions: [0]
        }
      ]
    },
    {
      path: '/var/lib/kafka/data-2',
      topics: [
        {
          name: 'test-topic-2',
          partitions: [0]
        }
      ]
    }
  ]

  const writer = createRequest(dirs)
  const reader = Reader.from(writer)

  // Read dirs array
  const dirsArray = reader.readArray(() => {
    const path = reader.readString()

    // Read topics array (simplified)
    const topics = reader.readArray(() => {
      const name = reader.readString()

      // Skip partitions details for this test
      reader.readArray(() => reader.readInt32(), true, false)

      return { name }
    })

    return { path, topics }
  })

  // Verify directory paths and topic names
  deepStrictEqual(
    dirsArray.map(d => ({ path: d.path, topicName: d.topics[0].name })),
    [
      { path: '/var/lib/kafka/data-1', topicName: 'test-topic-1' },
      { path: '/var/lib/kafka/data-2', topicName: 'test-topic-2' }
    ],
    'Should correctly serialize multiple directories with their topics'
  )
})

test('createRequest serializes multiple topics per directory correctly', () => {
  const dirs = [
    {
      path: '/var/lib/kafka/data-1',
      topics: [
        {
          name: 'test-topic-1',
          partitions: [0]
        },
        {
          name: 'test-topic-2',
          partitions: [0]
        }
      ]
    }
  ]

  const writer = createRequest(dirs)
  const reader = Reader.from(writer)

  // Read dirs array
  const dirsArray = reader.readArray(() => {
    const path = reader.readString()

    // Read topics array
    const topics = reader.readArray(() => {
      const name = reader.readString()

      // Skip partitions details for this test
      reader.readArray(() => reader.readInt32(), true, false)

      return { name }
    })

    return { path, topics }
  })

  // Verify topics under the same directory
  deepStrictEqual(
    dirsArray[0].topics.map(t => t.name),
    ['test-topic-1', 'test-topic-2'],
    'Should correctly serialize multiple topics under the same directory'
  )
})

test('createRequest serializes multiple partitions correctly', () => {
  const dirs = [
    {
      path: '/var/lib/kafka/data-1',
      topics: [
        {
          name: 'test-topic',
          partitions: [0, 1, 2]
        }
      ]
    }
  ]

  const writer = createRequest(dirs)
  const reader = Reader.from(writer)

  // Read dirs array
  const dirsArray = reader.readArray(() => {
    const path = reader.readString()

    // Read topics array
    const topics = reader.readArray(() => {
      const name = reader.readString()

      // Read partitions array
      const partitions = reader.readArray(() => reader.readInt32(), true, false)

      return { name, partitions }
    })

    return { path, topics }
  })

  // Verify partitions
  deepStrictEqual(dirsArray[0].topics[0].partitions, [0, 1, 2], 'Should correctly serialize multiple partitions')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            }
          ]
        }
      ],
      (w, result) => {
        w.appendString(result.topicName).appendArray(result.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 34, 2, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      results: [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            }
          ]
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse handles multiple results correctly', () => {
  // Create a response with multiple results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          topicName: 'test-topic-1',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            }
          ]
        },
        {
          topicName: 'test-topic-2',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            }
          ]
        }
      ],
      (w, result) => {
        w.appendString(result.topicName).appendArray(result.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 34, 2, Reader.from(writer))

  // Verify multiple results
  deepStrictEqual(
    response.results.map(r => r.topicName),
    ['test-topic-1', 'test-topic-2'],
    'Response should correctly parse multiple results'
  )
})

test('parseResponse handles partition level errors correctly', () => {
  // Create a response with partition level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 57 // REPLICA_NOT_AVAILABLE
            }
          ]
        }
      ],
      (w, result) => {
        w.appendString(result.topicName).appendArray(result.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 34, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response contains the partition error
      deepStrictEqual(
        err.response.results[0].partitions[0],
        {
          partitionIndex: 0,
          errorCode: 57
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
    .appendArray(
      [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0 // Success
            },
            {
              partitionIndex: 1,
              errorCode: 57 // REPLICA_NOT_AVAILABLE
            }
          ]
        }
      ],
      (w, result) => {
        w.appendString(result.topicName).appendArray(result.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 34, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify response contains both successful and error partitions
      deepStrictEqual(
        err.response.results[0].partitions,
        [
          {
            partitionIndex: 0,
            errorCode: 0
          },
          {
            partitionIndex: 1,
            errorCode: 57
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
    .appendArray(
      [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            }
          ]
        }
      ],
      (w, result) => {
        w.appendString(result.topicName).appendArray(result.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 34, 2, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
