import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { deleteRecordsV2, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = deleteRecordsV2

test('createRequest serializes basic parameters correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          offset: BigInt(100)
        }
      ]
    }
  ]
  const timeoutMs = 30000

  const writer = createRequest(topics, timeoutMs)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const offset = reader.readInt64()
      return { partitionIndex, offset }
    })

    return { name, partitions }
  })

  // Read timeoutMs
  const serializedTimeoutMs = reader.readInt32()

  // Verify serialized data
  deepStrictEqual(
    {
      topics: topicsArray,
      timeoutMs: serializedTimeoutMs
    },
    {
      topics: [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              offset: BigInt(100)
            }
          ]
        }
      ],
      timeoutMs: 30000
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple topics correctly', () => {
  const topics = [
    {
      name: 'test-topic-1',
      partitions: [
        {
          partitionIndex: 0,
          offset: BigInt(100)
        }
      ]
    },
    {
      name: 'test-topic-2',
      partitions: [
        {
          partitionIndex: 0,
          offset: BigInt(200)
        }
      ]
    }
  ]
  const timeoutMs = 30000

  const writer = createRequest(topics, timeoutMs)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const offset = reader.readInt64()
      return { partitionIndex, offset }
    })

    return { name, partitions }
  })

  // Skip timeoutMs
  reader.readInt32()

  // Verify multiple topics
  deepStrictEqual(
    topicsArray.map(t => t.name),
    ['test-topic-1', 'test-topic-2'],
    'Multiple topics should be serialized correctly'
  )
})

test('createRequest serializes multiple partitions correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          offset: BigInt(100)
        },
        {
          partitionIndex: 1,
          offset: BigInt(200)
        },
        {
          partitionIndex: 2,
          offset: BigInt(300)
        }
      ]
    }
  ]
  const timeoutMs = 30000

  const writer = createRequest(topics, timeoutMs)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const offset = reader.readInt64()
      return { partitionIndex, offset }
    })

    return { name, partitions }
  })

  // Skip timeoutMs
  reader.readInt32()

  // Verify multiple partitions
  deepStrictEqual(
    topicsArray[0].partitions,
    [
      {
        partitionIndex: 0,
        offset: BigInt(100)
      },
      {
        partitionIndex: 1,
        offset: BigInt(200)
      },
      {
        partitionIndex: 2,
        offset: BigInt(300)
      }
    ],
    'Multiple partitions should be serialized correctly'
  )
})

test('createRequest handles large offsets correctly', () => {
  const largeOffset = BigInt('9223372036854775807') // Max int64 value
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          offset: largeOffset
        }
      ]
    }
  ]
  const timeoutMs = 30000

  const writer = createRequest(topics, timeoutMs)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const offset = reader.readInt64()
      return { partitionIndex, offset }
    })

    return { name, partitions }
  })

  // Skip timeoutMs
  reader.readInt32()

  // Verify large offset
  deepStrictEqual(topicsArray[0].partitions[0].offset, largeOffset, 'Large offsets should be serialized correctly')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              lowWatermark: BigInt(100),
              errorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt64(partition.lowWatermark).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 21, 2, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    {
      throttleTimeMs: response.throttleTimeMs,
      topics: [
        {
          name: response.topics[0].name,
          partitions: [
            {
              partitionIndex: response.topics[0].partitions[0].partitionIndex,
              lowWatermark: response.topics[0].partitions[0].lowWatermark,
              errorCode: response.topics[0].partitions[0].errorCode
            }
          ]
        }
      ]
    },
    {
      throttleTimeMs: 0,
      topics: [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              lowWatermark: BigInt(100),
              errorCode: 0
            }
          ]
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse correctly processes multiple topics', () => {
  // Create a response with multiple topics
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic-1',
          partitions: [
            {
              partitionIndex: 0,
              lowWatermark: BigInt(100),
              errorCode: 0
            }
          ]
        },
        {
          name: 'test-topic-2',
          partitions: [
            {
              partitionIndex: 0,
              lowWatermark: BigInt(200),
              errorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt64(partition.lowWatermark).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 21, 2, Reader.from(writer))

  // Verify multiple topics
  deepStrictEqual(response.topics.length, 2, 'Response should contain 2 topics')

  deepStrictEqual(
    response.topics.map(t => t.name),
    ['test-topic-1', 'test-topic-2'],
    'Response should contain correct topic names'
  )
})

test('parseResponse correctly processes multiple partitions', () => {
  // Create a response with multiple partitions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              lowWatermark: BigInt(100),
              errorCode: 0
            },
            {
              partitionIndex: 1,
              lowWatermark: BigInt(200),
              errorCode: 0
            },
            {
              partitionIndex: 2,
              lowWatermark: BigInt(300),
              errorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt64(partition.lowWatermark).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 21, 2, Reader.from(writer))

  // Verify multiple partitions
  deepStrictEqual(response.topics[0].partitions.length, 3, 'Response should contain 3 partitions')

  deepStrictEqual(
    response.topics[0].partitions.map(p => p.partitionIndex),
    [0, 1, 2],
    'Response should contain correct partition indices'
  )
})

test('parseResponse handles partition level errors correctly', () => {
  // Create a response with partition-level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              lowWatermark: BigInt(-1),
              errorCode: 3 // UNKNOWN_TOPIC_OR_PARTITION
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt64(partition.lowWatermark).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 21, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify partition error is preserved
      deepStrictEqual(err.response.topics[0].partitions[0].errorCode, 3, 'Partition error code should be preserved')

      return true
    }
  )
})

test('parseResponse handles mixed partition successes and errors', () => {
  // Create a response with mixed partition successes and errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              lowWatermark: BigInt(100),
              errorCode: 0 // Success
            },
            {
              partitionIndex: 1,
              lowWatermark: BigInt(-1),
              errorCode: 3 // UNKNOWN_TOPIC_OR_PARTITION
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt64(partition.lowWatermark).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 21, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify both success and error partitions are preserved
      deepStrictEqual(
        err.response.topics[0].partitions.map((p: Record<string, number>) => ({
          partitionIndex: p.partitionIndex,
          errorCode: p.errorCode
        })),
        [
          { partitionIndex: 0, errorCode: 0 },
          { partitionIndex: 1, errorCode: 3 }
        ],
        'Both success and error partitions should be preserved'
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
          partitions: [
            {
              partitionIndex: 0,
              lowWatermark: BigInt(100),
              errorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt64(partition.lowWatermark).appendInt16(partition.errorCode)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 21, 2, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
