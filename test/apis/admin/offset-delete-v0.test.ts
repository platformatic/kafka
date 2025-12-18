import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { offsetDeleteV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetDeleteV0

test('createRequest serializes basic parameters correctly', () => {
  const groupId = 'test-group'
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0
        }
      ]
    }
  ]

  const writer = createRequest(groupId, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read groupId
  const serializedGroupId = reader.readString(false)

  // Read topics array
  const topicsArray = reader.readArray(
    () => {
      const name = reader.readString(false)

      // Read partitions array
      const partitions = reader.readArray(
        () => {
          const partitionIndex = reader.readInt32()
          return { partitionIndex }
        },
        false,
        false
      )

      return { name, partitions }
    },
    false,
    false
  )

  // Verify serialized data
  deepStrictEqual(
    {
      groupId: serializedGroupId,
      topics: topicsArray
    },
    {
      groupId: 'test-group',
      topics: [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0
            }
          ]
        }
      ]
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple topics correctly', () => {
  const groupId = 'test-group'
  const topics = [
    {
      name: 'test-topic-1',
      partitions: [
        {
          partitionIndex: 0
        }
      ]
    },
    {
      name: 'test-topic-2',
      partitions: [
        {
          partitionIndex: 0
        }
      ]
    }
  ]

  const writer = createRequest(groupId, topics)
  const reader = Reader.from(writer)

  // Skip groupId
  reader.readString(false)

  // Read topics array
  const topicsArray = reader.readArray(
    () => {
      const name = reader.readString(false)

      // Read partitions array
      const partitions = reader.readArray(
        () => {
          const partitionIndex = reader.readInt32()
          return { partitionIndex }
        },
        false,
        false
      )

      return { name, partitions }
    },
    false,
    false
  )

  // Verify multiple topics
  deepStrictEqual(
    topicsArray.map(t => t.name),
    ['test-topic-1', 'test-topic-2'],
    'Multiple topics should be serialized correctly'
  )
})

test('createRequest serializes multiple partitions correctly', () => {
  const groupId = 'test-group'
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0
        },
        {
          partitionIndex: 1
        },
        {
          partitionIndex: 2
        }
      ]
    }
  ]

  const writer = createRequest(groupId, topics)
  const reader = Reader.from(writer)

  // Skip groupId
  reader.readString(false)

  // Read topics array
  const topicsArray = reader.readArray(
    () => {
      const name = reader.readString(false)

      // Read partitions array
      const partitions = reader.readArray(
        () => {
          const partitionIndex = reader.readInt32()
          return { partitionIndex }
        },
        false,
        false
      )

      return { name, partitions }
    },
    false,
    false
  )

  // Verify multiple partitions
  deepStrictEqual(
    topicsArray[0].partitions,
    [
      {
        partitionIndex: 0
      },
      {
        partitionIndex: 1
      },
      {
        partitionIndex: 2
      }
    ],
    'Multiple partitions should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
          },
          false,
          false
        )
      },
      false,
      false
    )

  const response = parseResponse(1, 47, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      errorCode: 0,
      throttleTimeMs: 0,
      topics: [
        {
          name: 'test-topic',
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

test('parseResponse correctly processes multiple topics', () => {
  // Create a response with multiple topics
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic-1',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            }
          ]
        },
        {
          name: 'test-topic-2',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
          },
          false,
          false
        )
      },
      false,
      false
    )

  const response = parseResponse(1, 47, 0, Reader.from(writer))

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
    .appendInt16(0) // errorCode
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            },
            {
              partitionIndex: 1,
              errorCode: 0
            },
            {
              partitionIndex: 2,
              errorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
          },
          false,
          false
        )
      },
      false,
      false
    )

  const response = parseResponse(1, 47, 0, Reader.from(writer))

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
    .appendInt16(0) // Top-level errorCode (success)
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 3 // UNKNOWN_TOPIC_OR_PARTITION
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
          },
          false,
          false
        )
      },
      false,
      false
    )

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 47, 0, Reader.from(writer))
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
    .appendInt16(0) // Top-level errorCode (success)
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0 // Success
            },
            {
              partitionIndex: 1,
              errorCode: 3 // UNKNOWN_TOPIC_OR_PARTITION
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
          },
          false,
          false
        )
      },
      false,
      false
    )

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 47, 0, Reader.from(writer))
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

test('parseResponse handles top-level error correctly', () => {
  // Create a response with a top-level error
  const writer = Writer.create()
    .appendInt16(16) // GROUP_AUTHORIZATION_FAILED
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}, false, false) // Empty topics array

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 47, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')
      deepStrictEqual(err.response.errorCode, 16, 'Top-level error code should be preserved')
      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendInt32(throttleTimeMs)
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
          },
          false,
          false
        )
      },
      false,
      false
    )

  const response = parseResponse(1, 47, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
