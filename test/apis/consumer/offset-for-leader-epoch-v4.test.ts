import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { offsetForLeaderEpochV4, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetForLeaderEpochV4

test('createRequest serializes basic parameters correctly', () => {
  const replicaId = -1
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 0,
          leaderEpoch: 1
        }
      ]
    }
  ]

  const writer = createRequest(replicaId, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Verify replica_id
  deepStrictEqual(reader.readInt32(), replicaId)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const currentLeaderEpoch = reader.readInt32()
      const leaderEpoch = reader.readInt32()

      return {
        partitionIndex,
        currentLeaderEpoch,
        leaderEpoch
      }
    })

    return { name, partitions }
  })

  // Verify the topics details
  deepStrictEqual(topicsArray, [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 0,
          leaderEpoch: 1
        }
      ]
    }
  ])
})

test('createRequest serializes multiple topics and partitions', () => {
  const replicaId = -1
  const topics = [
    {
      name: 'topic-1',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 5,
          leaderEpoch: 10
        },
        {
          partitionIndex: 1,
          currentLeaderEpoch: 3,
          leaderEpoch: 8
        }
      ]
    },
    {
      name: 'topic-2',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 2,
          leaderEpoch: 6
        }
      ]
    }
  ]

  const writer = createRequest(replicaId, topics)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Verify replica_id
  deepStrictEqual(reader.readInt32(), replicaId)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const partitions = reader.readArray(() => {
      return {
        partitionIndex: reader.readInt32(),
        currentLeaderEpoch: reader.readInt32(),
        leaderEpoch: reader.readInt32()
      }
    })
    return { name, partitions }
  })

  // Verify the topics details
  deepStrictEqual(topicsArray, [
    {
      name: 'topic-1',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 5,
          leaderEpoch: 10
        },
        {
          partitionIndex: 1,
          currentLeaderEpoch: 3,
          leaderEpoch: 8
        }
      ]
    },
    {
      name: 'topic-2',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 2,
          leaderEpoch: 6
        }
      ]
    }
  ])
})

test('createRequest handles empty topics array', () => {
  const replicaId = -1
  const topics: Array<{
    name: string
    partitions: Array<{ partitionIndex: number; currentLeaderEpoch: number; leaderEpoch: number }>
  }> = []

  const writer = createRequest(replicaId, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Verify replica_id
  deepStrictEqual(reader.readInt32(), replicaId)

  // Read topics array (should be empty)
  const topicsArray = reader.readArray(() => {})

  // Verify the topics array is empty
  deepStrictEqual(topicsArray, [])
})

test('parseResponse correctly processes a successful simple response', () => {
  // Create a successful response with one topic and partition
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array
    .appendArray(
      [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              leaderEpoch: 1,
              endOffset: 100n
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topic)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partition)
              .appendInt16(partition.errorCode)
              .appendInt32(partition.leaderEpoch)
              .appendInt64(partition.endOffset)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 23, 4, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    topics: [
      {
        topic: 'test-topic',
        partitions: [
          {
            partition: 0,
            errorCode: 0,
            leaderEpoch: 1,
            endOffset: 100n
          }
        ]
      }
    ]
  })
})

test('parseResponse handles partition-level error code', () => {
  // Create a response with a partition-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array
    .appendArray(
      [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              errorCode: 6, // NOT_LEADER_OR_FOLLOWER
              leaderEpoch: 1,
              endOffset: 100n
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topic)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partition)
              .appendInt16(partition.errorCode)
              .appendInt32(partition.leaderEpoch)
              .appendInt64(partition.endOffset)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 23, 4, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        topics: [
          {
            topic: 'test-topic',
            partitions: [
              {
                partition: 0,
                errorCode: 6, // NOT_LEADER_OR_FOLLOWER
                leaderEpoch: 1,
                endOffset: 100n
              }
            ]
          }
        ]
      })

      return true
    }
  )
})

test('parseResponse handles multiple topics and partitions', () => {
  // Create a response with multiple topics and partitions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array
    .appendArray(
      [
        {
          topic: 'topic-1',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              leaderEpoch: 1,
              endOffset: 100n
            },
            {
              partition: 1,
              errorCode: 0,
              leaderEpoch: 2,
              endOffset: 200n
            }
          ]
        },
        {
          topic: 'topic-2',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              leaderEpoch: 3,
              endOffset: 300n
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topic)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partition)
              .appendInt16(partition.errorCode)
              .appendInt32(partition.leaderEpoch)
              .appendInt64(partition.endOffset)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 23, 4, Reader.from(writer))

  // Verify the response structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    topics: [
      {
        topic: 'topic-1',
        partitions: [
          {
            partition: 0,
            errorCode: 0,
            leaderEpoch: 1,
            endOffset: 100n
          },
          {
            partition: 1,
            errorCode: 0,
            leaderEpoch: 2,
            endOffset: 200n
          }
        ]
      },
      {
        topic: 'topic-2',
        partitions: [
          {
            partition: 0,
            errorCode: 0,
            leaderEpoch: 3,
            endOffset: 300n
          }
        ]
      }
    ]
  })
})

test('parseResponse handles multiple partition errors', () => {
  // Create a response with multiple partition-level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array
    .appendArray(
      [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              errorCode: 6, // NOT_LEADER_OR_FOLLOWER
              leaderEpoch: 1,
              endOffset: 100n
            },
            {
              partition: 1,
              errorCode: 9, // REPLICA_NOT_AVAILABLE
              leaderEpoch: 2,
              endOffset: 200n
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topic)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partition)
              .appendInt16(partition.errorCode)
              .appendInt32(partition.leaderEpoch)
              .appendInt64(partition.endOffset)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 23, 4, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved with both errors
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        topics: [
          {
            topic: 'test-topic',
            partitions: [
              {
                partition: 0,
                errorCode: 6,
                leaderEpoch: 1,
                endOffset: 100n
              },
              {
                partition: 1,
                errorCode: 9,
                leaderEpoch: 2,
                endOffset: 200n
              }
            ]
          }
        ]
      })

      return true
    }
  )
})

test('parseResponse handles empty topics array', () => {
  // Create a response with empty topics
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Empty topics array
    .appendArray([], () => {})
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 23, 4, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    topics: []
  })
})

test('parseResponse handles throttleTimeMs value', () => {
  // Create a response with throttleTimeMs set
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs = 100ms
    // Topics array
    .appendArray(
      [
        {
          topic: 'test-topic',
          partitions: [
            {
              partition: 0,
              errorCode: 0,
              leaderEpoch: 1,
              endOffset: 100n
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topic).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partition)
            .appendInt16(partition.errorCode)
            .appendInt32(partition.leaderEpoch)
            .appendInt64(partition.endOffset)
        })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 23, 4, Reader.from(writer))

  // Verify throttleTimeMs is correctly parsed
  deepStrictEqual(response.throttleTimeMs, 100)
})
