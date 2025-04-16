import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { listOffsetsV9, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = listOffsetsV9

test('createRequest serializes basic parameters correctly', () => {
  const replica = -1 // Consumer request
  const isolationLevel = 0 // READ_UNCOMMITTED
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 0,
          timestamp: -1n // Latest offset
        }
      ]
    }
  ]

  const writer = createRequest(replica, isolationLevel, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read basic parameters
  const basicParams = {
    replica: reader.readInt32(),
    isolationLevel: reader.readInt8()
  }

  // Verify basic parameters are correctly serialized
  deepStrictEqual(basicParams, {
    replica,
    isolationLevel
  })

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicName = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const currentLeaderEpoch = reader.readInt32()
      const timestamp = reader.readInt64()

      return {
        partitionIndex,
        currentLeaderEpoch,
        timestamp
      }
    })

    return { topicName, partitions }
  })

  // Verify the topics and partitions
  deepStrictEqual(topicsArray, [
    {
      topicName: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 0,
          timestamp: -1n
        }
      ]
    }
  ])

  // Verify tagged fields
  deepStrictEqual(reader.readUnsignedVarInt(), 0, 'Should have 0 tagged fields')
})

test('createRequest with earliest timestamp', () => {
  const replica = -1 // Consumer request
  const isolationLevel = 0 // READ_UNCOMMITTED
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 0,
          timestamp: -2n // Earliest offset
        }
      ]
    }
  ]

  const writer = createRequest(replica, isolationLevel, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read basic parameters
  const basicParams = {
    replica: reader.readInt32(),
    isolationLevel: reader.readInt8()
  }

  // Verify basic parameters are correctly serialized
  deepStrictEqual(basicParams, {
    replica,
    isolationLevel
  })

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicName = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const currentLeaderEpoch = reader.readInt32()
      const timestamp = reader.readInt64()

      return {
        partitionIndex,
        currentLeaderEpoch,
        timestamp
      }
    })

    return { topicName, partitions }
  })

  // Verify topics array length and structure
  deepStrictEqual(topicsArray.length, 1)
  deepStrictEqual(topicsArray[0].topicName, 'test-topic')
  deepStrictEqual(topicsArray[0].partitions.length, 1)

  // Verify timestamp is -2 (earliest)
  deepStrictEqual(topicsArray[0].partitions[0].partitionIndex, 0)
  deepStrictEqual(topicsArray[0].partitions[0].currentLeaderEpoch, 0)
  deepStrictEqual(topicsArray[0].partitions[0].timestamp, -2n)

  // Verify tags field
  deepStrictEqual(reader.readUnsignedVarInt(), 0)
})

test('createRequest with specific timestamp', () => {
  const replica = -1 // Consumer request
  const isolationLevel = 0 // READ_UNCOMMITTED
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 0,
          timestamp: 1617234567890n // Specific timestamp
        }
      ]
    }
  ]

  const writer = createRequest(replica, isolationLevel, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read basic parameters
  const basicParams = {
    replica: reader.readInt32(),
    isolationLevel: reader.readInt8()
  }

  // Verify basic parameters are correctly serialized
  deepStrictEqual(basicParams, {
    replica,
    isolationLevel
  })

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicName = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const currentLeaderEpoch = reader.readInt32()
      const timestamp = reader.readInt64()

      return {
        partitionIndex,
        currentLeaderEpoch,
        timestamp
      }
    })

    return { topicName, partitions }
  })

  // Verify topics array structure
  deepStrictEqual(topicsArray.length, 1)
  deepStrictEqual(topicsArray[0].topicName, 'test-topic')
  deepStrictEqual(topicsArray[0].partitions.length, 1)

  // Verify partition data including specific timestamp
  const partition = topicsArray[0].partitions[0]
  deepStrictEqual(partition.partitionIndex, 0)
  deepStrictEqual(partition.currentLeaderEpoch, 0)
  deepStrictEqual(partition.timestamp, 1617234567890n)

  // Verify tags field
  deepStrictEqual(reader.readUnsignedVarInt(), 0)
})

test('createRequest with multiple topics and partitions', () => {
  const replica = -1 // Consumer request
  const isolationLevel = 0 // READ_UNCOMMITTED
  const topics = [
    {
      name: 'topic-1',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 0,
          timestamp: -1n
        },
        {
          partitionIndex: 1,
          currentLeaderEpoch: 0,
          timestamp: -1n
        }
      ]
    },
    {
      name: 'topic-2',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 0,
          timestamp: -1n
        }
      ]
    }
  ]

  const writer = createRequest(replica, isolationLevel, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read basic parameters
  const basicParams = {
    replica: reader.readInt32(),
    isolationLevel: reader.readInt8()
  }

  // Verify basic parameters
  deepStrictEqual(
    basicParams,
    {
      replica,
      isolationLevel
    },
    'Basic parameters should match'
  )

  // Read topics array
  const topicsRead = reader.readArray(() => {
    const topicName = reader.readString()

    // Read full partitions data
    const partitions = reader.readArray(() => {
      return {
        partitionIndex: reader.readInt32(),
        currentLeaderEpoch: reader.readInt32(),
        timestamp: reader.readInt64()
      }
    })

    return { topicName, partitions }
  })

  // Verify the topics array contains the correct number of topics
  deepStrictEqual(topicsRead.length, topics.length, 'Should have the correct number of topics')

  // Verify each topic has the right name
  deepStrictEqual(topicsRead[0].topicName, 'topic-1', 'First topic should be topic-1')
  deepStrictEqual(topicsRead[1].topicName, 'topic-2', 'Second topic should be topic-2')

  // Verify each topic has the right number of partitions
  deepStrictEqual(
    topicsRead[0].partitions.length,
    topics[0].partitions.length,
    'First topic should have correct number of partitions'
  )
  deepStrictEqual(
    topicsRead[1].partitions.length,
    topics[1].partitions.length,
    'Second topic should have correct number of partitions'
  )

  // Verify the first topic's first partition details
  const firstPartition = topicsRead[0].partitions[0]
  deepStrictEqual(
    {
      partitionIndex: firstPartition.partitionIndex,
      currentLeaderEpoch: firstPartition.currentLeaderEpoch,
      timestamp: firstPartition.timestamp
    },
    {
      partitionIndex: 0,
      currentLeaderEpoch: 0,
      timestamp: -1n
    },
    'First partition details should match'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array - using tagged fields format
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              timestamp: 1617234567890n,
              offset: 100n,
              leaderEpoch: 5
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.timestamp)
              .appendInt64(partition.offset)
              .appendInt32(partition.leaderEpoch)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 2, 9, writer.bufferList)

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    topics: [
      {
        name: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            timestamp: 1617234567890n,
            offset: 100n,
            leaderEpoch: 5
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
    // Topics array - using tagged fields format
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 1, // OFFSET_OUT_OF_RANGE
              timestamp: 0n, // not valid with error
              offset: -1n, // not valid with error
              leaderEpoch: -1 // not valid with error
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.timestamp)
              .appendInt64(partition.offset)
              .appendInt32(partition.leaderEpoch)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 2, 9, writer.bufferList)
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists and has the correct type
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        topics: [
          {
            name: 'test-topic',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 1,
                timestamp: 0n,
                offset: -1n,
                leaderEpoch: -1
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
    // Topics array - using tagged fields format
    .appendArray(
      [
        {
          name: 'topic-1',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              timestamp: 1617234567890n,
              offset: 100n,
              leaderEpoch: 5
            },
            {
              partitionIndex: 1,
              errorCode: 0,
              timestamp: 1617234567890n,
              offset: 200n,
              leaderEpoch: 5
            }
          ]
        },
        {
          name: 'topic-2',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              timestamp: 1617234567890n,
              offset: 300n,
              leaderEpoch: 5
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.timestamp)
              .appendInt64(partition.offset)
              .appendInt32(partition.leaderEpoch)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 2, 9, writer.bufferList)

  // Verify the response structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    topics: [
      {
        name: 'topic-1',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            timestamp: 1617234567890n,
            offset: 100n,
            leaderEpoch: 5
          },
          {
            partitionIndex: 1,
            errorCode: 0,
            timestamp: 1617234567890n,
            offset: 200n,
            leaderEpoch: 5
          }
        ]
      },
      {
        name: 'topic-2',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            timestamp: 1617234567890n,
            offset: 300n,
            leaderEpoch: 5
          }
        ]
      }
    ]
  })
})
