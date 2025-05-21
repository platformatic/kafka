import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { offsetCommitV9, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = offsetCommitV9

test('createRequest serializes basic parameters correctly', () => {
  const groupId = 'test-group'
  const generationIdOrMemberEpoch = 1
  const memberId = 'test-member'
  const groupInstanceId = null
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          committedOffset: 100n,
          committedLeaderEpoch: 0,
          committedMetadata: null
        }
      ]
    }
  ]

  const writer = createRequest(groupId, generationIdOrMemberEpoch, memberId, groupInstanceId, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read and store basic parameters
  const basicParams = {
    groupId: reader.readString(),
    generationIdOrMemberEpoch: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readNullableString()
  }

  // Verify basic parameters match expected values
  deepStrictEqual(basicParams, {
    groupId,
    generationIdOrMemberEpoch,
    memberId,
    groupInstanceId
  })

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicName = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const committedOffset = reader.readInt64()
      const committedLeaderEpoch = reader.readInt32()
      const committedMetadata = reader.readNullableString()

      return {
        partitionIndex,
        committedOffset,
        committedLeaderEpoch,
        committedMetadata
      }
    })

    return { topicName, partitions }
  })

  // Verify topics and partitions
  deepStrictEqual(topicsArray, [
    {
      topicName: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          committedOffset: 100n,
          committedLeaderEpoch: 0,
          committedMetadata: null
        }
      ]
    }
  ])

  // Verify tagged fields
  deepStrictEqual(reader.readUnsignedVarInt(), 0, 'Should have 0 tagged fields')
})

test('createRequest with committed metadata', () => {
  const groupId = 'test-group'
  const generationIdOrMemberEpoch = 1
  const memberId = 'test-member'
  const groupInstanceId = null
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          committedOffset: 100n,
          committedLeaderEpoch: 0,
          committedMetadata: 'test-metadata'
        }
      ]
    }
  ]

  const writer = createRequest(groupId, generationIdOrMemberEpoch, memberId, groupInstanceId, topics)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all basic parameters
  const basicParams = {
    groupId: reader.readString(),
    generationIdOrMemberEpoch: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readNullableString()
  }

  // Verify basic parameters match expected values
  deepStrictEqual(basicParams, {
    groupId,
    generationIdOrMemberEpoch,
    memberId,
    groupInstanceId
  })

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicName = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const committedOffset = reader.readInt64()
      const committedLeaderEpoch = reader.readInt32()
      const committedMetadata = reader.readString()

      return {
        partitionIndex,
        committedOffset,
        committedLeaderEpoch,
        committedMetadata
      }
    })

    return { topicName, partitions }
  })

  // Verify topic name and length
  deepStrictEqual(topicsArray.length, 1)
  deepStrictEqual(topicsArray[0].topicName, 'test-topic')

  // Verify partitions length
  deepStrictEqual(topicsArray[0].partitions.length, 1)

  // Verify committed metadata and other values
  deepStrictEqual(topicsArray[0].partitions[0], {
    partitionIndex: 0,
    committedOffset: 100n,
    committedLeaderEpoch: 0,
    committedMetadata: 'test-metadata'
  })

  // Verify tags count
  deepStrictEqual(reader.readUnsignedVarInt(), 0)
})

test('createRequest with group instance ID', () => {
  const groupId = 'test-group'
  const generationIdOrMemberEpoch = 1
  const memberId = 'test-member'
  const groupInstanceId = 'test-instance'
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          committedOffset: 100n,
          committedLeaderEpoch: 0,
          committedMetadata: null
        }
      ]
    }
  ]

  const writer = createRequest(groupId, generationIdOrMemberEpoch, memberId, groupInstanceId, topics)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all parameters
  const serializedData = {
    groupId: reader.readString(),
    generationIdOrMemberEpoch: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readString()
  }

  // Verify all parameters including group instance ID
  deepStrictEqual(serializedData, {
    groupId,
    generationIdOrMemberEpoch,
    memberId,
    groupInstanceId
  })

  // Verify the topics array exists
  const topicsCount = reader.readUnsignedVarInt()
  ok(topicsCount > 0, 'Should have at least one topic')
})

test('createRequest with multiple topics and partitions', () => {
  const groupId = 'test-group'
  const generationIdOrMemberEpoch = 1
  const memberId = 'test-member'
  const groupInstanceId = null
  const topics = [
    {
      name: 'topic-1',
      partitions: [
        {
          partitionIndex: 0,
          committedOffset: 100n,
          committedLeaderEpoch: 0,
          committedMetadata: null
        },
        {
          partitionIndex: 1,
          committedOffset: 200n,
          committedLeaderEpoch: 0,
          committedMetadata: null
        }
      ]
    },
    {
      name: 'topic-2',
      partitions: [
        {
          partitionIndex: 0,
          committedOffset: 300n,
          committedLeaderEpoch: 0,
          committedMetadata: null
        }
      ]
    }
  ]

  const writer = createRequest(groupId, generationIdOrMemberEpoch, memberId, groupInstanceId, topics)

  // Verify it's a Writer instance
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read basic parameters
  const basicParams = {
    groupId: reader.readString(),
    generationIdOrMemberEpoch: reader.readInt32(),
    memberId: reader.readString(),
    groupInstanceId: reader.readNullableString()
  }

  // Verify basic parameters match expected values
  deepStrictEqual(
    basicParams,
    {
      groupId,
      generationIdOrMemberEpoch,
      memberId,
      groupInstanceId
    },
    'Basic parameters should match'
  )

  // Read topics array
  const topicsRead = reader.readArray(() => {
    const topicName = reader.readString()

    // Read and collect all partition data
    const partitions = reader.readArray(() => {
      return {
        partitionIndex: reader.readInt32(),
        committedOffset: reader.readInt64(),
        committedLeaderEpoch: reader.readInt32(),
        committedMetadata: reader.readNullableString()
      }
    })

    return { topicName, partitions }
  })

  // Verify we have the right number of topics
  deepStrictEqual(topicsRead.length, topics.length, 'Should have the correct number of topics')

  // Verify topic names and partition counts with separate assertions
  deepStrictEqual(topicsRead[0].topicName, 'topic-1', 'First topic should be topic-1')
  deepStrictEqual(
    topicsRead[0].partitions.length,
    topics[0].partitions.length,
    'First topic should have correct number of partitions'
  )

  deepStrictEqual(topicsRead[1].topicName, 'topic-2', 'Second topic should be topic-2')
  deepStrictEqual(
    topicsRead[1].partitions.length,
    topics[1].partitions.length,
    'Second topic should have correct number of partitions'
  )

  // Verify first partition data from the first topic
  const firstPartition = topicsRead[0].partitions[0]
  deepStrictEqual(
    {
      partitionIndex: firstPartition.partitionIndex,
      committedOffset: firstPartition.committedOffset,
      committedLeaderEpoch: firstPartition.committedLeaderEpoch
    },
    {
      partitionIndex: 0,
      committedOffset: 100n,
      committedLeaderEpoch: 0
    },
    'First partition data should match'
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
              errorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 8, 9, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
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
              errorCode: 22 // errorCode (INVALID_COMMIT_OFFSET_SIZE)
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 8, 9, Reader.from(writer))
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
            name: 'test-topic',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 22
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
              errorCode: 0
            },
            {
              partitionIndex: 1,
              errorCode: 0
            }
          ]
        },
        {
          name: 'topic-2',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 8, 9, Reader.from(writer))

  // Verify the response structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    topics: [
      {
        name: 'topic-1',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0
          },
          {
            partitionIndex: 1,
            errorCode: 0
          }
        ]
      },
      {
        name: 'topic-2',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0
          }
        ]
      }
    ]
  })
})
