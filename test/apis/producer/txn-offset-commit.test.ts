import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, txnOffsetCommitV4, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = txnOffsetCommitV4

test('createRequest serializes parameters correctly', () => {
  const transactionalId = 'transaction-123'
  const groupId = 'consumer-group-1'
  const producerId = 1234567890n
  const producerEpoch = 5
  const generationId = 42
  const memberId = 'member-1'
  const groupInstanceId = 'instance-1'
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          committedOffset: 100n,
          committedLeaderEpoch: 15,
          committedMetadata: 'metadata-1'
        },
        {
          partitionIndex: 1,
          committedOffset: 200n,
          committedLeaderEpoch: 15,
          committedMetadata: null
        }
      ]
    }
  ]

  const writer = createRequest(
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    generationId,
    memberId,
    groupInstanceId,
    topics
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Create a complete structure of the serialized request
  const serializedRequest = {
    header: {
      transactionalId: reader.readString(),
      groupId: reader.readString(),
      producerId: reader.readInt64(),
      producerEpoch: reader.readInt16(),
      generationId: reader.readInt32(),
      memberId: reader.readString(),
      groupInstanceId: reader.readString()
    },
    topics: reader.readArray(() => {
      const topicName = reader.readString()

      // Read partitions array
      const partitions = reader.readArray(() => {
        const partitionIndex = reader.readInt32()
        const committedOffset = reader.readInt64()
        const committedLeaderEpoch = reader.readInt32()
        const committedMetadata = reader.readNullableString()

        // Include tagged fields for partition
        const partitionTagsCount = reader.readUnsignedVarInt()

        return {
          partitionIndex,
          committedOffset,
          committedLeaderEpoch,
          committedMetadata,
          partitionTagsCount
        }
      })

      // Include tagged fields for topic
      const topicTagsCount = reader.readUnsignedVarInt()

      return { topicName, partitions, topicTagsCount }
    })
  }

  // Verify the complete serialized structure with one deepStrictEqual
  deepStrictEqual(
    serializedRequest,
    {
      header: {
        transactionalId,
        groupId,
        producerId,
        producerEpoch,
        generationId,
        memberId,
        groupInstanceId
      },
      topics: [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              committedOffset: 100n,
              committedLeaderEpoch: 15,
              committedMetadata: 'metadata-1',
              partitionTagsCount: 0
            },
            {
              partitionIndex: 1,
              committedOffset: 200n,
              committedLeaderEpoch: 15,
              committedMetadata: null,
              partitionTagsCount: 0
            }
          ],
          topicTagsCount: 0
        }
      ]
    },
    'Serialized request should match expected structure'
  )
})

test('createRequest handles null group instance ID', () => {
  const transactionalId = 'transaction-123'
  const groupId = 'consumer-group-1'
  const producerId = 1234567890n
  const producerEpoch = 5
  const generationId = 42
  const memberId = 'member-1'
  const groupInstanceId = null // Null group instance ID
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          committedOffset: 100n,
          committedLeaderEpoch: 15
        }
      ]
    }
  ]

  const writer = createRequest(
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    generationId,
    memberId,
    groupInstanceId,
    topics
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Create a complete structure of the serialized request
  const serializedRequest = {
    header: {
      transactionalId: reader.readString(),
      groupId: reader.readString(),
      producerId: reader.readInt64(),
      producerEpoch: reader.readInt16(),
      generationId: reader.readInt32(),
      memberId: reader.readString(),
      groupInstanceId: reader.readNullableString()
    },
    topics: reader.readArray(() => {
      const topicName = reader.readString()

      // Read partitions array
      const partitions = reader.readArray(() => {
        const partitionIndex = reader.readInt32()
        const committedOffset = reader.readInt64()
        const committedLeaderEpoch = reader.readInt32()
        const committedMetadata = reader.readNullableString()

        // Include tagged fields for partition
        const partitionTagsCount = reader.readUnsignedVarInt()

        return {
          partitionIndex,
          committedOffset,
          committedLeaderEpoch,
          committedMetadata,
          partitionTagsCount
        }
      })

      // Include tagged fields for topic
      const topicTagsCount = reader.readUnsignedVarInt()

      return { topicName, partitions, topicTagsCount }
    })
  }

  // Verify the complete serialized structure with one deepStrictEqual
  deepStrictEqual(
    serializedRequest,
    {
      header: {
        transactionalId,
        groupId,
        producerId,
        producerEpoch,
        generationId,
        memberId,
        groupInstanceId
      },
      topics: [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              committedOffset: 100n,
              committedLeaderEpoch: 15,
              committedMetadata: null,
              partitionTagsCount: 0
            }
          ],
          topicTagsCount: 0
        }
      ]
    },
    'Serialized request with null group instance ID should match expected structure'
  )
})

test('createRequest handles multiple topics', () => {
  const transactionalId = 'transaction-123'
  const groupId = 'consumer-group-1'
  const producerId = 1234567890n
  const producerEpoch = 5
  const generationId = 42
  const memberId = 'member-1'
  const groupInstanceId = null
  const topics = [
    {
      name: 'topic-1',
      partitions: [
        {
          partitionIndex: 0,
          committedOffset: 100n,
          committedLeaderEpoch: 15
        }
      ]
    },
    {
      name: 'topic-2',
      partitions: [
        {
          partitionIndex: 0,
          committedOffset: 200n,
          committedLeaderEpoch: 20
        },
        {
          partitionIndex: 1,
          committedOffset: 300n,
          committedLeaderEpoch: 20
        }
      ]
    }
  ]

  const writer = createRequest(
    transactionalId,
    groupId,
    producerId,
    producerEpoch,
    generationId,
    memberId,
    groupInstanceId,
    topics
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Create a complete structure of the serialized request
  const serializedRequest = {
    header: {
      transactionalId: reader.readString(),
      groupId: reader.readString(),
      producerId: reader.readInt64(),
      producerEpoch: reader.readInt16(),
      generationId: reader.readInt32(),
      memberId: reader.readString(),
      groupInstanceId: reader.readNullableString()
    },
    topics: reader.readArray(() => {
      const topicName = reader.readString()

      // Read partitions array
      const partitions = reader.readArray(() => {
        const partitionIndex = reader.readInt32()
        const committedOffset = reader.readInt64()
        const committedLeaderEpoch = reader.readInt32()
        const committedMetadata = reader.readNullableString()

        // Include tagged fields for partition
        const partitionTagsCount = reader.readUnsignedVarInt()

        return {
          partitionIndex,
          committedOffset,
          committedLeaderEpoch,
          committedMetadata,
          partitionTagsCount
        }
      })

      // Include tagged fields for topic
      const topicTagsCount = reader.readUnsignedVarInt()

      return { topicName, partitions, topicTagsCount }
    })
  }

  // Verify the complete serialized structure with one deepStrictEqual
  deepStrictEqual(
    serializedRequest,
    {
      header: {
        transactionalId,
        groupId,
        producerId,
        producerEpoch,
        generationId,
        memberId,
        groupInstanceId
      },
      topics: [
        {
          topicName: 'topic-1',
          partitions: [
            {
              partitionIndex: 0,
              committedOffset: 100n,
              committedLeaderEpoch: 15,
              committedMetadata: null,
              partitionTagsCount: 0
            }
          ],
          topicTagsCount: 0
        },
        {
          topicName: 'topic-2',
          partitions: [
            {
              partitionIndex: 0,
              committedOffset: 200n,
              committedLeaderEpoch: 20,
              committedMetadata: null,
              partitionTagsCount: 0
            },
            {
              partitionIndex: 1,
              committedOffset: 300n,
              committedLeaderEpoch: 20,
              committedMetadata: null,
              partitionTagsCount: 0
            }
          ],
          topicTagsCount: 0
        }
      ]
    },
    'Serialized request with multiple topics should match expected structure'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array - compact format
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0 // success
            },
            {
              partitionIndex: 1,
              errorCode: 0 // success
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

  const response = parseResponse(1, 28, 4, Reader.from(writer))

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
          },
          {
            partitionIndex: 1,
            errorCode: 0
          }
        ]
      }
    ]
  })
})

test('parseResponse handles multiple topics', () => {
  // Create a response with multiple topics
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array - compact format
    .appendArray(
      [
        {
          name: 'topic-1',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0 // success
            }
          ]
        },
        {
          name: 'topic-2',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0 // success
            },
            {
              partitionIndex: 1,
              errorCode: 0 // success
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

  const response = parseResponse(1, 28, 4, Reader.from(writer))

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
          }
        ]
      },
      {
        name: 'topic-2',
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
      }
    ]
  })
})

test('parseResponse throws error on partition error code', () => {
  // Create a response with a partition error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Topics array - compact format
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 22 // INVALID_COMMIT_OFFSET_SIZE
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
      parseResponse(1, 28, 4, Reader.from(writer))
    },
    (err: any) => {
      // Verify error is the right type
      ok(err instanceof ResponseError, 'Should be a ResponseError')
      ok(
        err.message.includes('Received response with error while executing API'),
        'Error message should mention API execution error'
      )

      // Check that errors object exists
      ok(err.errors !== undefined && typeof err.errors === 'object', 'Errors object should exist')

      // Verify the response structure is preserved
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
