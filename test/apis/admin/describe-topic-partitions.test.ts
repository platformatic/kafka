import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, describeTopicPartitionsV0 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeTopicPartitionsV0

test('createRequest serializes basic parameters correctly', () => {
  const topics = [
    {
      name: 'test-topic'
    }
  ]
  const responsePartitionLimit = 100
  const cursor = undefined

  const writer = createRequest(topics, responsePartitionLimit, cursor)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    return { name }
  })

  // Read responsePartitionLimit
  const limit = reader.readInt32()

  // Read cursor flag (should be -1 for no cursor)
  const cursorFlag = reader.readInt8()

  // Verify serialized data
  deepStrictEqual(
    {
      topics: topicsArray,
      responsePartitionLimit: limit,
      cursorFlag
    },
    {
      topics: [
        {
          name: 'test-topic'
        }
      ],
      responsePartitionLimit: 100,
      cursorFlag: -1
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple topics correctly', () => {
  const topics = [
    {
      name: 'topic-1'
    },
    {
      name: 'topic-2'
    },
    {
      name: 'topic-3'
    }
  ]
  const responsePartitionLimit = 100
  const cursor = undefined

  const writer = createRequest(topics, responsePartitionLimit, cursor)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    return { name }
  })

  // Skip rest of data
  reader.readInt32() // responsePartitionLimit
  reader.readInt8() // cursor flag

  // Verify multiple topics
  deepStrictEqual(
    topicsArray,
    [
      {
        name: 'topic-1'
      },
      {
        name: 'topic-2'
      },
      {
        name: 'topic-3'
      }
    ],
    'Multiple topics should be serialized correctly'
  )
})

test('createRequest serializes cursor correctly', () => {
  const topics = [
    {
      name: 'test-topic'
    }
  ]
  const responsePartitionLimit = 100
  const cursor = {
    topicName: 'cursor-topic',
    partitionIndex: 5
  }

  const writer = createRequest(topics, responsePartitionLimit, cursor)
  const reader = Reader.from(writer)

  // Skip topics array
  reader.readArray(() => {
    reader.readString()
    return {}
  })

  // Skip responsePartitionLimit
  reader.readInt32()

  // Read cursor flag (should be 1 for cursor present)
  const cursorFlag = reader.readInt8()

  // Read cursor data if present
  let cursorData = null
  if (cursorFlag === 1) {
    cursorData = {
      topicName: reader.readString(),
      partitionIndex: reader.readInt32()
    }
  }

  // Verify cursor is serialized correctly
  deepStrictEqual(cursorFlag, 1, 'Cursor flag should be set to 1 when cursor is provided')
  deepStrictEqual(
    cursorData,
    {
      topicName: 'cursor-topic',
      partitionIndex: 5
    },
    'Cursor data should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no topics
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}) // Empty topics array
    .appendInt8(-1) // No next cursor
    .appendTaggedFields()

  const response = parseResponse(1, 75, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      topics: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a response with topic partitions', () => {
  const topicId = '12345678-1234-1234-1234-123456789abc'

  // Create a successful response with topic partitions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          name: 'test-topic',
          topicId,
          isInternal: false,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leaderId: 1,
              leaderEpoch: 5,
              replicaNodes: [1, 2, 3],
              isrNodes: [1, 2, 3],
              eligibleLeaderReplicas: [1, 2],
              lastKnownElr: [1],
              offlineReplicas: []
            }
          ],
          topicAuthorizedOperations: 0
        }
      ],
      (w, topic) => {
        w.appendInt16(topic.errorCode)
          .appendString(topic.name)
          .appendUUID(topic.topicId)
          .appendBoolean(topic.isInternal)
          .appendArray(topic.partitions, (w, p) => {
            w.appendInt16(p.errorCode)
              .appendInt32(p.partitionIndex)
              .appendInt32(p.leaderId)
              .appendInt32(p.leaderEpoch)
              .appendArray(p.replicaNodes, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.isrNodes, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.eligibleLeaderReplicas, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.lastKnownElr, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.offlineReplicas, (w, n) => w.appendInt32(n), true, false)
          })
          .appendInt32(topic.topicAuthorizedOperations)
      }
    )
    .appendInt8(-1) // No next cursor
    .appendTaggedFields()

  const response = parseResponse(1, 75, 0, Reader.from(writer))

  // Verify topic structure
  deepStrictEqual(response.topics.length, 1, 'Should have one topic')
  deepStrictEqual(response.topics[0].name, 'test-topic', 'Topic name should be parsed correctly')
  deepStrictEqual(response.topics[0].topicId, topicId, 'Topic ID should be parsed correctly')
  deepStrictEqual(response.topics[0].isInternal, false, 'isInternal flag should be parsed correctly')

  // Verify partition structure
  const partition = response.topics[0].partitions[0]
  deepStrictEqual(partition.partitionIndex, 0, 'Partition index should be parsed correctly')
  deepStrictEqual(partition.leaderId, 1, 'Leader ID should be parsed correctly')
  deepStrictEqual(partition.leaderEpoch, 5, 'Leader epoch should be parsed correctly')
  deepStrictEqual(partition.replicaNodes, [1, 2, 3], 'Replica nodes should be parsed correctly')
  deepStrictEqual(partition.isrNodes, [1, 2, 3], 'ISR nodes should be parsed correctly')
  deepStrictEqual(partition.eligibleLeaderReplicas, [1, 2], 'Eligible leader replicas should be parsed correctly')
  deepStrictEqual(partition.lastKnownElr, [1], 'Last known ELR should be parsed correctly')
  deepStrictEqual(partition.offlineReplicas, [], 'Offline replicas should be parsed correctly')
})

test('parseResponse correctly processes a response with multiple topics and partitions', () => {
  const topicId1 = '12345678-1234-1234-1234-123456789abc'
  const topicId2 = '87654321-4321-4321-4321-cba987654321'

  // Create a response with multiple topics and partitions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          name: 'topic-1',
          topicId: topicId1,
          isInternal: false,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leaderId: 1,
              leaderEpoch: 5,
              replicaNodes: [1, 2, 3],
              isrNodes: [1, 2, 3],
              eligibleLeaderReplicas: [1, 2],
              lastKnownElr: [1],
              offlineReplicas: []
            }
          ],
          topicAuthorizedOperations: 0
        },
        {
          errorCode: 0,
          name: 'topic-2',
          topicId: topicId2,
          isInternal: true,
          partitions: [
            {
              errorCode: 0,
              partitionIndex: 0,
              leaderId: 2,
              leaderEpoch: 3,
              replicaNodes: [1, 2],
              isrNodes: [1, 2],
              eligibleLeaderReplicas: [1, 2],
              lastKnownElr: [2],
              offlineReplicas: []
            },
            {
              errorCode: 0,
              partitionIndex: 1,
              leaderId: 1,
              leaderEpoch: 3,
              replicaNodes: [1, 2],
              isrNodes: [1, 2],
              eligibleLeaderReplicas: [1, 2],
              lastKnownElr: [1],
              offlineReplicas: []
            }
          ],
          topicAuthorizedOperations: 0
        }
      ],
      (w, topic) => {
        w.appendInt16(topic.errorCode)
          .appendString(topic.name)
          .appendUUID(topic.topicId)
          .appendBoolean(topic.isInternal)
          .appendArray(topic.partitions, (w, p) => {
            w.appendInt16(p.errorCode)
              .appendInt32(p.partitionIndex)
              .appendInt32(p.leaderId)
              .appendInt32(p.leaderEpoch)
              .appendArray(p.replicaNodes, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.isrNodes, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.eligibleLeaderReplicas, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.lastKnownElr, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.offlineReplicas, (w, n) => w.appendInt32(n), true, false)
          })
          .appendInt32(topic.topicAuthorizedOperations)
      }
    )
    .appendInt8(-1) // No next cursor
    .appendTaggedFields()

  const response = parseResponse(1, 75, 0, Reader.from(writer))

  // Verify multiple topics
  deepStrictEqual(response.topics.length, 2, 'Should have two topics')

  // Verify topic details
  deepStrictEqual(response.topics[0].name, 'topic-1', 'First topic name should be parsed correctly')
  deepStrictEqual(response.topics[1].name, 'topic-2', 'Second topic name should be parsed correctly')
  deepStrictEqual(response.topics[1].isInternal, true, 'Second topic isInternal flag should be parsed correctly')

  // Verify partition counts
  deepStrictEqual(response.topics[0].partitions.length, 1, 'First topic should have 1 partition')
  deepStrictEqual(response.topics[1].partitions.length, 2, 'Second topic should have 2 partitions')
})

test('parseResponse correctly processes a response with next cursor', () => {
  // Create a response with a next cursor
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}) // Empty topics array
    .appendInt8(1) // Next cursor present
    .appendString('next-topic') // cursor topic name
    .appendInt32(3) // cursor partition index
    .appendTaggedFields()

  const response = parseResponse(1, 75, 0, Reader.from(writer))

  // Verify next cursor
  deepStrictEqual(
    response.nextCursor,
    {
      topicName: 'next-topic',
      partitionIndex: 3
    },
    'Next cursor should be parsed correctly'
  )
})

test('parseResponse throws ResponseError on topic error', () => {
  // Create a response with a topic-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 39, // INVALID_TOPIC_EXCEPTION
          name: 'bad-topic',
          topicId: '00000000-0000-0000-0000-000000000000',
          isInternal: false,
          partitions: [],
          topicAuthorizedOperations: 0
        }
      ],
      (w, topic) => {
        w.appendInt16(topic.errorCode)
          .appendString(topic.name)
          .appendUUID(topic.topicId)
          .appendBoolean(topic.isInternal)
          .appendArray([], () => {}) // Empty partitions
          .appendInt32(topic.topicAuthorizedOperations)
      }
    )
    .appendInt8(-1) // No next cursor
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 75, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify topic error details
      deepStrictEqual(err.response.topics[0].errorCode, 39, 'Topic error code should be preserved in the response')

      deepStrictEqual(err.response.topics[0].name, 'bad-topic', 'Topic name should be preserved in the response')

      return true
    }
  )
})

test('parseResponse throws ResponseError on partition error', () => {
  // Create a response with a partition-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          name: 'test-topic',
          topicId: '12345678-1234-1234-1234-123456789abc',
          isInternal: false,
          partitions: [
            {
              errorCode: 9, // REPLICA_NOT_AVAILABLE
              partitionIndex: 0,
              leaderId: -1,
              leaderEpoch: -1,
              replicaNodes: [1, 2, 3],
              isrNodes: [2, 3],
              eligibleLeaderReplicas: [],
              lastKnownElr: [],
              offlineReplicas: [1]
            }
          ],
          topicAuthorizedOperations: 0
        }
      ],
      (w, topic) => {
        w.appendInt16(topic.errorCode)
          .appendString(topic.name)
          .appendUUID(topic.topicId)
          .appendBoolean(topic.isInternal)
          .appendArray(topic.partitions, (w, p) => {
            w.appendInt16(p.errorCode)
              .appendInt32(p.partitionIndex)
              .appendInt32(p.leaderId)
              .appendInt32(p.leaderEpoch)
              .appendArray(p.replicaNodes, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.isrNodes, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.eligibleLeaderReplicas, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.lastKnownElr, (w, n) => w.appendInt32(n), true, false)
              .appendArray(p.offlineReplicas, (w, n) => w.appendInt32(n), true, false)
          })
          .appendInt32(topic.topicAuthorizedOperations)
      }
    )
    .appendInt8(-1) // No next cursor
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 75, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify partition error details
      deepStrictEqual(
        err.response.topics[0].partitions[0].errorCode,
        9,
        'Partition error code should be preserved in the response'
      )

      // Verify offline replicas
      deepStrictEqual(
        err.response.topics[0].partitions[0].offlineReplicas,
        [1],
        'Offline replicas should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendArray([], () => {}) // Empty topics array
    .appendInt8(-1) // No next cursor
    .appendTaggedFields()

  const response = parseResponse(1, 75, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
