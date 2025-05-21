import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, listPartitionReassignmentsV0 } from '../../../src/index.ts'

const { createRequest, parseResponse } = listPartitionReassignmentsV0

test('createRequest serializes basic parameters correctly', () => {
  const timeoutMs = 30000
  const topics: [] = []

  const writer = createRequest(timeoutMs, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read timeoutMs
  const serializedTimeoutMs = reader.readInt32()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const partitionIndexes = reader.readArray(() => reader.readInt32(), true, false)
    return { name, partitionIndexes }
  })

  // Verify serialized data
  deepStrictEqual(serializedTimeoutMs, 30000, 'Timeout should be serialized correctly')

  deepStrictEqual(topicsArray, [], 'Empty topics array should be serialized correctly')
})

test('createRequest serializes single topic with partitions correctly', () => {
  const timeoutMs = 30000
  const topics = [
    {
      name: 'test-topic',
      partitionIndexes: [0, 1, 2]
    }
  ]

  const writer = createRequest(timeoutMs, topics)
  const reader = Reader.from(writer)

  // Skip timeoutMs
  reader.readInt32()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const partitionIndexes = reader.readArray(() => reader.readInt32(), true, false)
    return { name, partitionIndexes }
  })

  // Verify serialized data
  deepStrictEqual(
    topicsArray,
    [
      {
        name: 'test-topic',
        partitionIndexes: [0, 1, 2]
      }
    ],
    'Single topic with partitions should be serialized correctly'
  )
})

test('createRequest serializes multiple topics correctly', () => {
  const timeoutMs = 30000
  const topics = [
    {
      name: 'topic-1',
      partitionIndexes: [0, 1]
    },
    {
      name: 'topic-2',
      partitionIndexes: [0, 1, 2]
    }
  ]

  const writer = createRequest(timeoutMs, topics)
  const reader = Reader.from(writer)

  // Skip timeoutMs
  reader.readInt32()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const partitionIndexes = reader.readArray(() => reader.readInt32(), true, false)
    return { name, partitionIndexes }
  })

  // Verify multiple topics
  deepStrictEqual(
    topicsArray,
    [
      {
        name: 'topic-1',
        partitionIndexes: [0, 1]
      },
      {
        name: 'topic-2',
        partitionIndexes: [0, 1, 2]
      }
    ],
    'Multiple topics should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no topics
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty topics array
    .appendTaggedFields()

  const response = parseResponse(1, 46, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      errorMessage: null,
      topics: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a response with reassignment information', () => {
  // Create a successful response with reassignment information
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
              replicas: [1, 2, 3],
              addingReplicas: [4],
              removingReplicas: [1]
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendArray(partition.replicas, (w, r) => w.appendInt32(r), true, false)
            .appendArray(partition.addingReplicas, (w, r) => w.appendInt32(r), true, false)
            .appendArray(partition.removingReplicas, (w, r) => w.appendInt32(r), true, false)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 46, 0, Reader.from(writer))

  // Verify topic structure
  deepStrictEqual(response.topics.length, 1, 'Should have one topic')
  deepStrictEqual(response.topics[0].name, 'test-topic', 'Topic name should be parsed correctly')

  // Verify partition structure
  const partition = response.topics[0].partitions[0]
  deepStrictEqual(partition.partitionIndex, 0, 'Partition index should be parsed correctly')
  deepStrictEqual(partition.replicas, [1, 2, 3], 'Replicas should be parsed correctly')
  deepStrictEqual(partition.addingReplicas, [4], 'Adding replicas should be parsed correctly')
  deepStrictEqual(partition.removingReplicas, [1], 'Removing replicas should be parsed correctly')
})

test('parseResponse correctly processes a response with multiple topics and partitions', () => {
  // Create a response with multiple topics and partitions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          name: 'topic-1',
          partitions: [
            {
              partitionIndex: 0,
              replicas: [1, 2, 3],
              addingReplicas: [4],
              removingReplicas: [1]
            }
          ]
        },
        {
          name: 'topic-2',
          partitions: [
            {
              partitionIndex: 0,
              replicas: [2, 3, 4],
              addingReplicas: [5],
              removingReplicas: [2]
            },
            {
              partitionIndex: 1,
              replicas: [3, 4, 5],
              addingReplicas: [1],
              removingReplicas: [3]
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendArray(partition.replicas, (w, r) => w.appendInt32(r), true, false)
            .appendArray(partition.addingReplicas, (w, r) => w.appendInt32(r), true, false)
            .appendArray(partition.removingReplicas, (w, r) => w.appendInt32(r), true, false)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 46, 0, Reader.from(writer))

  // Verify multiple topics
  deepStrictEqual(response.topics.length, 2, 'Should have two topics')

  // Verify topic names
  deepStrictEqual(
    response.topics.map(t => t.name),
    ['topic-1', 'topic-2'],
    'Topic names should be parsed correctly'
  )

  // Verify partition counts
  deepStrictEqual(response.topics[0].partitions.length, 1, 'First topic should have 1 partition')
  deepStrictEqual(response.topics[1].partitions.length, 2, 'Second topic should have 2 partitions')

  // Verify a specific partition detail
  deepStrictEqual(
    response.topics[1].partitions[1].addingReplicas,
    [1],
    'Adding replicas in second topic, second partition should be parsed correctly'
  )
})

test('parseResponse handles empty reassignment arrays correctly', () => {
  // Create a response with empty reassignment arrays
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
              replicas: [1, 2, 3],
              addingReplicas: [],
              removingReplicas: []
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendArray(partition.replicas, (w, r) => w.appendInt32(r), true, false)
            .appendArray(partition.addingReplicas, (w, r) => w.appendInt32(r), true, false)
            .appendArray(partition.removingReplicas, (w, r) => w.appendInt32(r), true, false)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 46, 0, Reader.from(writer))

  // Verify empty arrays
  const partition = response.topics[0].partitions[0]
  deepStrictEqual(partition.addingReplicas, [], 'Empty adding replicas array should be parsed correctly')
  deepStrictEqual(partition.removingReplicas, [], 'Empty removing replicas array should be parsed correctly')
})

test('parseResponse throws ResponseError on error response', () => {
  // Create an error response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(39) // errorCode - INVALID_TOPIC_EXCEPTION
    .appendString('Invalid topic') // errorMessage
    .appendArray([], () => {}) // Empty topics array
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 46, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error details
      deepStrictEqual(err.response.errorCode, 39, 'Error code should be preserved in the response')

      deepStrictEqual(err.response.errorMessage, 'Invalid topic', 'Error message should be preserved in the response')

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty topics array
    .appendTaggedFields()

  const response = parseResponse(1, 46, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
