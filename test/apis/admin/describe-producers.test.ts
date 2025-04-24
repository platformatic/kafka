import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, describeProducersV0 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeProducersV0

test('createRequest serializes empty topics array correctly', () => {
  const topics: [] = []

  const writer = createRequest(topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const partitionIndexes = reader.readArray(() => reader.readInt32(), true, false)
    return { name, partitionIndexes }
  })

  // Verify serialized data
  deepStrictEqual(topicsArray, [], 'Empty topics array should be serialized correctly')
})

test('createRequest serializes single topic with partitions correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      partitionIndexes: [0, 1, 2]
    }
  ]

  const writer = createRequest(topics)
  const reader = Reader.from(writer)

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
  const topics = [
    {
      name: 'topic-1',
      partitionIndexes: [0, 1]
    },
    {
      name: 'topic-2',
      partitionIndexes: [0, 1, 2, 3]
    }
  ]

  const writer = createRequest(topics)
  const reader = Reader.from(writer)

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
        partitionIndexes: [0, 1, 2, 3]
      }
    ],
    'Multiple topics should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no topics
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}) // Empty topics array
    .appendTaggedFields()

  const response = parseResponse(1, 61, 0, Reader.from(writer))

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

test('parseResponse correctly processes a response with active producers', () => {
  // Create a successful response with active producers
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null,
              activeProducers: [
                {
                  producerId: BigInt(1000),
                  producerEpoch: 5,
                  lastSequence: 100,
                  lastTimestamp: BigInt(1630000000000),
                  coordinatorEpoch: 3,
                  currentTxnStartOffset: BigInt(-1)
                }
              ]
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendString(partition.errorMessage)
            .appendArray(partition.activeProducers, (w, producer) => {
              w.appendInt64(producer.producerId)
                .appendInt32(producer.producerEpoch)
                .appendInt32(producer.lastSequence)
                .appendInt64(producer.lastTimestamp)
                .appendInt32(producer.coordinatorEpoch)
                .appendInt64(producer.currentTxnStartOffset)
            })
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 61, 0, Reader.from(writer))

  // Verify producers structure
  deepStrictEqual(response.topics.length, 1, 'Should have one topic')
  deepStrictEqual(response.topics[0].name, 'test-topic', 'Topic name should be parsed correctly')
  deepStrictEqual(response.topics[0].partitions.length, 1, 'Should have one partition')
  deepStrictEqual(response.topics[0].partitions[0].partitionIndex, 0, 'Partition index should be parsed correctly')
  deepStrictEqual(response.topics[0].partitions[0].activeProducers.length, 1, 'Should have one active producer')

  const producer = response.topics[0].partitions[0].activeProducers[0]
  deepStrictEqual(producer.producerId, BigInt(1000), 'Producer ID should be parsed correctly')
  deepStrictEqual(producer.producerEpoch, 5, 'Producer epoch should be parsed correctly')
  deepStrictEqual(producer.lastSequence, 100, 'Last sequence should be parsed correctly')
  deepStrictEqual(producer.lastTimestamp, BigInt(1630000000000), 'Last timestamp should be parsed correctly')
  deepStrictEqual(producer.coordinatorEpoch, 3, 'Coordinator epoch should be parsed correctly')
  deepStrictEqual(
    producer.currentTxnStartOffset,
    BigInt(-1),
    'Current transaction start offset should be parsed correctly'
  )
})

test('parseResponse correctly processes a response with multiple topics and partitions', () => {
  // Create a response with multiple topics and partitions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'topic-1',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null,
              activeProducers: [
                {
                  producerId: BigInt(1000),
                  producerEpoch: 5,
                  lastSequence: 100,
                  lastTimestamp: BigInt(1630000000000),
                  coordinatorEpoch: 3,
                  currentTxnStartOffset: BigInt(-1)
                }
              ]
            }
          ]
        },
        {
          name: 'topic-2',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null,
              activeProducers: []
            },
            {
              partitionIndex: 1,
              errorCode: 0,
              errorMessage: null,
              activeProducers: [
                {
                  producerId: BigInt(2000),
                  producerEpoch: 10,
                  lastSequence: 200,
                  lastTimestamp: BigInt(1640000000000),
                  coordinatorEpoch: 6,
                  currentTxnStartOffset: BigInt(500)
                }
              ]
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendString(partition.errorMessage)
            .appendArray(partition.activeProducers, (w, producer) => {
              w.appendInt64(producer.producerId)
                .appendInt32(producer.producerEpoch)
                .appendInt32(producer.lastSequence)
                .appendInt64(producer.lastTimestamp)
                .appendInt32(producer.coordinatorEpoch)
                .appendInt64(producer.currentTxnStartOffset)
            })
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 61, 0, Reader.from(writer))

  // Verify multiple topics and partitions
  deepStrictEqual(response.topics.length, 2, 'Should have two topics')

  // Verify topic names
  deepStrictEqual(
    response.topics.map(t => t.name),
    ['topic-1', 'topic-2'],
    'Topic names should be parsed correctly'
  )

  // Verify partition counts
  deepStrictEqual(response.topics[1].partitions.length, 2, 'Second topic should have two partitions')

  // Verify active producers counts
  deepStrictEqual(
    response.topics[1].partitions[0].activeProducers.length,
    0,
    'First partition of second topic should have no active producers'
  )

  deepStrictEqual(
    response.topics[1].partitions[1].activeProducers.length,
    1,
    'Second partition of second topic should have one active producer'
  )

  // Verify producer details in second topic
  const producer = response.topics[1].partitions[1].activeProducers[0]
  deepStrictEqual(producer.producerId, BigInt(2000), 'Producer ID in second topic should be parsed correctly')
  deepStrictEqual(
    producer.currentTxnStartOffset,
    BigInt(500),
    'Transaction offset in second topic should be parsed correctly'
  )
})

test('parseResponse correctly processes a response with multiple producers per partition', () => {
  // Create a response with multiple producers per partition
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null,
              activeProducers: [
                {
                  producerId: BigInt(1000),
                  producerEpoch: 5,
                  lastSequence: 100,
                  lastTimestamp: BigInt(1630000000000),
                  coordinatorEpoch: 3,
                  currentTxnStartOffset: BigInt(-1)
                },
                {
                  producerId: BigInt(1001),
                  producerEpoch: 6,
                  lastSequence: 150,
                  lastTimestamp: BigInt(1635000000000),
                  coordinatorEpoch: 3,
                  currentTxnStartOffset: BigInt(-1)
                }
              ]
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendString(partition.errorMessage)
            .appendArray(partition.activeProducers, (w, producer) => {
              w.appendInt64(producer.producerId)
                .appendInt32(producer.producerEpoch)
                .appendInt32(producer.lastSequence)
                .appendInt64(producer.lastTimestamp)
                .appendInt32(producer.coordinatorEpoch)
                .appendInt64(producer.currentTxnStartOffset)
            })
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 61, 0, Reader.from(writer))

  // Verify multiple producers
  deepStrictEqual(
    response.topics[0].partitions[0].activeProducers.length,
    2,
    'Should have two active producers in the partition'
  )

  // Verify producer details for both producers
  deepStrictEqual(
    response.topics[0].partitions[0].activeProducers.map(p => p.producerId),
    [BigInt(1000), BigInt(1001)],
    'Producer IDs should be parsed correctly for multiple producers'
  )

  deepStrictEqual(
    response.topics[0].partitions[0].activeProducers.map(p => p.producerEpoch),
    [5, 6],
    'Producer epochs should be parsed correctly for multiple producers'
  )
})

test('parseResponse throws ResponseError on partition error', () => {
  // Create a response with a partition error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 39, // INVALID_TOPIC_EXCEPTION
              errorMessage: 'Invalid topic',
              activeProducers: []
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendString(partition.errorMessage)
            .appendArray([], () => {}) // Empty active producers
        })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 61, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify partition error details
      deepStrictEqual(
        err.response.topics[0].partitions[0].errorCode,
        39,
        'Partition error code should be preserved in the response'
      )

      deepStrictEqual(
        err.response.topics[0].partitions[0].errorMessage,
        'Invalid topic',
        'Partition error message should be preserved in the response'
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
    .appendTaggedFields()

  const response = parseResponse(1, 61, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
