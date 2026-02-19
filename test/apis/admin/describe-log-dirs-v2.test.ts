import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, describeLogDirsV2 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeLogDirsV2

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
    const partitions = reader.readArray(() => reader.readInt32(), true, false)
    return { name, partitions }
  })

  // Verify serialized data
  deepStrictEqual(topicsArray, [], 'Empty topics array should be serialized correctly')
})

test('createRequest serializes single topic with partitions correctly', () => {
  const topics = [
    {
      name: 'test-topic',
      partitions: [0, 1, 2]
    }
  ]

  const writer = createRequest(topics)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const partitions = reader.readArray(() => reader.readInt32(), true, false)
    return { name, partitions }
  })

  // Verify serialized data
  deepStrictEqual(
    topicsArray,
    [
      {
        name: 'test-topic',
        partitions: [0, 1, 2]
      }
    ],
    'Single topic with partitions should be serialized correctly'
  )
})

test('createRequest serializes multiple topics correctly', () => {
  const topics = [
    {
      name: 'topic-1',
      partitions: [0, 1]
    },
    {
      name: 'topic-2',
      partitions: [0, 1, 2, 3]
    }
  ]

  const writer = createRequest(topics)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const name = reader.readString()
    const partitions = reader.readArray(() => reader.readInt32(), true, false)
    return { name, partitions }
  })

  // Verify multiple topics
  deepStrictEqual(
    topicsArray,
    [
      {
        name: 'topic-1',
        partitions: [0, 1]
      },
      {
        name: 'topic-2',
        partitions: [0, 1, 2, 3]
      }
    ],
    'Multiple topics should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no results (no root errorCode in v2)
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}) // Empty results array
    .appendTaggedFields()

  const response = parseResponse(1, 35, 2, Reader.from(writer))

  // Verify response structure (no errorCode at root level in v2)
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      results: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a response with log dirs', () => {
  // Create a successful response with log dirs (no totalBytes/usableBytes, no root errorCode in v2)
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          logDir: '/var/lib/kafka/data-1',
          topics: [
            {
              name: 'test-topic',
              partitions: [
                {
                  partitionIndex: 0,
                  partitionSize: BigInt(1024),
                  offsetLag: BigInt(0),
                  isFutureKey: false
                }
              ]
            }
          ]
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode)
          .appendString(result.logDir)
          .appendArray(result.topics, (w, topic) => {
            w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
              w.appendInt32(partition.partitionIndex)
                .appendInt64(partition.partitionSize)
                .appendInt64(partition.offsetLag)
                .appendBoolean(partition.isFutureKey)
            })
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 35, 2, Reader.from(writer))

  // Verify log dir structure
  deepStrictEqual(response.results.length, 1, 'Should have one log dir result')
  deepStrictEqual(response.results[0].logDir, '/var/lib/kafka/data-1', 'Log dir should be parsed correctly')
  deepStrictEqual(response.results[0].topics.length, 1, 'Should have one topic')
  deepStrictEqual(response.results[0].topics[0].name, 'test-topic', 'Topic name should be parsed correctly')
  deepStrictEqual(response.results[0].topics[0].partitions.length, 1, 'Should have one partition')
  deepStrictEqual(
    response.results[0].topics[0].partitions[0].partitionIndex,
    0,
    'Partition index should be parsed correctly'
  )
  deepStrictEqual(
    response.results[0].topics[0].partitions[0].partitionSize,
    BigInt(1024),
    'Partition size should be parsed correctly'
  )
  deepStrictEqual(
    response.results[0].topics[0].partitions[0].offsetLag,
    BigInt(0),
    'Offset lag should be parsed correctly'
  )
  deepStrictEqual(
    response.results[0].topics[0].partitions[0].isFutureKey,
    false,
    'IsFutureKey should be parsed correctly'
  )
})

test('parseResponse correctly processes a response with multiple log dirs', () => {
  // Create a response with multiple log dirs
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 0,
          logDir: '/var/lib/kafka/data-1',
          topics: [
            {
              name: 'test-topic',
              partitions: [
                {
                  partitionIndex: 0,
                  partitionSize: BigInt(1024),
                  offsetLag: BigInt(0),
                  isFutureKey: false
                }
              ]
            }
          ]
        },
        {
          errorCode: 0,
          logDir: '/var/lib/kafka/data-2',
          topics: [
            {
              name: 'test-topic',
              partitions: [
                {
                  partitionIndex: 1,
                  partitionSize: BigInt(2048),
                  offsetLag: BigInt(0),
                  isFutureKey: false
                }
              ]
            }
          ]
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode)
          .appendString(result.logDir)
          .appendArray(result.topics, (w, topic) => {
            w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
              w.appendInt32(partition.partitionIndex)
                .appendInt64(partition.partitionSize)
                .appendInt64(partition.offsetLag)
                .appendBoolean(partition.isFutureKey)
            })
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 35, 2, Reader.from(writer))

  // Verify multiple log dirs
  deepStrictEqual(response.results.length, 2, 'Should have two log dir results')

  // Verify log dir paths
  deepStrictEqual(
    response.results.map(r => r.logDir),
    ['/var/lib/kafka/data-1', '/var/lib/kafka/data-2'],
    'Log dir paths should be parsed correctly'
  )

  // Verify partition sizes across log dirs
  deepStrictEqual(
    response.results.map(r => r.topics[0].partitions[0].partitionSize),
    [BigInt(1024), BigInt(2048)],
    'Partition sizes should be parsed correctly across log dirs'
  )
})

test('parseResponse throws ResponseError on result-level error response', () => {
  // Create a response with a result-level error (no root errorCode in v2)
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          errorCode: 58, // DISK_ERROR
          logDir: '/var/lib/kafka/data-1'
        }
      ],
      (w, result) => {
        w.appendInt16(result.errorCode)
          .appendString(result.logDir)
          .appendArray([], () => {}) // Empty topics
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 35, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify result-level error
      deepStrictEqual(err.response.results[0].errorCode, 58, 'Result-level error code should be preserved')
      deepStrictEqual(err.response.results[0].logDir, '/var/lib/kafka/data-1', 'Log dir should be preserved with error')

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendArray([], () => {}) // Empty results
    .appendTaggedFields()

  const response = parseResponse(1, 35, 2, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
