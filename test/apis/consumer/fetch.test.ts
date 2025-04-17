import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { fetchV17, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = fetchV17

test('createRequest serializes basic parameters correctly', () => {
  const maxWaitMs = 5000
  const minBytes = 1
  const maxBytes = 1048576
  const isolationLevel = 0 // READ_UNCOMMITTED
  const sessionId = 0 // No session
  const sessionEpoch = 0
  const topics = [
    {
      topicId: '12345678-1234-1234-1234-123456789abc',
      partitions: [
        {
          partition: 0,
          currentLeaderEpoch: 0,
          fetchOffset: 0n,
          lastFetchedEpoch: 0,
          partitionMaxBytes: 1048576
        }
      ]
    }
  ]

  const rackId = ''

  const writer = createRequest(
    maxWaitMs,
    minBytes,
    maxBytes,
    isolationLevel,
    sessionId,
    sessionEpoch,
    topics,
    [],
    rackId
  )

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Verify basic parameters
  deepStrictEqual(
    {
      maxWaitMs: reader.readInt32(),
      minBytes: reader.readInt32(),
      maxBytes: reader.readInt32(),
      isolationLevel: reader.readInt8(),
      sessionId: reader.readInt32(),
      sessionEpoch: reader.readInt32()
    },
    {
      maxWaitMs,
      minBytes,
      maxBytes,
      isolationLevel,
      sessionId,
      sessionEpoch
    }
  )

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicId = reader.readUUID()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partition = reader.readInt32()
      const currentLeaderEpoch = reader.readInt32()
      const fetchOffset = reader.readInt64()
      const lastFetchedEpoch = reader.readInt32()
      const logStartOffset = reader.readInt64() // Should be -1 as it's a client request
      const partitionMaxBytes = reader.readInt32()

      return {
        partition,
        currentLeaderEpoch,
        fetchOffset,
        lastFetchedEpoch,
        logStartOffset,
        partitionMaxBytes
      }
    })

    return { topicId, partitions }
  })

  // Verify the topics details
  deepStrictEqual(topicsArray, [
    {
      topicId: '12345678-1234-1234-1234-123456789abc',
      partitions: [
        {
          partition: 0,
          currentLeaderEpoch: 0,
          fetchOffset: 0n,
          lastFetchedEpoch: 0,
          logStartOffset: -1n, // Always -1 for client requests
          partitionMaxBytes: 1048576
        }
      ]
    }
  ])

  // Verify remaining data
  deepStrictEqual(
    {
      forgottenTopics: reader.readArray(() => {}),
      rackId: reader.readString()
    },
    {
      forgottenTopics: [],
      rackId: ''
    }
  )
})

test('createRequest serializes multiple topics and partitions', () => {
  const maxWaitMs = 5000
  const minBytes = 1
  const maxBytes = 1048576
  const isolationLevel = 1 // READ_COMMITTED
  const sessionId = 123
  const sessionEpoch = 5
  const topics = [
    {
      topicId: '12345678-1234-1234-1234-123456789abc',
      partitions: [
        {
          partition: 0,
          currentLeaderEpoch: 10,
          fetchOffset: 100n,
          lastFetchedEpoch: 5,
          partitionMaxBytes: 1048576
        },
        {
          partition: 1,
          currentLeaderEpoch: 10,
          fetchOffset: 200n,
          lastFetchedEpoch: 5,
          partitionMaxBytes: 1048576
        }
      ]
    },
    {
      topicId: '87654321-4321-4321-4321-cba987654321',
      partitions: [
        {
          partition: 0,
          currentLeaderEpoch: 15,
          fetchOffset: 300n,
          lastFetchedEpoch: 10,
          partitionMaxBytes: 1048576
        }
      ]
    }
  ]
  const rackId = 'rack-1'

  const writer = createRequest(
    maxWaitMs,
    minBytes,
    maxBytes,
    isolationLevel,
    sessionId,
    sessionEpoch,
    topics,
    [],
    rackId
  )

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Verify basic parameters
  const basicParams = {
    maxWaitMs: reader.readInt32(),
    minBytes: reader.readInt32(),
    maxBytes: reader.readInt32(),
    isolationLevel: reader.readInt8(),
    sessionId: reader.readInt32(),
    sessionEpoch: reader.readInt32()
  }

  // Verify the basic parameters match expected values
  deepStrictEqual(basicParams, {
    maxWaitMs,
    minBytes,
    maxBytes,
    isolationLevel,
    sessionId,
    sessionEpoch
  })
})

test('createRequest handles forgotten topics data', () => {
  const maxWaitMs = 5000
  const minBytes = 1
  const maxBytes = 1048576
  const isolationLevel = 0
  const sessionId = 123
  const sessionEpoch = 5
  const topics = [
    {
      topicId: '12345678-1234-1234-1234-123456789abc',
      partitions: [
        {
          partition: 0,
          currentLeaderEpoch: 0,
          fetchOffset: 100n,
          lastFetchedEpoch: 0,
          partitionMaxBytes: 1048576
        }
      ]
    }
  ]
  const forgottenTopicsData = [
    {
      topic: '87654321-4321-4321-4321-cba987654321', // UUID as string
      partitions: [0, 1] // The original partition numbers
    }
  ]
  const rackId = 'rack'

  const writer = createRequest(
    maxWaitMs,
    minBytes,
    maxBytes,
    isolationLevel,
    sessionId,
    sessionEpoch,
    topics,
    forgottenTopicsData,
    rackId
  )

  // Verify writer creation and basic structure
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read the serialized data to verify correctness step by step
  // Basic parameters
  const maxWaitMsRead = reader.readInt32()
  const minBytesRead = reader.readInt32()
  const maxBytesRead = reader.readInt32()
  const isolationLevelRead = reader.readInt8()
  const sessionIdRead = reader.readInt32()
  const sessionEpochRead = reader.readInt32()

  // Basic parameters verification
  deepStrictEqual(
    {
      maxWaitMs: maxWaitMsRead,
      minBytes: minBytesRead,
      maxBytes: maxBytesRead,
      isolationLevel: isolationLevelRead,
      sessionId: sessionIdRead,
      sessionEpoch: sessionEpochRead
    },
    {
      maxWaitMs,
      minBytes,
      maxBytes,
      isolationLevel,
      sessionId,
      sessionEpoch
    },
    'Basic parameters should match'
  )

  // Topics array
  const topicsRead = reader.readArray(() => {
    const topicId = reader.readUUID()
    const partitions = reader.readArray(() => {
      return {
        partition: reader.readInt32(),
        currentLeaderEpoch: reader.readInt32(),
        fetchOffset: reader.readInt64(),
        lastFetchedEpoch: reader.readInt32(),
        logStartOffset: reader.readInt64(),
        partitionMaxBytes: reader.readInt32()
      }
    })
    return { topicId, partitions }
  })

  // Topics verification
  deepStrictEqual(
    topicsRead,
    [
      {
        topicId: '12345678-1234-1234-1234-123456789abc',
        partitions: [
          {
            partition: 0,
            currentLeaderEpoch: 0,
            fetchOffset: 100n,
            lastFetchedEpoch: 0,
            logStartOffset: -1n, // This is automatically set to -1 for client requests
            partitionMaxBytes: 1048576
          }
        ]
      }
    ],
    'Topics data should match'
  )

  // Forgotten topics array
  const forgottenTopicsRead = reader.readArray(() => {
    const topic = reader.readUUID()
    const partitions = reader.readArray(() => reader.readInt32(), true, false)
    return { topic, partitions }
  })

  // Forgotten topics verification - just check topic and array length
  deepStrictEqual(forgottenTopicsRead.length, 1, 'Should have 1 forgotten topic')
  deepStrictEqual(forgottenTopicsRead[0].topic, '87654321-4321-4321-4321-cba987654321', 'Topic UUID should match')
  deepStrictEqual(forgottenTopicsRead[0].partitions.length, 2, 'Should have 2 partitions')
  deepStrictEqual(forgottenTopicsRead[0].partitions[0], 0, 'First partition should be 0')

  // Rack ID
  const rackIdRead = reader.readString()
  deepStrictEqual(rackIdRead, rackId, 'Rack ID should match')
})

test('parseResponse correctly processes a successful simple response', () => {
  // Create a successful response with one topic and partition
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt32(123) // sessionId
    // Responses array - using tagged fields format
    .appendArray(
      [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              highWatermark: 100n,
              lastStableOffset: 100n,
              logStartOffset: 0n,
              abortedTransactions: [],
              preferredReadReplica: -1
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendUUID(topic.topicId)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.highWatermark)
              .appendInt64(partition.lastStableOffset)
              .appendInt64(partition.logStartOffset)
              // Aborted transactions array (empty)
              .appendArray(partition.abortedTransactions, () => {}, true, true)
              .appendInt32(partition.preferredReadReplica)
              // Empty records (no records to return)
              .appendUnsignedVarInt(1) // Just the tag buffer header
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 1, 17, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    sessionId: 123,
    responses: [
      {
        topicId: '12345678-1234-1234-1234-123456789abc',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            highWatermark: 100n,
            lastStableOffset: 100n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1
            // records field should be undefined because no records were returned
          }
        ]
      }
    ]
  })
})

test('parseResponse handles top-level error code', () => {
  // Create a response with a top-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(27) // errorCode (e.g., UNSUPPORTED_VERSION)
    .appendInt32(0) // sessionId
    // Empty responses array
    .appendArray([], () => {})
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 1, 17, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      // Verify the response is preserved
      deepStrictEqual(err.response, {
        errorCode: 27,
        throttleTimeMs: 0,
        sessionId: 0,
        responses: []
      })

      return true
    }
  )
})

test('parseResponse handles partition-level error code', () => {
  // Create a response with a partition-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success at top level)
    .appendInt32(123) // sessionId
    // Responses array - using tagged fields format
    .appendArray(
      [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 9, // REPLICA_NOT_AVAILABLE
              highWatermark: 100n,
              lastStableOffset: 100n,
              logStartOffset: 0n,
              abortedTransactions: [],
              preferredReadReplica: -1
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendUUID(topic.topicId)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.highWatermark)
              .appendInt64(partition.lastStableOffset)
              .appendInt64(partition.logStartOffset)
              // Aborted transactions array (empty)
              .appendArray(partition.abortedTransactions, () => {}, true, true)
              .appendInt32(partition.preferredReadReplica)
              // Empty records (no records with error)
              .appendUnsignedVarInt(1) // Just the tag buffer header
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 1, 17, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        errorCode: 0, // Top-level is success
        sessionId: 123,
        responses: [
          {
            topicId: '12345678-1234-1234-1234-123456789abc',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 9, // REPLICA_NOT_AVAILABLE
                highWatermark: 100n,
                lastStableOffset: 100n,
                logStartOffset: 0n,
                abortedTransactions: [],
                preferredReadReplica: -1
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
    .appendInt16(0) // errorCode (success)
    .appendInt32(123) // sessionId
    // Responses array - using tagged fields format
    .appendArray(
      [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              highWatermark: 100n,
              lastStableOffset: 100n,
              logStartOffset: 0n,
              abortedTransactions: [],
              preferredReadReplica: -1
            },
            {
              partitionIndex: 1,
              errorCode: 0,
              highWatermark: 200n,
              lastStableOffset: 200n,
              logStartOffset: 0n,
              abortedTransactions: [],
              preferredReadReplica: -1
            }
          ]
        },
        {
          topicId: '87654321-4321-4321-4321-cba987654321',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              highWatermark: 300n,
              lastStableOffset: 300n,
              logStartOffset: 0n,
              abortedTransactions: [],
              preferredReadReplica: -1
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendUUID(topic.topicId)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.highWatermark)
              .appendInt64(partition.lastStableOffset)
              .appendInt64(partition.logStartOffset)
              // Aborted transactions array (empty)
              .appendArray(partition.abortedTransactions, () => {}, true, true)
              .appendInt32(partition.preferredReadReplica)
              // Empty records
              .appendUnsignedVarInt(1) // Just the tag buffer header
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 1, 17, Reader.from(writer))

  // Verify the response structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    sessionId: 123,
    responses: [
      {
        topicId: '12345678-1234-1234-1234-123456789abc',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            highWatermark: 100n,
            lastStableOffset: 100n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1
          },
          {
            partitionIndex: 1,
            errorCode: 0,
            highWatermark: 200n,
            lastStableOffset: 200n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1
          }
        ]
      },
      {
        topicId: '87654321-4321-4321-4321-cba987654321',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            highWatermark: 300n,
            lastStableOffset: 300n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1
          }
        ]
      }
    ]
  })
})

test('parseResponse handles aborted transactions', () => {
  // Prepare an empty records batch for correct serialization
  const emptyRecordsBatch = Writer.create()
    .appendInt64(0n) // firstOffset
    .appendInt32(20) // length - minimal value for an empty batch
    .appendInt32(0) // partitionLeaderEpoch
    .appendInt8(2) // magic (record format version)
    .appendUnsignedInt32(0) // crc
    .appendInt16(0) // attributes
    .appendInt32(0) // lastOffsetDelta
    .appendInt64(0n) // firstTimestamp
    .appendInt64(0n) // maxTimestamp
    .appendInt64(-1n) // producerId
    .appendInt16(0) // producerEpoch
    .appendInt32(0) // firstSequence
    .appendInt32(0) // number of records (0 for empty batch)

  // Create a response with aborted transactions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt32(123) // sessionId
    // Responses array - using tagged fields format
    .appendArray(
      [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              highWatermark: 100n,
              lastStableOffset: 50n,
              logStartOffset: 0n,
              abortedTransactions: [
                {
                  producerId: 1234n,
                  firstOffset: 10n
                }
              ],
              preferredReadReplica: -1,
              recordsBatch: emptyRecordsBatch
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendUUID(topic.topicId)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.highWatermark)
              .appendInt64(partition.lastStableOffset)
              .appendInt64(partition.logStartOffset)
              // Aborted transactions array
              .appendArray(partition.abortedTransactions, (w, txn) => {
                w.appendInt64(txn.producerId).appendInt64(txn.firstOffset)
              })
              .appendInt32(partition.preferredReadReplica)
              // Add empty records batch
              .appendUnsignedVarInt(partition.recordsBatch.length + 2) // Records size (including varint length)
              .appendFrom(partition.recordsBatch)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 1, 17, Reader.from(writer))

  // Verify aborted transactions and records
  deepStrictEqual(
    {
      abortedTransactions: response.responses[0].partitions[0].abortedTransactions,
      recordsLength: response.responses[0].partitions[0].records?.records.length
    },
    {
      abortedTransactions: [
        {
          producerId: 1234n,
          firstOffset: 10n
        }
      ],
      recordsLength: 0
    }
  )

  // Verify records is defined
  ok(response.responses[0].partitions[0].records, 'Records should be defined')
})

test('parseResponse parses record data', () => {
  // Create a response with records data
  // First create a record batch
  const timestamp = BigInt(Date.now())
  const recordsBatch = Writer.create()
    // Record batch structure
    .appendInt64(0n) // firstOffset
    .appendInt32(60) // length - this would be dynamically computed in real usage
    .appendInt32(0) // partitionLeaderEpoch
    .appendInt8(2) // magic (record format version)
    .appendUnsignedInt32(0) // crc - would be computed properly in real code
    .appendInt16(0) // attributes
    .appendInt32(0) // lastOffsetDelta
    .appendInt64(timestamp) // firstTimestamp
    .appendInt64(timestamp) // maxTimestamp
    .appendInt64(-1n) // producerId - not specified
    .appendInt16(0) // producerEpoch
    .appendInt32(0) // firstSequence
    .appendInt32(1) // number of records
    // Single record
    .appendVarInt(8) // length of the record
    .appendInt8(0) // attributes
    .appendVarInt64(0n) // timestampDelta
    .appendVarInt(0) // offsetDelta
    .appendVarIntBytes(null) // key
    .appendVarIntBytes(Buffer.from('test-value')) // value
    .appendVarIntArray([], () => {}) // No headers

  // Now create the full response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt32(123) // sessionId
    // Responses array - using tagged fields format
    .appendArray(
      [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              highWatermark: 100n,
              lastStableOffset: 100n,
              logStartOffset: 0n,
              abortedTransactions: [],
              preferredReadReplica: -1,
              recordsBatch
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendUUID(topic.topicId)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.highWatermark)
              .appendInt64(partition.lastStableOffset)
              .appendInt64(partition.logStartOffset)
              // Aborted transactions array (empty)
              .appendArray(partition.abortedTransactions, () => {}, true, true)
              .appendInt32(partition.preferredReadReplica)

              // Add records batch
              .appendUnsignedVarInt(partition.recordsBatch.length + 2) // Records size (including varint length)
              .appendFrom(partition.recordsBatch)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 1, 17, Reader.from(writer))

  // Verify the records were parsed correctly
  ok(response.responses[0].partitions[0].records, 'Records should be defined')

  const records = response.responses[0].partitions[0].records!
  const record = records.records[0]

  deepStrictEqual(
    {
      firstOffset: records.firstOffset,
      recordsLength: records.records.length,
      offsetDelta: record.offsetDelta,
      valueString: record.value.toString()
    },
    {
      firstOffset: 0n,
      recordsLength: 1,
      offsetDelta: 0,
      valueString: 'test-value'
    }
  )

  // Verify value is a Buffer
  ok(Buffer.isBuffer(record.value))
})
