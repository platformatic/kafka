import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { createRecordsBatch, crc32c, fetchV11, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = fetchV11

test('createRequest serializes basic parameters correctly', () => {
  const maxWaitMs = 5000
  const minBytes = 1
  const maxBytes = 1048576
  const isolationLevel = 0 // READ_UNCOMMITTED
  const sessionId = 0 // No session
  const sessionEpoch = 0
  const topics = [
    {
      topicId: 'test-topic',
      partitions: [
        {
          partition: 0,
          lastFetchedEpoch: -1,
          currentLeaderEpoch: 0,
          fetchOffset: 0n,
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

  // Verify basic parameters - v12 includes replica_id
  deepStrictEqual(
    {
      replicaId: reader.readInt32(),
      maxWaitMs: reader.readInt32(),
      minBytes: reader.readInt32(),
      maxBytes: reader.readInt32(),
      isolationLevel: reader.readInt8(),
      sessionId: reader.readInt32(),
      sessionEpoch: reader.readInt32()
    },
    {
      replicaId: -1,
      maxWaitMs,
      minBytes,
      maxBytes,
      isolationLevel,
      sessionId,
      sessionEpoch
    }
  )

  // Read topics array - v12 uses COMPACT_STRING for topic names, not UUID
  const topicsArray = reader.readArray(() => {
    const topicId = reader.readString(false)

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partition = reader.readInt32()
      const currentLeaderEpoch = reader.readInt32()
      const fetchOffset = reader.readInt64()
      const logStartOffset = reader.readInt64() // Should be -1 as it's a client request
      const partitionMaxBytes = reader.readInt32()

      return {
        partition,
        currentLeaderEpoch,
        fetchOffset,
        logStartOffset,
        partitionMaxBytes
      }
    }, false, false)

    return { topicId, partitions }
  }, false, false)

  // Verify the topics details
  deepStrictEqual(topicsArray, [
    {
      topicId: 'test-topic',
      partitions: [
        {
          partition: 0,
          currentLeaderEpoch: 0,
          fetchOffset: 0n,
          logStartOffset: -1n, // Always -1 for client requests
          partitionMaxBytes: 1048576
        }
      ]
    }
  ])

  // Verify remaining data
  deepStrictEqual(
    {
      forgottenTopics: reader.readArray(() => {}, false, false),
      rackId: reader.readString(false)
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
      topicId: 'topic-1',
      partitions: [
        {
          partition: 0,
          lastFetchedEpoch: -1,
          currentLeaderEpoch: 10,
          fetchOffset: 100n,
          partitionMaxBytes: 1048576
        },
        {
          partition: 1,
          lastFetchedEpoch: -1,
          currentLeaderEpoch: 10,
          fetchOffset: 200n,
          partitionMaxBytes: 1048576
        }
      ]
    },
    {
      topicId: 'topic-2',
      partitions: [
        {
          partition: 0,
          lastFetchedEpoch: -1,
          currentLeaderEpoch: 15,
          fetchOffset: 300n,
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

  // Verify basic parameters - v12 includes replica_id
  const basicParams = {
    replicaId: reader.readInt32(),
    maxWaitMs: reader.readInt32(),
    minBytes: reader.readInt32(),
    maxBytes: reader.readInt32(),
    isolationLevel: reader.readInt8(),
    sessionId: reader.readInt32(),
    sessionEpoch: reader.readInt32()
  }

  // Verify the basic parameters match expected values
  deepStrictEqual(basicParams, {
    replicaId: -1,
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
      topicId: 'test-topic',
      partitions: [
        {
          partition: 0,
          lastFetchedEpoch: -1,
          currentLeaderEpoch: 0,
          fetchOffset: 100n,
          partitionMaxBytes: 1048576
        }
      ]
    }
  ]
  const forgottenTopicsData = [
    {
      topicId: 'forgotten-topic',
      partitions: [0, 1]
    },
    {
      topic: 'legacy-forgotten-topic',
      partitions: [2]
    },
    {
      topicId: 'preferred-forgotten-topic',
      topic: 'ignored-legacy-topic',
      partitions: [3]
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
  // Basic parameters - v12 includes replica_id
  const replicaIdRead = reader.readInt32()
  const maxWaitMsRead = reader.readInt32()
  const minBytesRead = reader.readInt32()
  const maxBytesRead = reader.readInt32()
  const isolationLevelRead = reader.readInt8()
  const sessionIdRead = reader.readInt32()
  const sessionEpochRead = reader.readInt32()

  // Basic parameters verification
  deepStrictEqual(
    {
      replicaId: replicaIdRead,
      maxWaitMs: maxWaitMsRead,
      minBytes: minBytesRead,
      maxBytes: maxBytesRead,
      isolationLevel: isolationLevelRead,
      sessionId: sessionIdRead,
      sessionEpoch: sessionEpochRead
    },
    {
      replicaId: -1,
      maxWaitMs,
      minBytes,
      maxBytes,
      isolationLevel,
      sessionId,
      sessionEpoch
    },
    'Basic parameters should match'
  )

  // Topics array - v12 uses COMPACT_STRING for topic names
  const topicsRead = reader.readArray(() => {
    const topicId = reader.readString(false)
    const partitions = reader.readArray(() => {
      return {
        partition: reader.readInt32(),
        currentLeaderEpoch: reader.readInt32(),
        fetchOffset: reader.readInt64(),
        logStartOffset: reader.readInt64(),
        partitionMaxBytes: reader.readInt32()
      }
    }, false, false)
    return { topicId, partitions }
  }, false, false)

  // Topics verification
  deepStrictEqual(
    topicsRead,
    [
      {
        topicId: 'test-topic',
        partitions: [
          {
            partition: 0,
            currentLeaderEpoch: 0,
            fetchOffset: 100n,
            logStartOffset: -1n, // This is automatically set to -1 for client requests
            partitionMaxBytes: 1048576
          }
        ]
      }
    ],
    'Topics data should match'
  )

  // Forgotten topics array - v12 still uses UUID for forgotten topics
  const forgottenTopicsRead = reader.readArray(() => {
    const topic = reader.readString(false)
    const partitions = reader.readArray(() => reader.readInt32(), false, false)
    return { topic, partitions }
  }, false, false)

  deepStrictEqual(forgottenTopicsRead, [
    { topic: 'forgotten-topic', partitions: [0, 1] },
    { topic: 'legacy-forgotten-topic', partitions: [2] },
    { topic: 'preferred-forgotten-topic', partitions: [3] }
  ])

  // Rack ID
  const rackIdRead = reader.readString(false)
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
          topicId: 'test-topic',
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
        w.appendString(topic.topicId, false)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.highWatermark)
              .appendInt64(partition.lastStableOffset)
              .appendInt64(partition.logStartOffset)
              // Aborted transactions array (empty)
              .appendArray(partition.abortedTransactions, () => {}, false, false)
              .appendInt32(partition.preferredReadReplica)
              // Empty records (no records to return)
              .appendBytes(Buffer.alloc(0), false) // Empty records
          }, false, false)
      }
      , false, false)

  const response = parseResponse(1, 1, 11, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    sessionId: 123,
    responses: [
      {
        topicId: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            highWatermark: 100n,
            lastStableOffset: 100n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1,
            records: []
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
    .appendArray([], () => {}, false, false)

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 1, 11, Reader.from(writer))
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
          topicId: 'test-topic',
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
        w.appendString(topic.topicId, false)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.highWatermark)
              .appendInt64(partition.lastStableOffset)
              .appendInt64(partition.logStartOffset)
              // Aborted transactions array (empty)
              .appendArray(partition.abortedTransactions, () => {}, false, false)
              .appendInt32(partition.preferredReadReplica)
              // Empty records (no records with error)
              .appendBytes(Buffer.alloc(0), false) // Empty records
          }, false, false)
      }
      , false, false)

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 1, 11, Reader.from(writer))
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
            topicId: 'test-topic',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 9, // REPLICA_NOT_AVAILABLE
                highWatermark: 100n,
                lastStableOffset: 100n,
                logStartOffset: 0n,
                abortedTransactions: [],
                preferredReadReplica: -1,
                records: []
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
          topicId: 'topic-1',
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
          topicId: 'topic-2',
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
        w.appendString(topic.topicId, false)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.highWatermark)
              .appendInt64(partition.lastStableOffset)
              .appendInt64(partition.logStartOffset)
              // Aborted transactions array (empty)
              .appendArray(partition.abortedTransactions, () => {}, false, false)
              .appendInt32(partition.preferredReadReplica)
              // Empty records
              .appendBytes(Buffer.alloc(0), false) // Empty records
          }, false, false)
      }
      , false, false)

  const response = parseResponse(1, 1, 11, Reader.from(writer))

  // Verify the response structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    sessionId: 123,
    responses: [
      {
        topicId: 'topic-1',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            highWatermark: 100n,
            lastStableOffset: 100n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1,
            records: []
          },
          {
            partitionIndex: 1,
            errorCode: 0,
            highWatermark: 200n,
            lastStableOffset: 200n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1,
            records: []
          }
        ]
      },
      {
        topicId: 'topic-2',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            highWatermark: 300n,
            lastStableOffset: 300n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1,
            records: []
          }
        ]
      }
    ]
  })
})

test('parseResponse handles aborted transactions', () => {
  // Prepare an empty records batch for correct serialization
  const emptyRecordsBatch = Writer.create()
    .appendInt16(0) // attributes
    .appendInt32(0) // lastOffsetDelta
    .appendInt64(0n) // firstTimestamp
    .appendInt64(0n) // maxTimestamp
    .appendInt64(-1n) // producerId
    .appendInt16(0) // producerEpoch
    .appendInt32(0) // firstSequence
    .appendInt32(0) // number of records (0 for empty batch)

  emptyRecordsBatch.appendUnsignedInt32(crc32c(emptyRecordsBatch.dynamicBuffer), false).appendInt8(2, false).appendInt32(0, false).prependLength().appendInt64(0n, false)
  const emptyBatchReader = Reader.from(emptyRecordsBatch)
  strictEqual(emptyBatchReader.readInt64(), 0n)
  strictEqual(emptyBatchReader.readInt32(), 49)
  strictEqual(emptyRecordsBatch.length, 61)

  // Create a response with aborted transactions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt32(123) // sessionId
    // Responses array - using tagged fields format
    .appendArray(
      [
        {
          topicId: 'test-topic',
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
        w.appendString(topic.topicId, false)
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
              }, false, false)
              .appendInt32(partition.preferredReadReplica)
              // Add empty records batch
              .appendBytes(partition.recordsBatch.buffer, false)
          }, false, false)
      }
      , false, false)

  const response = parseResponse(1, 1, 11, Reader.from(writer))

  // Verify aborted transactions and records
  deepStrictEqual(
    {
      abortedTransactions: response.responses[0].partitions[0].abortedTransactions,
      recordsLength: response.responses[0].partitions[0].records?.[0]?.records?.length
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
  const recordsBatch = createRecordsBatch([{ topic: 'test-topic', value: Buffer.from('test-value'), timestamp: 1720000000000n }])
  const recordsBatchReader = Reader.from(recordsBatch)
  strictEqual(recordsBatchReader.readInt64(), 0n)
  strictEqual(recordsBatchReader.readInt32(), recordsBatch.length - 12)

  // Now create the full response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt32(123) // sessionId
    // Responses array - using tagged fields format
    .appendArray(
      [
        {
          topicId: 'test-topic',
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
        w.appendString(topic.topicId, false)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.highWatermark)
              .appendInt64(partition.lastStableOffset)
              .appendInt64(partition.logStartOffset)
              // Aborted transactions array (empty)
              .appendArray(partition.abortedTransactions, () => {}, false, false)
              .appendInt32(partition.preferredReadReplica)

              // Add records batch
              .appendBytes(partition.recordsBatch.buffer, false)
          }, false, false)
      }
      , false, false)

  const response = parseResponse(1, 1, 11, Reader.from(writer))

  // Verify the records were parsed correctly
  ok(response.responses[0].partitions[0].records, 'Records should be defined')

  const batch = response.responses[0].partitions[0].records[0]!
  const record = batch.records[0]

  deepStrictEqual(
    {
      firstOffset: batch.firstOffset,
      recordsLength: batch.records.length,
      offsetDelta: record.offsetDelta,
      valueString: record.value!.toString()
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

test('parseResponse handles truncated records', () => {
  const completeRecordsBatch = createRecordsBatch([{ topic: 'test-topic', value: Buffer.from('test-value'), timestamp: 1720000000000n }])
  const completeBatchReader = Reader.from(completeRecordsBatch)
  strictEqual(completeBatchReader.readInt64(), 0n)
  strictEqual(completeBatchReader.readInt32(), completeRecordsBatch.length - 12)
  const recordsBatch = Writer.create().appendFrom(completeRecordsBatch)
    // The final batch is deliberately incomplete.
    .appendInt64(0n) // firstOffset
    .appendInt32(60) // length

  // Now create the full response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt32(123) // sessionId
    // Responses array - using tagged fields format
    .appendArray(
      [
        {
          topicId: 'test-topic',
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
        w.appendString(topic.topicId, false)
          // Partitions array
          .appendArray(topic.partitions, (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt16(partition.errorCode)
              .appendInt64(partition.highWatermark)
              .appendInt64(partition.lastStableOffset)
              .appendInt64(partition.logStartOffset)
              // Aborted transactions array (empty)
              .appendArray(partition.abortedTransactions, () => {}, false, false)
              .appendInt32(partition.preferredReadReplica)

              // Add records batch
              .appendBytes(partition.recordsBatch.buffer, false)
          }, false, false)
      }
      , false, false)

  const response = parseResponse(1, 1, 11, Reader.from(writer))

  // Verify the records were parsed correctly
  ok(response.responses[0].partitions[0].records, 'Records should be defined')

  const batch = response.responses[0].partitions[0].records[0]!
  const record = batch.records[0]

  deepStrictEqual(
    {
      firstOffset: batch.firstOffset,
      recordsLength: batch.records.length,
      offsetDelta: record.offsetDelta,
      valueString: record.value!.toString()
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

test('parseResponse preserves nullable aborted transactions', () => {
  const response = parseResponse(1, 1, 11, Reader.from(Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendInt32(0)
    .appendArray([{ topic: 'topic' }], w => w.appendString('topic', false).appendArray([null, [], [{ producerId: 1n, firstOffset: 2n }]], (w, abortedTransactions, partition) => w
      .appendInt32(partition)
      .appendInt16(0)
      .appendInt64(10n)
      .appendInt64(9n)
      .appendInt64(0n)
      .appendArray(abortedTransactions, (w, transaction) => w.appendInt64(transaction.producerId).appendInt64(transaction.firstOffset), false, false)
      .appendInt32(-1)
      .appendBytes(Buffer.alloc(0), false), false, false), false, false)))

  deepStrictEqual(response.responses[0]!.partitions.map(partition => partition.abortedTransactions), [null, [], [{ producerId: 1n, firstOffset: 2n }]])
})

test('parseResponse preserves nullable records', () => {
  const batch = createRecordsBatch([{ topic: 'topic', value: Buffer.from('value') }])
  const records = [null, Buffer.alloc(0), batch.buffer]
  const recordsWire = Writer.create().appendBytes(records[0], false).appendBytes(records[1], false).appendBytes(records[2], false)
  const recordsReader = Reader.from(recordsWire)
  strictEqual(recordsReader.readInt32(), -1)
  strictEqual(recordsReader.readInt32(), 0)
  strictEqual(recordsReader.readInt32(), batch.length)

  const response = parseResponse(1, 1, 11, Reader.from(Writer.create().appendInt32(0).appendInt16(0).appendInt32(0).appendArray([{ topic: 'topic' }], w => w.appendString('topic', false).appendArray(records, (w, records, partition) => w
    .appendInt32(partition).appendInt16(0).appendInt64(10n).appendInt64(9n).appendInt64(0n).appendArray([], () => {}, false, false).appendInt32(-1).appendBytes(records, false), false, false), false, false)))

  deepStrictEqual(response.responses[0]!.partitions.map(partition => partition.records?.length === 1 ? partition.records[0].records[0].value?.toString() : partition.records), [null, [], 'value'])
})
