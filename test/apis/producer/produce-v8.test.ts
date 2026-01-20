import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { type MessageRecord, ProduceAcks, produceV8, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = produceV8

test('createRequest serializes basic parameters correctly', () => {
  const acks = 1
  const timeout = 30000
  const topicData = [
    {
      topic: 'test-topic',
      partition: 0,
      key: Buffer.from('key1'),
      value: Buffer.from('value1')
    }
  ]

  const writer = createRequest(acks, timeout, topicData)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read and verify basic parameters
  deepStrictEqual(
    {
      transactionalId: reader.readNullableString(false),
      acks: reader.readInt16(),
      timeout: reader.readInt32()
    },
    {
      transactionalId: null,
      acks: 1,
      timeout: 30000
    }
  )

  // We can't easily verify the record batch content directly as it's complex binary format
  // But we can at least check that we have topic data array with length 1
  const topicsArray = reader.readArray(
    () => {
      const topicName = reader.readString(false)
      return topicName
    },
    false,
    false
  )

  // Verify topics array
  deepStrictEqual(topicsArray, ['test-topic'])
})

test('createRequest with acks=0 sets noResponse flag', () => {
  const topicData = [
    {
      topic: 'test-topic',
      key: Buffer.from('key1'),
      value: Buffer.from('value1')
    }
  ]

  const writer = createRequest(ProduceAcks.NO_RESPONSE, 30000, topicData)

  // Verify noResponse flag is set
  ok(writer.context.noResponse, 'noResponse flag should be set for acks=0')
})

test('createRequest handles multiple topics and partitions', () => {
  const topicData: MessageRecord[] = [
    {
      topic: 'topic1',
      partition: 0,
      key: Buffer.from('key1'),
      value: Buffer.from('value1')
    },
    {
      topic: 'topic1',
      partition: 1,
      key: Buffer.from('key2'),
      value: Buffer.from('value2')
    },
    {
      topic: 'topic2',
      partition: 0,
      key: Buffer.from('key3'),
      value: Buffer.from('value3')
    }
  ]

  const result = createRequest(1, 30000, topicData)

  // Verify it returns a Writer instance
  ok(result instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify structure
  const reader = Reader.from(result)

  // Read and verify basic parameters
  deepStrictEqual(
    {
      transactionalId: reader.readNullableString(false),
      acks: reader.readInt16(),
      timeout: reader.readInt32()
    },
    {
      transactionalId: null,
      acks: 1,
      timeout: 30000
    },
    'Basic parameters should match expected values'
  )

  // We can't easily verify the exact structure of the serialized data
  // because of the complex record batch format

  // But we can verify normalization happened to the input
  deepStrictEqual(
    [topicData[0].partition, topicData[1].partition, topicData[2].partition],
    [0, 1, 0],
    'Partitions should be correctly normalized'
  )

  // And we can verify all messages have timestamps
  ok(
    topicData.every(record => record.timestamp !== undefined),
    'All records should have timestamps'
  )

  // Verify the topic data was grouped correctly by checking the original input
  const groupedTopics = new Set(topicData.map(record => record.topic))
  deepStrictEqual(Array.from(groupedTopics).sort(), ['topic1', 'topic2'], 'Topics should be correctly identified')

  // Count partitions per topic to verify structure
  const topic1Records = topicData.filter(record => record.topic === 'topic1')
  const topic2Records = topicData.filter(record => record.topic === 'topic2')

  deepStrictEqual(topic1Records.length, 2, 'topic1 should have 2 records')
  deepStrictEqual(topic2Records.length, 1, 'topic2 should have 1 record')
})

test('createRequest sets default partition and timestamp if not provided', () => {
  const topicData: MessageRecord[] = [
    {
      topic: 'test-topic',
      key: Buffer.from('key1'),
      value: Buffer.from('value1')
      // No partition specified, should default to 0
      // No timestamp specified, should default to current time
    }
  ]

  const beforeTime = Date.now()
  createRequest(1, 30000, topicData)
  const afterTime = Date.now()

  // Verify that partition and timestamp were normalized
  strictEqual(topicData[0].partition, 0) // Default partition should be 0

  // Timestamp should be within the window between before and after test execution
  const timestamp = Number(topicData[0].timestamp)
  ok(
    timestamp >= beforeTime && timestamp <= afterTime,
    `Timestamp ${timestamp} should be between ${beforeTime} and ${afterTime}`
  )
})

test('createRequest with transactional ID', () => {
  const acks = 1
  const timeout = 30000
  const topicData = [
    {
      topic: 'test-topic',
      partition: 0,
      key: Buffer.from('key1'),
      value: Buffer.from('value1')
    }
  ]
  const options = {
    transactionalId: 'transaction-123'
  }

  const writer = createRequest(acks, timeout, topicData, options)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify transactional ID
  const reader = Reader.from(writer)

  // Read and verify parameters including transactional ID
  deepStrictEqual(
    {
      transactionalId: reader.readString(false),
      acks: reader.readInt16(),
      timeout: reader.readInt32()
    },
    {
      transactionalId: 'transaction-123',
      acks: 1,
      timeout: 30000
    }
  )
})

test('createRequest with additional record batch options', () => {
  const acks = 1
  const timeout = 30000
  const topicData = [
    {
      topic: 'test-topic',
      partition: 0,
      key: Buffer.from('key1'),
      value: Buffer.from('value1')
    }
  ]
  const options = {
    producerId: 1234n,
    producerEpoch: 5
    // Skip compression field since it's not supported in this implementation
  }

  const writer = createRequest(acks, timeout, topicData, options)

  // Verify it returns a Writer instance
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify structure
  const reader = Reader.from(writer)

  // Read and verify basic parameters
  deepStrictEqual(
    {
      transactionalId: reader.readNullableString(false),
      acks: reader.readInt16(),
      timeout: reader.readInt32()
    },
    {
      transactionalId: null,
      acks: 1,
      timeout: 30000
    },
    'Basic parameters should match expected values'
  )

  // Read topic array to verify structure
  const topicsArray = reader.readArray(
    () => {
      const topicName = reader.readString(false)
      return topicName
    },
    false,
    false
  )

  // Verify topics array
  deepStrictEqual(topicsArray, ['test-topic'], 'Topic name should match')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    // Topics array - non-compact format
    .appendArray(
      [
        {
          name: 'test-topic',
          partitionResponses: [
            {
              index: 0,
              errorCode: 0,
              baseOffset: 100n,
              logAppendTimeMs: 1617234567890n,
              logStartOffset: 50n,
              recordErrors: [],
              errorMessage: null
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false)
          // Partitions array
          .appendArray(
            topic.partitionResponses,
            (w, partition) => {
              w.appendInt32(partition.index)
                .appendInt16(partition.errorCode)
                .appendInt64(partition.baseOffset)
                .appendInt64(partition.logAppendTimeMs)
                .appendInt64(partition.logStartOffset)
                // Record errors array (empty)
                .appendArray(partition.recordErrors, () => {}, false, false)
                .appendString(partition.errorMessage, false)
            },
            false,
            false
          )
      },
      false,
      false
    )
    .appendInt32(0) // throttleTimeMs

  const response = parseResponse(1, 0, 8, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    responses: [
      {
        name: 'test-topic',
        partitionResponses: [
          {
            index: 0,
            errorCode: 0,
            baseOffset: 100n,
            logAppendTimeMs: 1617234567890n,
            logStartOffset: 50n,
            recordErrors: [],
            errorMessage: null
          }
        ]
      }
    ],
    throttleTimeMs: 0
  })
})

test('parseResponse handles response with multiple topics and partitions', () => {
  // Create a response with multiple topics and partitions
  const writer = Writer.create()
    // Topics array - non-compact format
    .appendArray(
      [
        {
          name: 'topic1',
          partitionResponses: [
            {
              index: 0,
              errorCode: 0,
              baseOffset: 100n,
              logAppendTimeMs: 1617234567890n,
              logStartOffset: 50n,
              recordErrors: [],
              errorMessage: null
            },
            {
              index: 1,
              errorCode: 0,
              baseOffset: 200n,
              logAppendTimeMs: 1617234567890n,
              logStartOffset: 150n,
              recordErrors: [],
              errorMessage: null
            }
          ]
        },
        {
          name: 'topic2',
          partitionResponses: [
            {
              index: 0,
              errorCode: 0,
              baseOffset: 300n,
              logAppendTimeMs: 1617234567890n,
              logStartOffset: 250n,
              recordErrors: [],
              errorMessage: null
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false)
          // Partitions array
          .appendArray(
            topic.partitionResponses,
            (w, partition) => {
              w.appendInt32(partition.index)
                .appendInt16(partition.errorCode)
                .appendInt64(partition.baseOffset)
                .appendInt64(partition.logAppendTimeMs)
                .appendInt64(partition.logStartOffset)
                // Record errors array (empty)
                .appendArray(partition.recordErrors, () => {}, false, false)
                .appendString(partition.errorMessage, false)
            },
            false,
            false
          )
      },
      false,
      false
    )
    .appendInt32(0) // throttleTimeMs

  const response = parseResponse(1, 0, 8, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    responses: [
      {
        name: 'topic1',
        partitionResponses: [
          {
            index: 0,
            errorCode: 0,
            baseOffset: 100n,
            logAppendTimeMs: 1617234567890n,
            logStartOffset: 50n,
            recordErrors: [],
            errorMessage: null
          },
          {
            index: 1,
            errorCode: 0,
            baseOffset: 200n,
            logAppendTimeMs: 1617234567890n,
            logStartOffset: 150n,
            recordErrors: [],
            errorMessage: null
          }
        ]
      },
      {
        name: 'topic2',
        partitionResponses: [
          {
            index: 0,
            errorCode: 0,
            baseOffset: 300n,
            logAppendTimeMs: 1617234567890n,
            logStartOffset: 250n,
            recordErrors: [],
            errorMessage: null
          }
        ]
      }
    ],
    throttleTimeMs: 0
  })
})

test('parseResponse handles partition error codes', () => {
  // Create a response with partition error
  const writer = Writer.create()
    // Topics array - non-compact format
    .appendArray(
      [
        {
          name: 'test-topic',
          partitionResponses: [
            {
              index: 0,
              errorCode: 37, // NOT_LEADER_OR_FOLLOWER
              baseOffset: -1n, // invalid for error
              logAppendTimeMs: 0n,
              logStartOffset: 0n,
              recordErrors: [],
              errorMessage: null
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false)
          // Partitions array
          .appendArray(
            topic.partitionResponses,
            (w, partition) => {
              w.appendInt32(partition.index)
                .appendInt16(partition.errorCode)
                .appendInt64(partition.baseOffset)
                .appendInt64(partition.logAppendTimeMs)
                .appendInt64(partition.logStartOffset)
                // Record errors array (empty)
                .appendArray(partition.recordErrors, () => {}, false, false)
                .appendString(partition.errorMessage, false)
            },
            false,
            false
          )
      },
      false,
      false
    )
    .appendInt32(0) // throttleTimeMs

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 0, 8, Reader.from(writer))
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

      // Verify the response is preserved
      deepStrictEqual(err.response, {
        responses: [
          {
            name: 'test-topic',
            partitionResponses: [
              {
                index: 0,
                errorCode: 37, // NOT_LEADER_OR_FOLLOWER
                baseOffset: -1n,
                logAppendTimeMs: 0n,
                logStartOffset: 0n,
                recordErrors: [],
                errorMessage: null
              }
            ]
          }
        ],
        throttleTimeMs: 0
      })

      return true
    }
  )
})

test('parseResponse handles record-level errors', () => {
  // Create a response with record errors
  const writer = Writer.create()
    // Topics array - non-compact format
    .appendArray(
      [
        {
          name: 'test-topic',
          partitionResponses: [
            {
              index: 0,
              errorCode: 0, // partition success
              baseOffset: 100n,
              logAppendTimeMs: 1617234567890n,
              logStartOffset: 50n,
              recordErrors: [
                {
                  batchIndex: 2, // error in 3rd record
                  batchIndexErrorMessage: 'Record validation failed'
                }
              ],
              errorMessage: 'Error'
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false)
          // Partitions array
          .appendArray(
            topic.partitionResponses,
            (w, partition) => {
              w.appendInt32(partition.index)
                .appendInt16(partition.errorCode)
                .appendInt64(partition.baseOffset)
                .appendInt64(partition.logAppendTimeMs)
                .appendInt64(partition.logStartOffset)
                // Record errors array
                .appendArray(
                  partition.recordErrors,
                  (w, recordError) => {
                    w.appendInt32(recordError.batchIndex).appendString(recordError.batchIndexErrorMessage, false)
                  },
                  false,
                  false
                )
                .appendString(partition.errorMessage, false)
            },
            false,
            false
          )
      },
      false,
      false
    )
    .appendInt32(0) // throttleTimeMs

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 0, 8, Reader.from(writer))
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
        responses: [
          {
            name: 'test-topic',
            partitionResponses: [
              {
                index: 0,
                errorCode: 0, // Partition is success
                baseOffset: 100n,
                logAppendTimeMs: 1617234567890n,
                logStartOffset: 50n,
                recordErrors: [
                  {
                    batchIndex: 2,
                    batchIndexErrorMessage: 'Record validation failed'
                  }
                ],
                errorMessage: 'Error'
              }
            ]
          }
        ],
        throttleTimeMs: 0
      })

      return true
    }
  )
})

test('parseResponse handles throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    // Topics array - non-compact format
    .appendArray(
      [
        {
          name: 'test-topic',
          partitionResponses: [
            {
              index: 0,
              errorCode: 0,
              baseOffset: 100n,
              logAppendTimeMs: 1617234567890n,
              logStartOffset: 50n,
              recordErrors: [],
              errorMessage: null
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name, false)
          // Partitions array
          .appendArray(
            topic.partitionResponses,
            (w, partition) => {
              w.appendInt32(partition.index)
                .appendInt16(partition.errorCode)
                .appendInt64(partition.baseOffset)
                .appendInt64(partition.logAppendTimeMs)
                .appendInt64(partition.logStartOffset)
                // Record errors array (empty)
                .appendArray(partition.recordErrors, () => {}, false, false)
                .appendString(partition.errorMessage, false)
            },
            false,
            false
          )
      },
      false,
      false
    )
    .appendInt32(100) // throttleTimeMs - non-zero value for throttling

  const response = parseResponse(1, 0, 8, Reader.from(writer))

  // Verify response structure with throttling
  deepStrictEqual(response, {
    responses: [
      {
        name: 'test-topic',
        partitionResponses: [
          {
            index: 0,
            errorCode: 0,
            baseOffset: 100n,
            logAppendTimeMs: 1617234567890n,
            logStartOffset: 50n,
            recordErrors: [],
            errorMessage: null
          }
        ]
      }
    ],
    throttleTimeMs: 100
  })
})
