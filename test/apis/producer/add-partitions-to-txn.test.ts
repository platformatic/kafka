import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { addPartitionsToTxnV5, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = addPartitionsToTxnV5

test('createRequest serializes basic transaction parameters correctly', () => {
  const transactions = [
    {
      transactionalId: 'transaction-123',
      producerId: 1234567890n,
      producerEpoch: 5,
      verifyOnly: false,
      topics: [
        {
          name: 'test-topic',
          partitions: [0, 1, 2]
        }
      ]
    }
  ]

  const writer = createRequest(transactions)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read the transactions array
  const transactionsArray = reader.readArray(() => {
    const transactionalId = reader.readString()
    const producerId = reader.readInt64()
    const producerEpoch = reader.readInt16()
    const verifyOnly = reader.readBoolean()

    // Read topics array
    const topics = reader.readArray(() => {
      const topicName = reader.readString()

      // Read partitions array
      const partitions = reader.readArray(() => reader.readInt32(), true, false)

      return { topicName, partitions }
    })

    return { transactionalId, producerId, producerEpoch, verifyOnly, topics }
  })

  // Verify the transactions array structure and content
  deepStrictEqual(transactionsArray.length, 1, 'Should have 1 transaction')

  // Verify first transaction details
  const transaction = transactionsArray[0]
  deepStrictEqual(transaction.transactionalId, 'transaction-123')
  deepStrictEqual(transaction.producerId, 1234567890n)
  deepStrictEqual(transaction.producerEpoch, 5)
  deepStrictEqual(transaction.verifyOnly, false)

  // Verify topics and partitions
  deepStrictEqual(transaction.topics.length, 1, 'Should have 1 topic')
  deepStrictEqual(transaction.topics[0].topicName, 'test-topic')
  deepStrictEqual(transaction.topics[0].partitions, [0, 1, 2])
})

test('createRequest serializes multiple transactions correctly', () => {
  const transactions = [
    {
      transactionalId: 'transaction-1',
      producerId: 1234n,
      producerEpoch: 5,
      verifyOnly: false,
      topics: [
        {
          name: 'topic-1',
          partitions: [0, 1]
        }
      ]
    },
    {
      transactionalId: 'transaction-2',
      producerId: 5678n,
      producerEpoch: 10,
      verifyOnly: true, // Set verify only to true
      topics: [
        {
          name: 'topic-2',
          partitions: [0]
        },
        {
          name: 'topic-3',
          partitions: [0, 1, 2]
        }
      ]
    }
  ]

  const writer = createRequest(transactions)

  // Verify it's a writer instance
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read the transactions array
  const transactionsArray = reader.readArray(() => {
    const transactionalId = reader.readString()
    const producerId = reader.readInt64()
    const producerEpoch = reader.readInt16()
    const verifyOnly = reader.readBoolean()

    // Read topics array
    const topics = reader.readArray(() => {
      const topicName = reader.readString()

      // Read partitions array
      const partitions = reader.readArray(() => reader.readInt32(), true, false)

      return { topicName, partitions }
    })

    return { transactionalId, producerId, producerEpoch, verifyOnly, topics }
  })

  // Verify the entire transactions array structure with deepStrictEqual
  deepStrictEqual(transactionsArray, [
    {
      transactionalId: 'transaction-1',
      producerId: 1234n,
      producerEpoch: 5,
      verifyOnly: false,
      topics: [
        {
          topicName: 'topic-1',
          partitions: [0, 1]
        }
      ]
    },
    {
      transactionalId: 'transaction-2',
      producerId: 5678n,
      producerEpoch: 10,
      verifyOnly: true,
      topics: [
        {
          topicName: 'topic-2',
          partitions: [0]
        },
        {
          topicName: 'topic-3',
          partitions: [0, 1, 2]
        }
      ]
    }
  ])
})

test('parseResponse correctly processes a successful simple response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    // resultsByTransaction array - compact format
    .appendArray(
      [
        {
          transactionalId: 'transaction-123',
          topicResults: [
            {
              name: 'test-topic',
              resultsByPartition: [
                {
                  partitionIndex: 0,
                  partitionErrorCode: 0
                }
              ]
            }
          ]
        }
      ],
      (w, transaction) => {
        w.appendString(transaction.transactionalId)
          // topicResults array
          .appendArray(transaction.topicResults, (w, topic) => {
            w.appendString(topic.name)
              // resultsByPartition array
              .appendArray(topic.resultsByPartition, (w, partition) => {
                w.appendInt32(partition.partitionIndex).appendInt16(partition.partitionErrorCode)
              })
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 24, 5, writer.bufferList)

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    resultsByTransaction: [
      {
        transactionalId: 'transaction-123',
        topicResults: [
          {
            name: 'test-topic',
            resultsByPartition: [
              {
                partitionIndex: 0,
                partitionErrorCode: 0
              }
            ]
          }
        ]
      }
    ]
  })
})

test('parseResponse correctly processes a complex response', () => {
  // Create a response with multiple transactions, topics, and partitions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    // resultsByTransaction array - compact format
    .appendArray(
      [
        {
          transactionalId: 'transaction-1',
          topicResults: [
            {
              name: 'topic-1',
              resultsByPartition: [
                {
                  partitionIndex: 0,
                  partitionErrorCode: 0
                },
                {
                  partitionIndex: 1,
                  partitionErrorCode: 0
                }
              ]
            }
          ]
        },
        {
          transactionalId: 'transaction-2',
          topicResults: [
            {
              name: 'topic-2',
              resultsByPartition: [
                {
                  partitionIndex: 0,
                  partitionErrorCode: 0
                }
              ]
            },
            {
              name: 'topic-3',
              resultsByPartition: [
                {
                  partitionIndex: 0,
                  partitionErrorCode: 0
                }
              ]
            }
          ]
        }
      ],
      (w, transaction) => {
        w.appendString(transaction.transactionalId)
          // topicResults array
          .appendArray(transaction.topicResults, (w, topic) => {
            w.appendString(topic.name)
              // resultsByPartition array
              .appendArray(topic.resultsByPartition, (w, partition) => {
                w.appendInt32(partition.partitionIndex).appendInt16(partition.partitionErrorCode)
              })
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 24, 5, writer.bufferList)

  // Verify full response structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    resultsByTransaction: [
      {
        transactionalId: 'transaction-1',
        topicResults: [
          {
            name: 'topic-1',
            resultsByPartition: [
              {
                partitionIndex: 0,
                partitionErrorCode: 0
              },
              {
                partitionIndex: 1,
                partitionErrorCode: 0
              }
            ]
          }
        ]
      },
      {
        transactionalId: 'transaction-2',
        topicResults: [
          {
            name: 'topic-2',
            resultsByPartition: [
              {
                partitionIndex: 0,
                partitionErrorCode: 0
              }
            ]
          },
          {
            name: 'topic-3',
            resultsByPartition: [
              {
                partitionIndex: 0,
                partitionErrorCode: 0
              }
            ]
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
    .appendInt16(47) // errorCode (INVALID_TXN_STATE)
    // Empty resultsByTransaction array - compact format
    .appendArray([], () => {})
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 24, 5, writer.bufferList)
    },
    (err: any) => {
      // Verify error is the right type
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists and has the right type
      ok(err.errors && typeof err.errors === 'object', 'Errors object should exist')

      // Verify the response is preserved
      deepStrictEqual(err.response.errorCode, 47)
      deepStrictEqual(err.response.throttleTimeMs, 0)
      deepStrictEqual(err.response.resultsByTransaction.length, 0)

      return true
    }
  )
})

test('parseResponse handles partition-level error code', () => {
  // Create a response with a partition-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success at top level)
    // resultsByTransaction array - compact format
    .appendArray(
      [
        {
          transactionalId: 'transaction-123',
          topicResults: [
            {
              name: 'test-topic',
              resultsByPartition: [
                {
                  partitionIndex: 0,
                  partitionErrorCode: 37 // NOT_LEADER_OR_FOLLOWER
                }
              ]
            }
          ]
        }
      ],
      (w, transaction) => {
        w.appendString(transaction.transactionalId)
          // topicResults array
          .appendArray(transaction.topicResults, (w, topic) => {
            w.appendString(topic.name)
              // resultsByPartition array
              .appendArray(topic.resultsByPartition, (w, partition) => {
                w.appendInt32(partition.partitionIndex).appendInt16(partition.partitionErrorCode)
              })
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 24, 5, writer.bufferList)
    },
    (err: any) => {
      // Verify error is the right type
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that errors object exists and has the right type
      ok(err.errors && typeof err.errors === 'object', 'Errors object should exist')

      // Verify that the response structure is preserved
      deepStrictEqual(err.response.errorCode, 0, 'Top-level should be success')
      deepStrictEqual(err.response.resultsByTransaction.length, 1)

      // Verify transaction details
      const transaction = err.response.resultsByTransaction[0]
      deepStrictEqual(transaction.transactionalId, 'transaction-123')

      // Verify topic results
      deepStrictEqual(transaction.topicResults.length, 1)
      deepStrictEqual(transaction.topicResults[0].name, 'test-topic')

      // Verify partition results
      const partition = transaction.topicResults[0].resultsByPartition[0]
      deepStrictEqual(partition.partitionIndex, 0)
      deepStrictEqual(partition.partitionErrorCode, 37)

      return true
    }
  )
})
