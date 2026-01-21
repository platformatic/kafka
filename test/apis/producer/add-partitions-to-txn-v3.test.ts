import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { addPartitionsToTxnV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = addPartitionsToTxnV3

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
  const reader = Reader.from(writer)

  // Read the transactions array

  const transactionalId = reader.readString()
  const producerId = reader.readInt64()
  const producerEpoch = reader.readInt16()

  // Read topics array
  const topics = reader.readArray(() => {
    const topicName = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => reader.readInt32(), true, false)

    return { topicName, partitions }
  })

  // Verify transaction details
  deepStrictEqual(transactionalId, 'transaction-123')
  deepStrictEqual(producerId, 1234567890n)
  deepStrictEqual(producerEpoch, 5)

  // Verify topics and partitions
  deepStrictEqual(topics.length, 1, 'Should have 1 topic')
  deepStrictEqual(topics[0].topicName, 'test-topic')
  deepStrictEqual(topics[0].partitions, [0, 1, 2])
})

test('parseResponse correctly processes a successful simple response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          resultsByPartition: [
            {
              partitionIndex: 0,
              partitionErrorCode: 0
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          // resultsByPartition array
          .appendArray(topic.resultsByPartition, (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.partitionErrorCode)
          })
      }
    )

  const response = parseResponse(1, 24, 5, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    resultsByTransaction: [
      {
        transactionalId: '',
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
    .appendArray(
      [
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
        },
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
      ],
      (w, topic) => {
        w.appendString(topic.name)
          // resultsByPartition array
          .appendArray(topic.resultsByPartition, (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.partitionErrorCode)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 24, 3, Reader.from(writer))

  // Verify full response structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    resultsByTransaction: [
      {
        transactionalId: '',
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
          },
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

test('parseResponse handles partition-level error code', () => {
  // Create a response with a partition-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          name: 'test-topic',
          resultsByPartition: [
            {
              partitionIndex: 0,
              partitionErrorCode: 37 // NOT_LEADER_OR_FOLLOWER
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.name)
          // resultsByPartition array
          .appendArray(topic.resultsByPartition, (w, partition) => {
            w.appendInt32(partition.partitionIndex).appendInt16(partition.partitionErrorCode)
          })
      }
    )
    .appendInt8(0) // Root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 24, 3, Reader.from(writer))
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
      deepStrictEqual(transaction.transactionalId, '')

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
