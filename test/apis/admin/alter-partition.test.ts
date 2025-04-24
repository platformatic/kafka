import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { alterPartitionV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = alterPartitionV3

test('createRequest serializes basic parameters correctly', () => {
  const brokerId = 1
  const brokerEpoch = BigInt(42)
  const topics = [
    {
      topicId: '12345678-1234-1234-1234-123456789abc',
      partitions: [
        {
          partitionIndex: 0,
          leaderEpoch: 5,
          newIsrWithEpochs: [
            {
              brokerId: 1,
              brokerEpoch: BigInt(42)
            },
            {
              brokerId: 2,
              brokerEpoch: BigInt(41)
            }
          ],
          leaderRecoveryState: 0,
          partitionEpoch: 10
        }
      ]
    }
  ]

  const writer = createRequest(brokerId, brokerEpoch, topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read broker ID and epoch
  const serializedBrokerId = reader.readInt32()
  const serializedBrokerEpoch = reader.readInt64()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicId = reader.readString()

    // Read partitions array
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const leaderEpoch = reader.readInt32()

      // Read newIsrWithEpochs array
      const newIsrWithEpochs = reader.readArray(() => {
        const brokerId = reader.readInt32()
        const brokerEpoch = reader.readInt64()
        return { brokerId, brokerEpoch }
      })

      const leaderRecoveryState = reader.readInt8()
      const partitionEpoch = reader.readInt32()

      return {
        partitionIndex,
        leaderEpoch,
        newIsrWithEpochs,
        leaderRecoveryState,
        partitionEpoch
      }
    })

    return { topicId, partitions }
  })

  // Verify serialized data
  deepStrictEqual(
    {
      brokerId: serializedBrokerId,
      brokerEpoch: serializedBrokerEpoch,
      topics: topicsArray
    },
    {
      brokerId: 1,
      brokerEpoch: BigInt(42),
      topics: [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              leaderEpoch: 5,
              newIsrWithEpochs: [
                {
                  brokerId: 1,
                  brokerEpoch: BigInt(42)
                },
                {
                  brokerId: 2,
                  brokerEpoch: BigInt(41)
                }
              ],
              leaderRecoveryState: 0,
              partitionEpoch: 10
            }
          ]
        }
      ]
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes multiple topics correctly', () => {
  const brokerId = 1
  const brokerEpoch = BigInt(42)
  const topics = [
    {
      topicId: '12345678-1234-1234-1234-123456789abc',
      partitions: [
        {
          partitionIndex: 0,
          leaderEpoch: 5,
          newIsrWithEpochs: [
            {
              brokerId: 1,
              brokerEpoch: BigInt(42)
            }
          ],
          leaderRecoveryState: 0,
          partitionEpoch: 10
        }
      ]
    },
    {
      topicId: '87654321-4321-4321-4321-cba987654321',
      partitions: [
        {
          partitionIndex: 0,
          leaderEpoch: 3,
          newIsrWithEpochs: [
            {
              brokerId: 2,
              brokerEpoch: BigInt(41)
            }
          ],
          leaderRecoveryState: 0,
          partitionEpoch: 8
        }
      ]
    }
  ]

  const writer = createRequest(brokerId, brokerEpoch, topics)
  const reader = Reader.from(writer)

  // Skip broker ID and epoch
  reader.readInt32()
  reader.readInt64()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicId = reader.readString()

    // Read partitions array (simplify for this test)
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      reader.readInt32() // leaderEpoch

      // Skip newIsrWithEpochs array details
      reader.readArray(() => {
        reader.readInt32() // brokerId
        reader.readInt64() // brokerEpoch
        return {}
      })

      reader.readInt8() // leaderRecoveryState
      reader.readInt32() // partitionEpoch

      return { partitionIndex }
    })

    return { topicId, partitions }
  })

  // Verify topic IDs in the serialized data
  deepStrictEqual(
    [topicsArray[0].topicId, topicsArray[1].topicId],
    ['12345678-1234-1234-1234-123456789abc', '87654321-4321-4321-4321-cba987654321'],
    'Should correctly serialize multiple topic IDs'
  )
})

test('createRequest serializes multiple partitions correctly', () => {
  const brokerId = 1
  const brokerEpoch = BigInt(42)
  const topics = [
    {
      topicId: '12345678-1234-1234-1234-123456789abc',
      partitions: [
        {
          partitionIndex: 0,
          leaderEpoch: 5,
          newIsrWithEpochs: [
            {
              brokerId: 1,
              brokerEpoch: BigInt(42)
            }
          ],
          leaderRecoveryState: 0,
          partitionEpoch: 10
        },
        {
          partitionIndex: 1,
          leaderEpoch: 4,
          newIsrWithEpochs: [
            {
              brokerId: 2,
              brokerEpoch: BigInt(41)
            }
          ],
          leaderRecoveryState: 0,
          partitionEpoch: 9
        }
      ]
    }
  ]

  const writer = createRequest(brokerId, brokerEpoch, topics)
  const reader = Reader.from(writer)

  // Skip broker ID and epoch
  reader.readInt32()
  reader.readInt64()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    reader.readString() // topicId

    // Read partitions array to verify partition indices
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      const leaderEpoch = reader.readInt32()

      // Skip ISR details
      reader.readArray(() => {
        reader.readInt32() // brokerId
        reader.readInt64() // brokerEpoch
        return {}
      })

      reader.readInt8() // leaderRecoveryState
      reader.readInt32() // partitionEpoch

      return { partitionIndex, leaderEpoch }
    })

    return { partitions }
  })

  // Verify partition indices and leader epochs
  deepStrictEqual(
    topicsArray[0].partitions.map(p => ({ partitionIndex: p.partitionIndex, leaderEpoch: p.leaderEpoch })),
    [
      { partitionIndex: 0, leaderEpoch: 5 },
      { partitionIndex: 1, leaderEpoch: 4 }
    ],
    'Should correctly serialize multiple partitions with different indices and epochs'
  )
})

test('createRequest serializes multiple ISR entries correctly', () => {
  const brokerId = 1
  const brokerEpoch = BigInt(42)
  const topics = [
    {
      topicId: '12345678-1234-1234-1234-123456789abc',
      partitions: [
        {
          partitionIndex: 0,
          leaderEpoch: 5,
          newIsrWithEpochs: [
            {
              brokerId: 1,
              brokerEpoch: BigInt(42)
            },
            {
              brokerId: 2,
              brokerEpoch: BigInt(41)
            },
            {
              brokerId: 3,
              brokerEpoch: BigInt(40)
            }
          ],
          leaderRecoveryState: 0,
          partitionEpoch: 10
        }
      ]
    }
  ]

  const writer = createRequest(brokerId, brokerEpoch, topics)
  const reader = Reader.from(writer)

  // Skip broker ID and epoch
  reader.readInt32()
  reader.readInt64()

  // Read topics array
  const topicsArray = reader.readArray(() => {
    reader.readString() // topicId

    // Read partitions array
    const partitions = reader.readArray(() => {
      reader.readInt32() // partitionIndex
      reader.readInt32() // leaderEpoch

      // Read ISR entries
      const newIsrWithEpochs = reader.readArray(() => {
        const brokerId = reader.readInt32()
        const brokerEpoch = reader.readInt64()
        return { brokerId, brokerEpoch }
      })

      reader.readInt8() // leaderRecoveryState
      reader.readInt32() // partitionEpoch

      return { newIsrWithEpochs }
    })

    return { partitions }
  })

  // Verify ISR entries
  deepStrictEqual(
    topicsArray[0].partitions[0].newIsrWithEpochs,
    [
      {
        brokerId: 1,
        brokerEpoch: BigInt(42)
      },
      {
        brokerId: 2,
        brokerEpoch: BigInt(41)
      },
      {
        brokerId: 3,
        brokerEpoch: BigInt(40)
      }
    ],
    'Should correctly serialize multiple ISR entries'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendArray(
      [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              leaderId: 1,
              leaderEpoch: 5,
              isr: 3, // This seems like it should be an array in the schema but is an int in the code
              leaderRecoveryState: 0,
              partitionEpoch: 10
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topicId).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendInt32(partition.leaderId)
            .appendInt32(partition.leaderEpoch)
            .appendInt32(partition.isr)
            .appendInt8(partition.leaderRecoveryState)
            .appendInt32(partition.partitionEpoch)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 56, 3, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      topics: [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              leaderId: 1,
              leaderEpoch: 5,
              isr: 3,
              leaderRecoveryState: 0,
              partitionEpoch: 10
            }
          ]
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse handles global error correctly', () => {
  // Create a response with a global error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(41) // errorCode - CLUSTER_AUTHORIZATION_FAILED
    .appendArray([], () => {}) // Empty topics array
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 56, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(
        err.response,
        {
          throttleTimeMs: 0,
          errorCode: 41,
          topics: []
        },
        'Error response should preserve the original response structure'
      )

      return true
    }
  )
})

test('parseResponse handles partition level errors correctly', () => {
  // Create a response with partition level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // No global error
    .appendArray(
      [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 9, // REPLICA_NOT_AVAILABLE
              leaderId: -1,
              leaderEpoch: -1,
              isr: 0,
              leaderRecoveryState: 0,
              partitionEpoch: 10
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topicId).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendInt32(partition.leaderId)
            .appendInt32(partition.leaderEpoch)
            .appendInt32(partition.isr)
            .appendInt8(partition.leaderRecoveryState)
            .appendInt32(partition.partitionEpoch)
        })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 56, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response contains the partition error
      deepStrictEqual(
        err.response.topics[0].partitions[0].errorCode,
        9,
        'Error response should contain partition level error code'
      )

      return true
    }
  )
})

test('parseResponse handles mixed partition successes and errors', () => {
  // Create a response with mixed partition results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // No global error
    .appendArray(
      [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              leaderId: 1,
              leaderEpoch: 5,
              isr: 3,
              leaderRecoveryState: 0,
              partitionEpoch: 10
            },
            {
              partitionIndex: 1,
              errorCode: 9, // REPLICA_NOT_AVAILABLE
              leaderId: -1,
              leaderEpoch: -1,
              isr: 0,
              leaderRecoveryState: 0,
              partitionEpoch: 10
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topicId).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendInt32(partition.leaderId)
            .appendInt32(partition.leaderEpoch)
            .appendInt32(partition.isr)
            .appendInt8(partition.leaderRecoveryState)
            .appendInt32(partition.partitionEpoch)
        })
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 56, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify response contains both successful and error partitions
      deepStrictEqual(
        err.response.topics[0].partitions.map((p: Record<string, number>) => ({
          partitionIndex: p.partitionIndex,
          errorCode: p.errorCode
        })),
        [
          { partitionIndex: 0, errorCode: 0 },
          { partitionIndex: 1, errorCode: 9 }
        ],
        'Response should contain both successful and error partitions'
      )

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs)
    .appendInt16(0) // No error
    .appendArray(
      [
        {
          topicId: '12345678-1234-1234-1234-123456789abc',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              leaderId: 1,
              leaderEpoch: 5,
              isr: 3,
              leaderRecoveryState: 0,
              partitionEpoch: 10
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topicId).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendInt32(partition.leaderId)
            .appendInt32(partition.leaderEpoch)
            .appendInt32(partition.isr)
            .appendInt8(partition.leaderRecoveryState)
            .appendInt32(partition.partitionEpoch)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 56, 3, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
