import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { type DescribeQuorumResponseVoter } from '../../../src/apis/admin/describe-quorum-v2.ts'
import { Reader, ResponseError, Writer, describeQuorumV2 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeQuorumV2

test('createRequest serializes empty topics array correctly', () => {
  const topics: [] = []

  const writer = createRequest(topics)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicName = reader.readString()
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      return { partitionIndex }
    })
    return { topicName, partitions }
  })

  // Verify serialized data
  deepStrictEqual(topicsArray, [], 'Empty topics array should be serialized correctly')
})

test('createRequest serializes single topic with partitions correctly', () => {
  const topics = [
    {
      topicName: 'test-topic',
      partitions: [
        {
          partitionIndex: 0
        },
        {
          partitionIndex: 1
        }
      ]
    }
  ]

  const writer = createRequest(topics)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicName = reader.readString()
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      return { partitionIndex }
    })
    return { topicName, partitions }
  })

  // Verify serialized data
  deepStrictEqual(
    topicsArray,
    [
      {
        topicName: 'test-topic',
        partitions: [
          {
            partitionIndex: 0
          },
          {
            partitionIndex: 1
          }
        ]
      }
    ],
    'Single topic with partitions should be serialized correctly'
  )
})

test('createRequest serializes multiple topics correctly', () => {
  const topics = [
    {
      topicName: 'topic-1',
      partitions: [
        {
          partitionIndex: 0
        }
      ]
    },
    {
      topicName: 'topic-2',
      partitions: [
        {
          partitionIndex: 0
        },
        {
          partitionIndex: 1
        }
      ]
    }
  ]

  const writer = createRequest(topics)
  const reader = Reader.from(writer)

  // Read topics array
  const topicsArray = reader.readArray(() => {
    const topicName = reader.readString()
    const partitions = reader.readArray(() => {
      const partitionIndex = reader.readInt32()
      return { partitionIndex }
    })
    return { topicName, partitions }
  })

  // Verify multiple topics
  deepStrictEqual(
    topicsArray,
    [
      {
        topicName: 'topic-1',
        partitions: [
          {
            partitionIndex: 0
          }
        ]
      },
      {
        topicName: 'topic-2',
        partitions: [
          {
            partitionIndex: 0
          },
          {
            partitionIndex: 1
          }
        ]
      }
    ],
    'Multiple topics should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no topics and no nodes
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty topics array
    .appendArray([], () => {}) // Empty nodes array
    .appendTaggedFields()

  const response = parseResponse(1, 55, 2, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      errorCode: 0,
      errorMessage: null,
      topics: [],
      nodes: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a response with quorum information', () => {
  // Create directory IDs as UUIDs
  const replicaDirectoryId1 = '12345678-1234-1234-1234-123456789abc'
  const replicaDirectoryId2 = '87654321-4321-4321-4321-cba987654321'

  // Create a successful response with quorum information
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null,
              leaderId: 1,
              leaderEpoch: 5,
              highWatermark: BigInt(100),
              currentVoters: [
                {
                  replicaId: 1,
                  replicaDirectoryId: replicaDirectoryId1,
                  logEndOffset: BigInt(100),
                  lastFetchTimestamp: BigInt(1630000000000),
                  lastCaughtUpTimestamp: BigInt(1630000000000)
                },
                {
                  replicaId: 2,
                  replicaDirectoryId: replicaDirectoryId2,
                  logEndOffset: BigInt(90),
                  lastFetchTimestamp: BigInt(1629900000000),
                  lastCaughtUpTimestamp: BigInt(1629900000000)
                }
              ],
              observers: []
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topicName).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendString(partition.errorMessage)
            .appendInt32(partition.leaderId)
            .appendInt32(partition.leaderEpoch)
            .appendInt64(partition.highWatermark)
            .appendArray(partition.currentVoters, (w, voter) => {
              w.appendInt32(voter.replicaId)
                .appendUUID(voter.replicaDirectoryId)
                .appendInt64(voter.logEndOffset)
                .appendInt64(voter.lastFetchTimestamp)
                .appendInt64(voter.lastCaughtUpTimestamp)
            })
            .appendArray(partition.observers, (w, observer: DescribeQuorumResponseVoter) => {
              w.appendInt32(observer.replicaId)
                .appendUUID(observer.replicaDirectoryId)
                .appendInt64(observer.logEndOffset)
                .appendInt64(observer.lastFetchTimestamp)
                .appendInt64(observer.lastCaughtUpTimestamp)
            })
        })
      }
    )
    .appendArray(
      [
        {
          nodeId: 1,
          listeners: [
            {
              name: 'PLAINTEXT',
              host: 'kafka-1',
              port: 9092
            }
          ]
        },
        {
          nodeId: 2,
          listeners: [
            {
              name: 'PLAINTEXT',
              host: 'kafka-2',
              port: 9092
            }
          ]
        }
      ],
      (w, node) => {
        w.appendInt32(node.nodeId).appendArray(node.listeners, (w, listener) => {
          w.appendString(listener.name).appendString(listener.host).appendUnsignedInt16(listener.port)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 55, 2, Reader.from(writer))

  // Verify topic and partition structure
  deepStrictEqual(response.topics.length, 1, 'Should have one topic')
  deepStrictEqual(response.topics[0].topicName, 'test-topic', 'Topic name should be parsed correctly')
  deepStrictEqual(response.topics[0].partitions.length, 1, 'Should have one partition')

  // Verify partition details
  const partition = response.topics[0].partitions[0]
  deepStrictEqual(partition.partitionIndex, 0, 'Partition index should be parsed correctly')
  deepStrictEqual(partition.leaderId, 1, 'Leader ID should be parsed correctly')
  deepStrictEqual(partition.leaderEpoch, 5, 'Leader epoch should be parsed correctly')
  deepStrictEqual(partition.highWatermark, BigInt(100), 'High watermark should be parsed correctly')

  // Verify voters
  deepStrictEqual(partition.currentVoters.length, 2, 'Should have two voters')
  deepStrictEqual(partition.currentVoters[0].replicaId, 1, 'First voter ID should be parsed correctly')
  deepStrictEqual(
    partition.currentVoters[0].replicaDirectoryId,
    replicaDirectoryId1,
    'First voter directory ID should be parsed correctly'
  )
  deepStrictEqual(
    partition.currentVoters[0].logEndOffset,
    BigInt(100),
    'First voter log end offset should be parsed correctly'
  )

  // Verify nodes
  deepStrictEqual(response.nodes.length, 2, 'Should have two nodes')
  deepStrictEqual(response.nodes[0].nodeId, 1, 'First node ID should be parsed correctly')
  deepStrictEqual(response.nodes[0].listeners.length, 1, 'First node should have one listener')
  deepStrictEqual(response.nodes[0].listeners[0].name, 'PLAINTEXT', 'Listener name should be parsed correctly')
  deepStrictEqual(response.nodes[0].listeners[0].host, 'kafka-1', 'Listener host should be parsed correctly')
  deepStrictEqual(response.nodes[0].listeners[0].port, 9092, 'Listener port should be parsed correctly')
})

test('parseResponse correctly processes a response with observers', () => {
  // Create directory IDs as UUIDs
  const voterDirectoryId = '12345678-1234-1234-1234-123456789abc'
  const observerDirectoryId = '87654321-4321-4321-4321-cba987654321'

  // Create a response with observers
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 0,
              errorMessage: null,
              leaderId: 1,
              leaderEpoch: 5,
              highWatermark: BigInt(100),
              currentVoters: [
                {
                  replicaId: 1,
                  replicaDirectoryId: voterDirectoryId,
                  logEndOffset: BigInt(100),
                  lastFetchTimestamp: BigInt(1630000000000),
                  lastCaughtUpTimestamp: BigInt(1630000000000)
                }
              ],
              observers: [
                {
                  replicaId: 3,
                  replicaDirectoryId: observerDirectoryId,
                  logEndOffset: BigInt(80),
                  lastFetchTimestamp: BigInt(1629800000000),
                  lastCaughtUpTimestamp: BigInt(1629800000000)
                }
              ]
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topicName).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendString(partition.errorMessage)
            .appendInt32(partition.leaderId)
            .appendInt32(partition.leaderEpoch)
            .appendInt64(partition.highWatermark)
            .appendArray(partition.currentVoters, (w, voter) => {
              w.appendInt32(voter.replicaId)
                .appendUUID(voter.replicaDirectoryId)
                .appendInt64(voter.logEndOffset)
                .appendInt64(voter.lastFetchTimestamp)
                .appendInt64(voter.lastCaughtUpTimestamp)
            })
            .appendArray(partition.observers, (w, observer) => {
              w.appendInt32(observer.replicaId)
                .appendUUID(observer.replicaDirectoryId)
                .appendInt64(observer.logEndOffset)
                .appendInt64(observer.lastFetchTimestamp)
                .appendInt64(observer.lastCaughtUpTimestamp)
            })
        })
      }
    )
    .appendArray([], () => {}) // Empty nodes array
    .appendTaggedFields()

  const response = parseResponse(1, 55, 2, Reader.from(writer))

  // Verify observers
  const partition = response.topics[0].partitions[0]
  deepStrictEqual(partition.observers.length, 1, 'Should have one observer')
  deepStrictEqual(partition.observers[0].replicaId, 3, 'Observer ID should be parsed correctly')
  deepStrictEqual(
    partition.observers[0].replicaDirectoryId,
    observerDirectoryId,
    'Observer directory ID should be parsed correctly'
  )
  deepStrictEqual(partition.observers[0].logEndOffset, BigInt(80), 'Observer log end offset should be parsed correctly')
})

test('parseResponse correctly processes a response with multiple listeners per node', () => {
  // Create a response with multiple listeners per node
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty topics array
    .appendArray(
      [
        {
          nodeId: 1,
          listeners: [
            {
              name: 'PLAINTEXT',
              host: 'kafka-1',
              port: 9092
            },
            {
              name: 'SSL',
              host: 'kafka-1',
              port: 9093
            }
          ]
        }
      ],
      (w, node) => {
        w.appendInt32(node.nodeId).appendArray(node.listeners, (w, listener) => {
          w.appendString(listener.name).appendString(listener.host).appendUnsignedInt16(listener.port)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 55, 2, Reader.from(writer))

  // Verify multiple listeners
  deepStrictEqual(response.nodes[0].listeners.length, 2, 'Should have two listeners for the node')
  deepStrictEqual(
    response.nodes[0].listeners.map(l => l.name),
    ['PLAINTEXT', 'SSL'],
    'Listener names should be parsed correctly'
  )
  deepStrictEqual(
    response.nodes[0].listeners.map(l => l.port),
    [9092, 9093],
    'Listener ports should be parsed correctly'
  )
})

test('parseResponse throws ResponseError on top-level error response', () => {
  // Create a response with a top-level error
  const writer = Writer.create()
    .appendInt16(58) // OPERATION_NOT_ATTEMPTED
    .appendString('Operation not attempted') // errorMessage
    .appendArray([], () => {}) // Empty topics array
    .appendArray([], () => {}) // Empty nodes array
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 55, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error details
      deepStrictEqual(err.response.errorCode, 58, 'Error code should be preserved in the response')
      deepStrictEqual(
        err.response.errorMessage,
        'Operation not attempted',
        'Error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse throws ResponseError on partition-level error response', () => {
  // Create a response with a partition-level error
  const writer = Writer.create()
    .appendInt16(0) // No top-level error
    .appendString(null) // No top-level error message
    .appendArray(
      [
        {
          topicName: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              errorCode: 39, // INVALID_TOPIC_EXCEPTION
              errorMessage: 'Invalid topic',
              leaderId: -1,
              leaderEpoch: -1,
              highWatermark: BigInt(-1),
              currentVoters: [],
              observers: []
            }
          ]
        }
      ],
      (w, topic) => {
        w.appendString(topic.topicName).appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendString(partition.errorMessage)
            .appendInt32(partition.leaderId)
            .appendInt32(partition.leaderEpoch)
            .appendInt64(partition.highWatermark)
            .appendArray([], () => {}) // Empty voters
            .appendArray([], () => {}) // Empty observers
        })
      }
    )
    .appendArray([], () => {}) // Empty nodes array
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 55, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify partition-level error
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
