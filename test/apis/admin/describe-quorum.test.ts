import BufferList from 'bl'
import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { describeQuorumV2 } from '../../../src/apis/admin/describe-quorum.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

// Helper function to mock connection and capture API functions
function captureApiHandlers(apiFunction: any) {
  const mockConnection = {
    send: (_apiKey: number, _apiVersion: number, createRequestFn: any, parseResponseFn: any) => {
      mockConnection.createRequestFn = createRequestFn
      mockConnection.parseResponseFn = parseResponseFn
      return true
    },
    createRequestFn: null as any,
    parseResponseFn: null as any
  }
  
  // Call the API to capture handlers
  apiFunction(mockConnection, {})
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn
  }
}

test('describeQuorumV2 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeQuorumV2)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createRequest serializes topics and partitions correctly', () => {
  const { createRequest } = captureApiHandlers(describeQuorumV2)
  
  // Test with empty topics array
  const emptyRequest = createRequest([])
  deepStrictEqual(emptyRequest instanceof Writer, true)
  
  // Test with a single topic and partition
  const singleTopicRequest = createRequest([
    {
      topicName: 'test-topic',
      partitions: [{ partitionIndex: 0 }]
    }
  ])
  deepStrictEqual(singleTopicRequest instanceof Writer, true)
  
  // Test with multiple topics and partitions
  const multipleTopicsRequest = createRequest([
    {
      topicName: 'test-topic-1',
      partitions: [{ partitionIndex: 0 }, { partitionIndex: 1 }]
    },
    {
      topicName: 'test-topic-2',
      partitions: [{ partitionIndex: 2 }]
    }
  ])
  deepStrictEqual(multipleTopicsRequest instanceof Writer, true)
})

test('parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(describeQuorumV2)
  
  // Create a mock successful response
  const writer = Writer.create()
    .appendInt16(0) // No top-level error
    .appendString(null) // No error message
    // Add one topic with one partition
    .appendArray([
      {
        topicName: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            errorMessage: null,
            leaderId: 1,
            leaderEpoch: 5,
            highWatermark: 100n,
            currentVoters: [
              {
                replicaId: 1,
                replicaDirectoryId: '12345678-1234-5678-1234-567812345678',
                logEndOffset: 100n,
                lastFetchTimestamp: 1621234567890n,
                lastCaughtUpTimestamp: 1621234567890n
              }
            ],
            observers: []
          }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.topicName)
        .appendArray(topic.partitions, (w, partition) => {
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
    })
    // Add nodes array
    .appendArray([
      {
        nodeId: 1,
        listeners: [
          {
            name: 'PLAINTEXT',
            host: 'localhost',
            port: 9092
          }
        ]
      }
    ], (w, node) => {
      w.appendInt32(node.nodeId)
        .appendArray(node.listeners, (w, listener) => {
          w.appendString(listener.name)
            .appendString(listener.host)
            .appendUnsignedInt16(listener.port)
        })
    })
  
  const response = parseResponse(1, 55, 2, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.errorMessage, null)
  deepStrictEqual(response.topics.length, 1)
  deepStrictEqual(response.topics[0].topicName, 'test-topic')
  deepStrictEqual(response.topics[0].partitions.length, 1)
  deepStrictEqual(response.topics[0].partitions[0].partitionIndex, 0)
  deepStrictEqual(response.topics[0].partitions[0].leaderId, 1)
  deepStrictEqual(response.topics[0].partitions[0].leaderEpoch, 5)
  deepStrictEqual(response.topics[0].partitions[0].highWatermark, 100n)
  deepStrictEqual(response.topics[0].partitions[0].currentVoters.length, 1)
  deepStrictEqual(response.topics[0].partitions[0].currentVoters[0].replicaId, 1)
  deepStrictEqual(response.topics[0].partitions[0].currentVoters[0].replicaDirectoryId, '12345678-1234-5678-1234-567812345678')
  deepStrictEqual(response.topics[0].partitions[0].observers.length, 0)
  deepStrictEqual(response.nodes.length, 1)
  deepStrictEqual(response.nodes[0].nodeId, 1)
  deepStrictEqual(response.nodes[0].listeners.length, 1)
  deepStrictEqual(response.nodes[0].listeners[0].name, 'PLAINTEXT')
  deepStrictEqual(response.nodes[0].listeners[0].host, 'localhost')
  deepStrictEqual(response.nodes[0].listeners[0].port, 9092)
})

test('parseResponse handles top-level error', () => {
  const { parseResponse } = captureApiHandlers(describeQuorumV2)
  
  // Create a mock response with a top-level error
  const writer = Writer.create()
    .appendInt16(1) // Error code 1
    .appendString('Top-level error') // Error message
    .appendArray([], () => {}) // Empty topics array
    .appendArray([], () => {}) // Empty nodes array
  
  // The parser should throw a ResponseError
  throws(() => {
    parseResponse(1, 55, 2, writer.bufferList)
  }, (error: any) => {
    return error instanceof ResponseError
  })
})

test('parseResponse handles partition-level error', () => {
  const { parseResponse } = captureApiHandlers(describeQuorumV2)
  
  // Create a mock response with a partition-level error
  const writer = Writer.create()
    .appendInt16(0) // No top-level error
    .appendString(null) // No error message
    // Add one topic with one partition that has an error
    .appendArray([
      {
        topicName: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 2, // Partition-level error
            errorMessage: 'Partition error',
            leaderId: -1,
            leaderEpoch: -1,
            highWatermark: -1n,
            currentVoters: [],
            observers: []
          }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.topicName)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendString(partition.errorMessage)
            .appendInt32(partition.leaderId)
            .appendInt32(partition.leaderEpoch)
            .appendInt64(partition.highWatermark)
            .appendArray(partition.currentVoters, () => {})
            .appendArray(partition.observers, () => {})
        })
    })
    .appendArray([], () => {}) // Empty nodes array
  
  // The parser should throw a ResponseError
  throws(() => {
    parseResponse(1, 55, 2, writer.bufferList)
  }, (error: any) => {
    return error instanceof ResponseError
  })
})

test('parseResponse handles complex response with multiple topics and partitions', () => {
  const { parseResponse } = captureApiHandlers(describeQuorumV2)
  
  // Create a mock response with multiple topics and partitions
  const writer = Writer.create()
    .appendInt16(0) // No top-level error
    .appendString(null) // No error message
    // Add multiple topics with multiple partitions
    .appendArray([
      {
        topicName: 'topic-1',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            errorMessage: null,
            leaderId: 1,
            leaderEpoch: 5,
            highWatermark: 100n,
            currentVoters: [
              {
                replicaId: 1,
                replicaDirectoryId: '12345678-1234-5678-1234-567812345678',
                logEndOffset: 100n,
                lastFetchTimestamp: 1621234567890n,
                lastCaughtUpTimestamp: 1621234567890n
              },
              {
                replicaId: 2,
                replicaDirectoryId: '87654321-4321-8765-4321-876543210987',
                logEndOffset: 95n,
                lastFetchTimestamp: 1621234567880n,
                lastCaughtUpTimestamp: 1621234567885n
              }
            ],
            observers: [
              {
                replicaId: 3,
                replicaDirectoryId: '11111111-2222-3333-4444-555555555555',
                logEndOffset: 90n,
                lastFetchTimestamp: 1621234567870n,
                lastCaughtUpTimestamp: 1621234567875n
              }
            ]
          },
          {
            partitionIndex: 1,
            errorCode: 0,
            errorMessage: null,
            leaderId: 2,
            leaderEpoch: 3,
            highWatermark: 50n,
            currentVoters: [
              {
                replicaId: 2,
                replicaDirectoryId: '87654321-4321-8765-4321-876543210987',
                logEndOffset: 50n,
                lastFetchTimestamp: 1621234567850n,
                lastCaughtUpTimestamp: 1621234567855n
              }
            ],
            observers: []
          }
        ]
      },
      {
        topicName: 'topic-2',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            errorMessage: null,
            leaderId: 3,
            leaderEpoch: 1,
            highWatermark: 25n,
            currentVoters: [
              {
                replicaId: 3,
                replicaDirectoryId: '11111111-2222-3333-4444-555555555555',
                logEndOffset: 25n,
                lastFetchTimestamp: 1621234567840n,
                lastCaughtUpTimestamp: 1621234567845n
              }
            ],
            observers: []
          }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.topicName)
        .appendArray(topic.partitions, (w, partition) => {
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
    })
    // Add multiple nodes
    .appendArray([
      {
        nodeId: 1,
        listeners: [
          {
            name: 'PLAINTEXT',
            host: 'broker1',
            port: 9092
          },
          {
            name: 'SSL',
            host: 'broker1',
            port: 9093
          }
        ]
      },
      {
        nodeId: 2,
        listeners: [
          {
            name: 'PLAINTEXT',
            host: 'broker2',
            port: 9092
          }
        ]
      },
      {
        nodeId: 3,
        listeners: [
          {
            name: 'PLAINTEXT',
            host: 'broker3',
            port: 9092
          }
        ]
      }
    ], (w, node) => {
      w.appendInt32(node.nodeId)
        .appendArray(node.listeners, (w, listener) => {
          w.appendString(listener.name)
            .appendString(listener.host)
            .appendUnsignedInt16(listener.port)
        })
    })
  
  const response = parseResponse(1, 55, 2, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.errorMessage, null)
  deepStrictEqual(response.topics.length, 2)
  
  // Verify first topic
  deepStrictEqual(response.topics[0].topicName, 'topic-1')
  deepStrictEqual(response.topics[0].partitions.length, 2)
  
  // Verify first partition of first topic
  deepStrictEqual(response.topics[0].partitions[0].partitionIndex, 0)
  deepStrictEqual(response.topics[0].partitions[0].leaderId, 1)
  deepStrictEqual(response.topics[0].partitions[0].leaderEpoch, 5)
  deepStrictEqual(response.topics[0].partitions[0].highWatermark, 100n)
  deepStrictEqual(response.topics[0].partitions[0].currentVoters.length, 2)
  deepStrictEqual(response.topics[0].partitions[0].observers.length, 1)
  
  // Verify second partition of first topic
  deepStrictEqual(response.topics[0].partitions[1].partitionIndex, 1)
  deepStrictEqual(response.topics[0].partitions[1].leaderId, 2)
  deepStrictEqual(response.topics[0].partitions[1].currentVoters.length, 1)
  deepStrictEqual(response.topics[0].partitions[1].observers.length, 0)
  
  // Verify second topic
  deepStrictEqual(response.topics[1].topicName, 'topic-2')
  deepStrictEqual(response.topics[1].partitions.length, 1)
  deepStrictEqual(response.topics[1].partitions[0].partitionIndex, 0)
  deepStrictEqual(response.topics[1].partitions[0].leaderId, 3)
  
  // Verify nodes
  deepStrictEqual(response.nodes.length, 3)
  deepStrictEqual(response.nodes[0].nodeId, 1)
  deepStrictEqual(response.nodes[0].listeners.length, 2)
  deepStrictEqual(response.nodes[0].listeners[0].name, 'PLAINTEXT')
  deepStrictEqual(response.nodes[0].listeners[0].host, 'broker1')
  deepStrictEqual(response.nodes[0].listeners[0].port, 9092)
  deepStrictEqual(response.nodes[0].listeners[1].name, 'SSL')
  deepStrictEqual(response.nodes[1].nodeId, 2)
  deepStrictEqual(response.nodes[2].nodeId, 3)
})

test('full API end-to-end test with mock connection', () => {
  // Mock connection with a custom send method
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any) => {
      // Verify API key and version
      deepStrictEqual(apiKey, 55)
      deepStrictEqual(apiVersion, 2)
      
      // Create request and verify
      const request = createRequestFn([
        {
          topicName: 'test-topic',
          partitions: [{ partitionIndex: 0 }]
        }
      ])
      deepStrictEqual(request instanceof Writer, true)
      
      // Create a mock successful response
      const writer = Writer.create()
        .appendInt16(0) // No top-level error
        .appendString(null) // No error message
        .appendArray([
          {
            topicName: 'test-topic',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0,
                errorMessage: null,
                leaderId: 1,
                leaderEpoch: 5,
                highWatermark: 100n,
                currentVoters: [
                  {
                    replicaId: 1,
                    replicaDirectoryId: '12345678-1234-5678-1234-567812345678',
                    logEndOffset: 100n,
                    lastFetchTimestamp: 1621234567890n,
                    lastCaughtUpTimestamp: 1621234567890n
                  }
                ],
                observers: []
              }
            ]
          }
        ], (w, topic) => {
          w.appendString(topic.topicName)
            .appendArray(topic.partitions, (w, partition) => {
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
                .appendArray(partition.observers, () => {})
            })
        })
        .appendArray([
          {
            nodeId: 1,
            listeners: [
              {
                name: 'PLAINTEXT',
                host: 'localhost',
                port: 9092
              }
            ]
          }
        ], (w, node) => {
          w.appendInt32(node.nodeId)
            .appendArray(node.listeners, (w, listener) => {
              w.appendString(listener.name)
                .appendString(listener.host)
                .appendUnsignedInt16(listener.port)
            })
        })
      
      // Parse the mock response and verify
      const result = parseResponseFn(1, apiKey, apiVersion, writer.bufferList)
      return Promise.resolve(result)
    }
  }
  
  // Execute the API with the mock connection
  return describeQuorumV2(mockConnection as any, {
    topics: [
      {
        topicName: 'test-topic',
        partitions: [{ partitionIndex: 0 }]
      }
    ]
  }).then(response => {
    // Verify the response
    deepStrictEqual(response.errorCode, 0)
    deepStrictEqual(response.topics.length, 1)
    deepStrictEqual(response.topics[0].topicName, 'test-topic')
    deepStrictEqual(response.topics[0].partitions.length, 1)
    deepStrictEqual(response.topics[0].partitions[0].partitionIndex, 0)
    deepStrictEqual(response.topics[0].partitions[0].leaderId, 1)
    deepStrictEqual(response.nodes.length, 1)
    deepStrictEqual(response.nodes[0].nodeId, 1)
  })
})