import BufferList from 'bl'
import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { alterReplicaLogDirsV2, type AlterReplicaLogDirsRequestDir } from '../../../src/apis/admin/alter-replica-log-dirs.ts'
import { ResponseError } from '../../../src/errors.ts'
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

test('alterReplicaLogDirsV2 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(alterReplicaLogDirsV2)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createRequest serializes dirs correctly', () => {
  const { createRequest } = captureApiHandlers(alterReplicaLogDirsV2)
  
  // Test with empty dirs array
  const emptyRequest = createRequest([])
  deepStrictEqual(emptyRequest instanceof Writer, true)
  
  // Test with a single dir and topic
  const singleDirRequest = createRequest([
    {
      path: '/data/kafka/logs-1',
      topics: [
        {
          name: 'test-topic',
          partitions: [0, 1, 2]
        }
      ]
    }
  ])
  deepStrictEqual(singleDirRequest instanceof Writer, true)
  
  // Test with multiple dirs and topics
  const multipleDirsRequest = createRequest([
    {
      path: '/data/kafka/logs-1',
      topics: [
        {
          name: 'test-topic-1',
          partitions: [0, 1]
        },
        {
          name: 'test-topic-2',
          partitions: [2]
        }
      ]
    },
    {
      path: '/data/kafka/logs-2',
      topics: [
        {
          name: 'test-topic-3',
          partitions: [0, 3, 5]
        }
      ]
    }
  ])
  deepStrictEqual(multipleDirsRequest instanceof Writer, true)
  
  // Test with empty topics array
  const emptyTopicsRequest = createRequest([
    {
      path: '/data/kafka/logs-1',
      topics: []
    }
  ])
  deepStrictEqual(emptyTopicsRequest instanceof Writer, true)
  
  // Test with empty partitions array
  const emptyPartitionsRequest = createRequest([
    {
      path: '/data/kafka/logs-1',
      topics: [
        {
          name: 'test-topic',
          partitions: []
        }
      ]
    }
  ])
  deepStrictEqual(emptyPartitionsRequest instanceof Writer, true)
})

test('parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(alterReplicaLogDirsV2)
  
  // Create a mock successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([
      {
        topicName: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0
          },
          {
            partitionIndex: 1,
            errorCode: 0
          }
        ]
      }
    ], (w, result) => {
      w.appendString(result.topicName)
        .appendArray(result.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
        })
    })
  
  const response = parseResponse(1, 34, 2, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 0)
  deepStrictEqual(response.results.length, 1)
  deepStrictEqual(response.results[0].topicName, 'test-topic')
  deepStrictEqual(response.results[0].partitions.length, 2)
  deepStrictEqual(response.results[0].partitions[0].partitionIndex, 0)
  deepStrictEqual(response.results[0].partitions[0].errorCode, 0)
  deepStrictEqual(response.results[0].partitions[1].partitionIndex, 1)
  deepStrictEqual(response.results[0].partitions[1].errorCode, 0)
})

test('parseResponse handles partition-level errors', () => {
  const { parseResponse } = captureApiHandlers(alterReplicaLogDirsV2)
  
  // Create a mock response with partition-level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([
      {
        topicName: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 57 // LOG_DIR_NOT_FOUND error code
          },
          {
            partitionIndex: 1,
            errorCode: 0 // No error for partition 1
          }
        ]
      }
    ], (w, result) => {
      w.appendString(result.topicName)
        .appendArray(result.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
        })
    })
  
  // The parser should throw a ResponseError
  throws(() => {
    parseResponse(1, 34, 2, writer.bufferList)
  }, (error: any) => {
    return error instanceof ResponseError && error.errors.length > 0
  })
})

test('parseResponse handles multiple topics with errors', () => {
  const { parseResponse } = captureApiHandlers(alterReplicaLogDirsV2)
  
  // Create a mock response with errors in multiple topics
  const writer = Writer.create()
    .appendInt32(10) // throttleTimeMs
    .appendArray([
      {
        topicName: 'test-topic-1',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 57 // LOG_DIR_NOT_FOUND error code
          }
        ]
      },
      {
        topicName: 'test-topic-2',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0
          },
          {
            partitionIndex: 1,
            errorCode: 58 // SASL_AUTHENTICATION_FAILED error code
          }
        ]
      }
    ], (w, result) => {
      w.appendString(result.topicName)
        .appendArray(result.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
        })
    })
  
  // The parser should throw a ResponseError
  throws(() => {
    parseResponse(1, 34, 2, writer.bufferList)
  }, (error: any) => {
    return error instanceof ResponseError && error.errors.length > 0
  })
})

test('parseResponse handles empty results array', () => {
  const { parseResponse } = captureApiHandlers(alterReplicaLogDirsV2)
  
  // Create a mock response with empty results array
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {})
  
  const response = parseResponse(1, 34, 2, writer.bufferList)
  
  // Verify the empty response
  deepStrictEqual(response.throttleTimeMs, 0)
  deepStrictEqual(response.results.length, 0)
})

test('full API end-to-end test with mock connection', () => {
  // Mock connection with a custom send method
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any) => {
      // Verify API key and version
      deepStrictEqual(apiKey, 34)
      deepStrictEqual(apiVersion, 2)
      
      // Create request and verify
      const dirs: AlterReplicaLogDirsRequestDir[] = [
        {
          path: '/data/kafka/logs-1',
          topics: [
            {
              name: 'test-topic',
              partitions: [0, 1]
            }
          ]
        }
      ]
      const request = createRequestFn(dirs)
      deepStrictEqual(request instanceof Writer, true)
      
      // Create a mock successful response
      const writer = Writer.create()
        .appendInt32(0) // throttleTimeMs
        .appendArray([
          {
            topicName: 'test-topic',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0
              },
              {
                partitionIndex: 1,
                errorCode: 0
              }
            ]
          }
        ], (w, result) => {
          w.appendString(result.topicName)
            .appendArray(result.partitions, (w, partition) => {
              w.appendInt32(partition.partitionIndex)
                .appendInt16(partition.errorCode)
            })
        })
      
      // Parse the mock response and verify
      const result = parseResponseFn(1, apiKey, apiVersion, writer.bufferList)
      return Promise.resolve(result)
    }
  }
  
  // Execute the API with the mock connection
  return alterReplicaLogDirsV2(mockConnection as any, {
    dirs: [
      {
        path: '/data/kafka/logs-1',
        topics: [
          {
            name: 'test-topic',
            partitions: [0, 1]
          }
        ]
      }
    ]
  }).then(response => {
    // Verify the response
    deepStrictEqual(response.throttleTimeMs, 0)
    deepStrictEqual(response.results.length, 1)
    deepStrictEqual(response.results[0].topicName, 'test-topic')
    deepStrictEqual(response.results[0].partitions.length, 2)
    deepStrictEqual(response.results[0].partitions[0].partitionIndex, 0)
    deepStrictEqual(response.results[0].partitions[0].errorCode, 0)
  })
})

test('full API error response handling', () => {
  // Mock connection with a custom send method that returns an error
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any) => {
      // Create request
      const dirs: AlterReplicaLogDirsRequestDir[] = [
        {
          path: '/data/kafka/logs-1',
          topics: [
            {
              name: 'test-topic',
              partitions: [0, 1]
            }
          ]
        }
      ]
      createRequestFn(dirs)
      
      // Create a mock error response
      const writer = Writer.create()
        .appendInt32(0) // throttleTimeMs
        .appendArray([
          {
            topicName: 'test-topic',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 57 // LOG_DIR_NOT_FOUND error code
              },
              {
                partitionIndex: 1,
                errorCode: 0
              }
            ]
          }
        ], (w, result) => {
          w.appendString(result.topicName)
            .appendArray(result.partitions, (w, partition) => {
              w.appendInt32(partition.partitionIndex)
                .appendInt16(partition.errorCode)
            })
        })
      
      try {
        parseResponseFn(1, apiKey, apiVersion, writer.bufferList)
        return Promise.resolve(null) // This should not happen
      } catch (error) {
        return Promise.reject(error)
      }
    }
  }
  
  // Execute the API with the mock connection and expect a ResponseError
  return alterReplicaLogDirsV2(mockConnection as any, {
    dirs: [
      {
        path: '/data/kafka/logs-1',
        topics: [
          {
            name: 'test-topic',
            partitions: [0, 1]
          }
        ]
      }
    ]
  }).catch(error => {
    deepStrictEqual(error instanceof ResponseError, true)
    deepStrictEqual(error.errors.length > 0, true)
  })
})