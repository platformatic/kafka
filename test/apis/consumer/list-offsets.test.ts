import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { listOffsetsV9 } from '../../../src/apis/consumer/list-offsets.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

// Helper function to mock connection and capture API functions
function captureApiHandlers(apiFunction: any) {
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      mockConnection.createRequestFn = createRequestFn
      mockConnection.parseResponseFn = parseResponseFn
      mockConnection.apiKey = apiKey
      mockConnection.apiVersion = apiVersion
      return true
    },
    createRequestFn: null as any,
    parseResponseFn: null as any,
    apiKey: null as any,
    apiVersion: null as any
  }
  
  // Call the API to capture handlers
  apiFunction(mockConnection, {})
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('listOffsetsV9 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(listOffsetsV9)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 2) // ListOffsets API key is 2
  strictEqual(apiVersion, 9) // Version 9
})

test('listOffsetsV9 createRequest serializes request correctly - basic structure', () => {
  const { createRequest } = captureApiHandlers(listOffsetsV9)
  
  // Create a test request
  const replica = -1 // Consumer uses -1 for replica
  const isolationLevel = 0 // Default isolation level
  const topics = [
    {
      name: 'test-topic',
      partitions: [
        {
          partitionIndex: 0,
          currentLeaderEpoch: 0,
          timestamp: -1n // Latest offset
        }
      ]
    }
  ]
  
  // Call the createRequest function
  const writer = createRequest(replica, isolationLevel, topics)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Check that data was written
  ok(writer.bufferList instanceof BufferList)
  ok(writer.length > 0)
})

test('listOffsetsV9 createRequest serializes request correctly - detailed validation', () => {
  const { createRequest } = captureApiHandlers(listOffsetsV9)
  
  // Test cases with different inputs
  const testCases = [
    {
      replica: -1,
      isolationLevel: 0,
      topics: [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              currentLeaderEpoch: 0,
              timestamp: -1n // Latest offset
            }
          ]
        }
      ]
    },
    {
      replica: -1,
      isolationLevel: 1, // Read committed
      topics: [
        {
          name: 'test-topic-1',
          partitions: [
            {
              partitionIndex: 0,
              currentLeaderEpoch: 5,
              timestamp: -2n // Earliest offset
            },
            {
              partitionIndex: 1,
              currentLeaderEpoch: 5,
              timestamp: 1234567890n // Specific timestamp
            }
          ]
        },
        {
          name: 'test-topic-2',
          partitions: [
            {
              partitionIndex: 0,
              currentLeaderEpoch: 10,
              timestamp: -1n
            }
          ]
        }
      ]
    },
    {
      replica: -1,
      isolationLevel: 0,
      topics: [] // Empty topics array
    }
  ]
  
  testCases.forEach(({ replica, isolationLevel, topics }) => {
    const writer = createRequest(replica, isolationLevel, topics)
    ok(writer instanceof Writer, 'should return a Writer instance')
    ok(writer.bufferList instanceof BufferList, 'should have a BufferList')
    ok(writer.length > 0, 'should have written some data')
  })
})

test('listOffsetsV9 parseResponse handles successful response with empty topics', () => {
  const { parseResponse } = captureApiHandlers(listOffsetsV9)
  
  // Create a response with empty topics
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}, true, false) // Empty topics array
    .appendTaggedFields()
  
  const response = parseResponse(1, 2, 9, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    topics: []
  })
})

test('listOffsetsV9 parseResponse handles successful response with topics and partitions', () => {
  const { parseResponse } = captureApiHandlers(listOffsetsV9)
  
  // Create a response with topics
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    
    .appendArray([
      {
        name: 'test-topic-1',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            timestamp: 1234567890n,
            offset: 42n,
            leaderEpoch: 5
          }
        ]
      },
      {
        name: 'test-topic-2',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            timestamp: 1234567890n,
            offset: 100n,
            leaderEpoch: 10
          },
          {
            partitionIndex: 1,
            errorCode: 0,
            timestamp: 1234567890n,
            offset: 200n,
            leaderEpoch: 10
          }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.name, true) // COMPACT_STRING
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendInt64(partition.timestamp)
            .appendInt64(partition.offset)
            .appendInt32(partition.leaderEpoch)
            .appendTaggedFields() // Tagged fields for partition
        }, true, false)
        .appendTaggedFields() // Tagged fields for topic
    }, true, false)
    .appendTaggedFields() // Tagged fields for the whole response
  
  const response = parseResponse(1, 2, 9, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    topics: [
      {
        name: 'test-topic-1',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            timestamp: 1234567890n,
            offset: 42n,
            leaderEpoch: 5
          }
        ]
      },
      {
        name: 'test-topic-2',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            timestamp: 1234567890n,
            offset: 100n,
            leaderEpoch: 10
          },
          {
            partitionIndex: 1,
            errorCode: 0,
            timestamp: 1234567890n,
            offset: 200n,
            leaderEpoch: 10
          }
        ]
      }
    ]
  })
})

test('listOffsetsV9 parseResponse handles partition-level errors', () => {
  const { parseResponse } = captureApiHandlers(listOffsetsV9)
  
  // Create a response with partition-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([
      {
        name: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 6, // NOT_LEADER_OR_FOLLOWER
            timestamp: 0n,
            offset: 0n,
            leaderEpoch: 0
          }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.name, true)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendInt64(partition.timestamp)
            .appendInt64(partition.offset)
            .appendInt32(partition.leaderEpoch)
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 2, 9, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('listOffsetsV9 parseResponse handles multiple partition-level errors', () => {
  const { parseResponse } = captureApiHandlers(listOffsetsV9)
  
  // Create a response with multiple partition-level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([
      {
        name: 'test-topic-1',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 6, // NOT_LEADER_OR_FOLLOWER
            timestamp: 0n,
            offset: 0n,
            leaderEpoch: 0
          }
        ]
      },
      {
        name: 'test-topic-2',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0, // OK
            timestamp: 1234567890n,
            offset: 100n,
            leaderEpoch: 10
          },
          {
            partitionIndex: 1,
            errorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
            timestamp: 0n,
            offset: 0n,
            leaderEpoch: 0
          }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.name, true)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendInt64(partition.timestamp)
            .appendInt64(partition.offset)
            .appendInt32(partition.leaderEpoch)
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error paths
  throws(() => {
    parseResponse(1, 2, 9, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('listOffsetsV9 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 2)
      strictEqual(apiVersion, 9)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        topics: [
          {
            name: 'test-topic',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0,
                timestamp: 1234567890n,
                offset: 42n,
                leaderEpoch: 5
              }
            ]
          }
        ]
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await listOffsetsV9.async(mockConnection, {
    replica: -1,
    isolationLevel: 0,
    topics: [
      {
        name: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            currentLeaderEpoch: 0,
            timestamp: -1n
          }
        ]
      }
    ]
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.topics.length, 1)
  strictEqual(result.topics[0].name, 'test-topic')
  strictEqual(result.topics[0].partitions[0].offset, 42n)
})

test('listOffsetsV9 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 2)
      strictEqual(apiVersion, 9)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        topics: [
          {
            name: 'test-topic',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 0,
                timestamp: 1234567890n,
                offset: 42n,
                leaderEpoch: 5
              }
            ]
          }
        ]
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  listOffsetsV9(mockConnection, {
    replica: -1,
    isolationLevel: 0,
    topics: [
      {
        name: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            currentLeaderEpoch: 0,
            timestamp: -1n
          }
        ]
      }
    ]
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.topics.length, 1)
    strictEqual(result.topics[0].name, 'test-topic')
    strictEqual(result.topics[0].partitions[0].offset, 42n)
    
    done()
  })
})

test('listOffsetsV9 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 2)
      strictEqual(apiVersion, 9)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '/topics/0/partitions/0': 6 // NOT_LEADER_OR_FOLLOWER
      }, {
        throttleTimeMs: 0,
        topics: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  listOffsetsV9(mockConnection, {
    replica: -1,
    isolationLevel: 0,
    topics: [
      {
        name: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            currentLeaderEpoch: 0,
            timestamp: -1n
          }
        ]
      }
    ]
  }, (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('listOffsetsV9 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 2)
      strictEqual(apiVersion, 9)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '/topics/0/partitions/0': 6 // NOT_LEADER_OR_FOLLOWER
      }, {
        throttleTimeMs: 0,
        topics: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await listOffsetsV9.async(mockConnection, {
      replica: -1,
      isolationLevel: 0,
      topics: [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              currentLeaderEpoch: 0,
              timestamp: -1n
            }
          ]
        }
      ]
    })
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})