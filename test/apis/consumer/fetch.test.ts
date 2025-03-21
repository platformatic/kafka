import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, notStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { fetchV17 } from '../../../src/apis/consumer/fetch.ts'
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

test('fetchV17 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(fetchV17)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 1) // Fetch API key is 1
  strictEqual(apiVersion, 17) // Version 17
})

test('fetchV17 createRequest serializes request correctly - basic structure', () => {
  const { createRequest } = captureApiHandlers(fetchV17)
  
  // Create a test request with minimal required parameters
  const maxWaitMs = 500
  const minBytes = 1
  const maxBytes = 1048576 // 1MB
  const isolationLevel = 0 // READ_UNCOMMITTED
  const sessionId = 0
  const sessionEpoch = -1
  const topics = [
    {
      topicId: '12345678-1234-1234-1234-123456789012',
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
  const forgottenTopicsData: any[] = []
  const rackId = ''
  
  // Call the createRequest function
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
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Check that data was written
  ok(writer.bufferList instanceof BufferList)
  ok(writer.length > 0)
})

test('fetchV17 createRequest serializes request correctly - detailed validation', () => {
  const { createRequest } = captureApiHandlers(fetchV17)
  
  // Test cases with different inputs
  const testCases = [
    {
      maxWaitMs: 500,
      minBytes: 1,
      maxBytes: 1048576,
      isolationLevel: 0,
      sessionId: 0,
      sessionEpoch: -1,
      topics: [
        {
          topicId: '12345678-1234-1234-1234-123456789012',
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
      ],
      forgottenTopicsData: [],
      rackId: ''
    },
    {
      maxWaitMs: 1000,
      minBytes: 1024,
      maxBytes: 5242880,
      isolationLevel: 1, // READ_COMMITTED
      sessionId: 1,
      sessionEpoch: 0,
      topics: [
        {
          topicId: '12345678-1234-1234-1234-123456789012',
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 5,
              fetchOffset: 100n,
              lastFetchedEpoch: 4,
              partitionMaxBytes: 1048576
            },
            {
              partition: 1,
              currentLeaderEpoch: 5,
              fetchOffset: 200n,
              lastFetchedEpoch: 4,
              partitionMaxBytes: 1048576
            }
          ]
        },
        {
          topicId: '87654321-4321-4321-4321-210987654321',
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 10,
              fetchOffset: 0n,
              lastFetchedEpoch: 0,
              partitionMaxBytes: 1048576
            }
          ]
        }
      ],
      forgottenTopicsData: [
        {
          topic: '12345678-1234-1234-1234-123456789012',
          partitions: [2]
        }
      ],
      rackId: 'rack-1'
    },
    {
      maxWaitMs: 100,
      minBytes: 1,
      maxBytes: 1048576,
      isolationLevel: 0,
      sessionId: 0,
      sessionEpoch: -1,
      topics: [], // Empty topics
      forgottenTopicsData: [],
      rackId: ''
    }
  ]
  
  testCases.forEach(({ maxWaitMs, minBytes, maxBytes, isolationLevel, sessionId, sessionEpoch, topics, forgottenTopicsData, rackId }) => {
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
    ok(writer instanceof Writer, 'should return a Writer instance')
    ok(writer.bufferList instanceof BufferList, 'should have a BufferList')
    ok(writer.length > 0, 'should have written some data')
  })
})

test('fetchV17 parseResponse handles successful response with empty topics', () => {
  const { parseResponse } = captureApiHandlers(fetchV17)
  
  // Create a response with empty topics
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendInt32(0) // sessionId
    .appendArray([], () => {}, true, false) // Empty responses array
    .appendTaggedFields()
  
  const response = parseResponse(1, 1, 17, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    sessionId: 0,
    responses: []
  })
})

test('fetchV17 parseResponse handles successful response with topics but no records', () => {
  const { parseResponse } = captureApiHandlers(fetchV17)
  
  // Create a response with topics but no records (recordsSize = 1 means no records)
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendInt32(1) // sessionId
    
    .appendArray([
      {
        topicId: '12345678-1234-1234-1234-123456789012',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            highWatermark: 100n,
            lastStableOffset: 100n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1,
            // No records 
          }
        ]
      }
    ], (w, topic) => {
      w.appendUUID(topic.topicId)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendInt64(partition.highWatermark)
            .appendInt64(partition.lastStableOffset)
            .appendInt64(partition.logStartOffset)
            .appendArray(partition.abortedTransactions || [], (w, txn) => {
              w.appendInt64(txn.producerId)
                .appendInt64(txn.firstOffset)
                .appendTaggedFields()
            }, true, false)
            .appendInt32(partition.preferredReadReplica)
            // Add no records by setting recordsSize to 1
            .appendUnsignedVarInt(1)
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  const response = parseResponse(1, 1, 17, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    errorCode: 0,
    sessionId: 1,
    responses: [
      {
        topicId: '12345678-1234-1234-1234-123456789012',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            highWatermark: 100n,
            lastStableOffset: 100n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1
            // No records property because no records were returned
          }
        ]
      }
    ]
  })
})

test('fetchV17 parseResponse handles top-level error', () => {
  const { parseResponse } = captureApiHandlers(fetchV17)
  
  // Create a response with a top-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(58) // errorCode - SASL_AUTHENTICATION_FAILED
    .appendInt32(0) // sessionId
    .appendArray([], () => {}, true, false) // Empty responses array
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 1, 17, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('fetchV17 parseResponse handles partition-level errors', () => {
  const { parseResponse } = captureApiHandlers(fetchV17)
  
  // Create a response with partition-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendInt32(0) // sessionId
    .appendArray([
      {
        topicId: '12345678-1234-1234-1234-123456789012',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 6, // NOT_LEADER_OR_FOLLOWER
            highWatermark: 0n,
            lastStableOffset: 0n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1
          }
        ]
      }
    ], (w, topic) => {
      w.appendUUID(topic.topicId)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendInt64(partition.highWatermark)
            .appendInt64(partition.lastStableOffset)
            .appendInt64(partition.logStartOffset)
            .appendArray(partition.abortedTransactions || [], (w, txn) => {
              w.appendInt64(txn.producerId)
                .appendInt64(txn.firstOffset)
                .appendTaggedFields()
            }, true, false)
            .appendInt32(partition.preferredReadReplica)
            .appendUnsignedVarInt(1) // No records
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 1, 17, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('fetchV17 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 1)
      strictEqual(apiVersion, 17)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        sessionId: 1,
        responses: [
          {
            topicId: '12345678-1234-1234-1234-123456789012',
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
        ]
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await fetchV17.async(mockConnection, {
    maxWaitMs: 500,
    minBytes: 1,
    maxBytes: 1048576,
    isolationLevel: 0,
    sessionId: 0,
    sessionEpoch: -1,
    topics: [
      {
        topicId: '12345678-1234-1234-1234-123456789012',
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
    ],
    forgottenTopicsData: [],
    rackId: ''
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.sessionId, 1)
  strictEqual(result.responses.length, 1)
  strictEqual(result.responses[0].topicId, '12345678-1234-1234-1234-123456789012')
})

test('fetchV17 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 1)
      strictEqual(apiVersion, 17)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        sessionId: 1,
        responses: [
          {
            topicId: '12345678-1234-1234-1234-123456789012',
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
        ]
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  fetchV17(mockConnection, {
    maxWaitMs: 500,
    minBytes: 1,
    maxBytes: 1048576,
    isolationLevel: 0,
    sessionId: 0,
    sessionEpoch: -1,
    topics: [
      {
        topicId: '12345678-1234-1234-1234-123456789012',
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
    ],
    forgottenTopicsData: [],
    rackId: ''
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.sessionId, 1)
    strictEqual(result.responses.length, 1)
    strictEqual(result.responses[0].topicId, '12345678-1234-1234-1234-123456789012')
    
    done()
  })
})

test('fetchV17 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 1)
      strictEqual(apiVersion, 17)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '/responses/0/partitions/0': 6 // NOT_LEADER_OR_FOLLOWER
      }, {
        throttleTimeMs: 0,
        errorCode: 0,
        sessionId: 0,
        responses: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  fetchV17(mockConnection, {
    maxWaitMs: 500,
    minBytes: 1,
    maxBytes: 1048576,
    isolationLevel: 0,
    sessionId: 0,
    sessionEpoch: -1,
    topics: [
      {
        topicId: '12345678-1234-1234-1234-123456789012',
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
    ],
    forgottenTopicsData: [],
    rackId: ''
  }, (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('fetchV17 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 1)
      strictEqual(apiVersion, 17)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '/responses/0/partitions/0': 6 // NOT_LEADER_OR_FOLLOWER
      }, {
        throttleTimeMs: 0,
        errorCode: 0,
        sessionId: 0,
        responses: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await fetchV17.async(mockConnection, {
      maxWaitMs: 500,
      minBytes: 1,
      maxBytes: 1048576,
      isolationLevel: 0,
      sessionId: 0,
      sessionEpoch: -1,
      topics: [
        {
          topicId: '12345678-1234-1234-1234-123456789012',
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
      ],
      forgottenTopicsData: [],
      rackId: ''
    })
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})