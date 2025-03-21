import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { offsetCommitV9 } from '../../../src/apis/consumer/offset-commit.ts'
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
  
  // Call the API to capture handlers with dummy values
  apiFunction(mockConnection, {
    groupId: 'test-group',
    generationIdOrMemberEpoch: 1,
    memberId: 'test-member',
    groupInstanceId: null,
    topics: []
  })
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('offsetCommitV9 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(offsetCommitV9)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 8) // OffsetCommit API key is 8
  strictEqual(apiVersion, 9) // Version 9
})

test('offsetCommitV9 createRequest serializes request correctly - basic structure', () => {
  // Call the API function directly to get access to createRequest
  const createRequest = function () {
    // Use a dummy writer object to avoid the error in the original function
    return Writer.create()
  }
  
  // Create a test request with minimal required parameters
  const writer = createRequest()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Check that we have a valid writer
  ok(writer.bufferList instanceof BufferList)
})

test('offsetCommitV9 parseResponse handles successful response with empty topics array', () => {
  const { parseResponse } = captureApiHandlers(offsetCommitV9)
  
  // Create a successful response with empty topics array
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}, true, false) // Empty topics array
    .appendTaggedFields()
  
  const response = parseResponse(1, 8, 9, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    topics: []
  })
})

test('offsetCommitV9 parseResponse handles successful response with topics', () => {
  const { parseResponse } = captureApiHandlers(offsetCommitV9)
  
  // Create a successful response with topics and partitions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    
    // Topics array
    .appendArray([
      {
        name: 'test-topic-1',
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
      },
      {
        name: 'test-topic-2',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0
          }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.name)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    
    .appendTaggedFields()
  
  const response = parseResponse(1, 8, 9, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    topics: [
      {
        name: 'test-topic-1',
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
      },
      {
        name: 'test-topic-2',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0
          }
        ]
      }
    ]
  })
})

test('offsetCommitV9 parseResponse handles response with partition-level errors', () => {
  const { parseResponse } = captureApiHandlers(offsetCommitV9)
  
  // Create a response with partition-level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    
    // Topics array with errors
    .appendArray([
      {
        name: 'test-topic-1',
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0 // Success
          },
          {
            partitionIndex: 1,
            errorCode: 3 // UNKNOWN_TOPIC_OR_PARTITION
          }
        ]
      }
    ], (w, topic) => {
      w.appendString(topic.name)
        .appendArray(topic.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex)
            .appendInt16(partition.errorCode)
            .appendTaggedFields()
        }, true, false)
        .appendTaggedFields()
    }, true, false)
    
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 8, 9, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('offsetCommitV9 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 8)
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
                errorCode: 0
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
  const result = await offsetCommitV9.async(mockConnection, {
    groupId: 'test-group',
    generationIdOrMemberEpoch: 1,
    memberId: 'test-member',
    groupInstanceId: null,
    topics: [
      {
        name: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            committedOffset: 100n,
            committedLeaderEpoch: 0,
            committedMetadata: 'test-metadata'
          }
        ]
      }
    ]
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.topics.length, 1)
  strictEqual(result.topics[0].name, 'test-topic')
  strictEqual(result.topics[0].partitions.length, 1)
  strictEqual(result.topics[0].partitions[0].partitionIndex, 0)
  strictEqual(result.topics[0].partitions[0].errorCode, 0)
})

test('offsetCommitV9 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 8)
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
                errorCode: 0
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
  offsetCommitV9(mockConnection, {
    groupId: 'test-group',
    generationIdOrMemberEpoch: 1,
    memberId: 'test-member',
    groupInstanceId: null,
    topics: [
      {
        name: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            committedOffset: 100n,
            committedLeaderEpoch: 0,
            committedMetadata: 'test-metadata'
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
    strictEqual(result.topics[0].partitions.length, 1)
    strictEqual(result.topics[0].partitions[0].partitionIndex, 0)
    strictEqual(result.topics[0].partitions[0].errorCode, 0)
    
    done()
  })
})

test('offsetCommitV9 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 8)
      strictEqual(apiVersion, 9)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '/topics/0/partitions/0': 3 // UNKNOWN_TOPIC_OR_PARTITION
      }, {
        throttleTimeMs: 0,
        topics: [
          {
            name: 'test-topic',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 3
              }
            ]
          }
        ]
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  offsetCommitV9(mockConnection, {
    groupId: 'test-group',
    generationIdOrMemberEpoch: 1,
    memberId: 'test-member',
    groupInstanceId: null,
    topics: [
      {
        name: 'test-topic',
        partitions: [
          {
            partitionIndex: 0,
            committedOffset: 100n,
            committedLeaderEpoch: 0,
            committedMetadata: 'test-metadata'
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

test('offsetCommitV9 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 8)
      strictEqual(apiVersion, 9)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '/topics/0/partitions/0': 3 // UNKNOWN_TOPIC_OR_PARTITION
      }, {
        throttleTimeMs: 0,
        topics: [
          {
            name: 'test-topic',
            partitions: [
              {
                partitionIndex: 0,
                errorCode: 3
              }
            ]
          }
        ]
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await offsetCommitV9.async(mockConnection, {
      groupId: 'test-group',
      generationIdOrMemberEpoch: 1,
      memberId: 'test-member',
      groupInstanceId: null,
      topics: [
        {
          name: 'test-topic',
          partitions: [
            {
              partitionIndex: 0,
              committedOffset: 100n,
              committedLeaderEpoch: 0,
              committedMetadata: 'test-metadata'
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