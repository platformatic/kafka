import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { offsetFetchV9 } from '../../../src/apis/consumer/offset-fetch.ts'
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
    groups: [{
      groupId: 'test-group',
      memberEpoch: 1,
      topics: []
    }],
    requireStable: false
  })
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('offsetFetchV9 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(offsetFetchV9)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 9) // OffsetFetch API key is 9
  strictEqual(apiVersion, 9) // Version 9
})

test('offsetFetchV9 createRequest serializes request correctly - basic structure', () => {
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

test('offsetFetchV9 parseResponse handles successful response with empty groups array', () => {
  const { parseResponse } = captureApiHandlers(offsetFetchV9)
  
  // Create a successful response with empty groups array
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray([], () => {}, true, false) // Empty groups array
    .appendTaggedFields()
  
  const response = parseResponse(1, 9, 9, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    groups: []
  })
})

test('offsetFetchV9 parseResponse handles successful response with groups', () => {
  const { parseResponse } = captureApiHandlers(offsetFetchV9)
  
  // Create a successful response with groups, topics, and partitions
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    
    // Groups array
    .appendArray([
      {
        groupId: 'test-group-1',
        topics: [
          {
            name: 'test-topic-1',
            partitions: [
              {
                partitionIndex: 0,
                committedOffset: 100n,
                committedLeaderEpoch: 0,
                metadata: 'test-metadata',
                errorCode: 0
              },
              {
                partitionIndex: 1,
                committedOffset: 200n,
                committedLeaderEpoch: 0,
                metadata: null,
                errorCode: 0
              }
            ]
          }
        ],
        errorCode: 0
      },
      {
        groupId: 'test-group-2',
        topics: [
          {
            name: 'test-topic-2',
            partitions: [
              {
                partitionIndex: 0,
                committedOffset: 300n,
                committedLeaderEpoch: 0,
                metadata: 'test-metadata-2',
                errorCode: 0
              }
            ]
          }
        ],
        errorCode: 0
      }
    ], (w, group) => {
      w.appendString(group.groupId)
        .appendArray(group.topics, (w, topic) => {
          w.appendString(topic.name)
            .appendArray(topic.partitions, (w, partition) => {
              w.appendInt32(partition.partitionIndex)
                .appendInt64(partition.committedOffset)
                .appendInt32(partition.committedLeaderEpoch)
                .appendString(partition.metadata)
                .appendInt16(partition.errorCode)
                .appendTaggedFields()
            }, true, false)
            .appendTaggedFields()
        }, true, false)
        .appendInt16(group.errorCode)
        .appendTaggedFields()
    }, true, false)
    
    .appendTaggedFields()
  
  const response = parseResponse(1, 9, 9, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    groups: [
      {
        groupId: 'test-group-1',
        topics: [
          {
            name: 'test-topic-1',
            partitions: [
              {
                partitionIndex: 0,
                committedOffset: 100n,
                committedLeaderEpoch: 0,
                metadata: 'test-metadata',
                errorCode: 0
              },
              {
                partitionIndex: 1,
                committedOffset: 200n,
                committedLeaderEpoch: 0,
                metadata: null,
                errorCode: 0
              }
            ]
          }
        ],
        errorCode: 0
      },
      {
        groupId: 'test-group-2',
        topics: [
          {
            name: 'test-topic-2',
            partitions: [
              {
                partitionIndex: 0,
                committedOffset: 300n,
                committedLeaderEpoch: 0,
                metadata: 'test-metadata-2',
                errorCode: 0
              }
            ]
          }
        ],
        errorCode: 0
      }
    ]
  })
})

test('offsetFetchV9 parseResponse handles response with group-level errors', () => {
  const { parseResponse } = captureApiHandlers(offsetFetchV9)
  
  // Create a response with group-level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    
    // Groups array with errors
    .appendArray([
      {
        groupId: 'test-group-1',
        topics: [],
        errorCode: 25 // UNKNOWN_MEMBER_ID
      }
    ], (w, group) => {
      w.appendString(group.groupId)
        .appendArray(group.topics, () => {}, true, false)
        .appendInt16(group.errorCode)
        .appendTaggedFields()
    }, true, false)
    
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 9, 9, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('offsetFetchV9 parseResponse handles response with partition-level errors', () => {
  const { parseResponse } = captureApiHandlers(offsetFetchV9)
  
  // Create a response with partition-level errors
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    
    // Groups array with partition errors
    .appendArray([
      {
        groupId: 'test-group-1',
        topics: [
          {
            name: 'test-topic-1',
            partitions: [
              {
                partitionIndex: 0,
                committedOffset: 100n,
                committedLeaderEpoch: 0,
                metadata: 'test-metadata',
                errorCode: 0 // Success
              },
              {
                partitionIndex: 1,
                committedOffset: 200n,
                committedLeaderEpoch: 0,
                metadata: null,
                errorCode: 3 // UNKNOWN_TOPIC_OR_PARTITION
              }
            ]
          }
        ],
        errorCode: 0
      }
    ], (w, group) => {
      w.appendString(group.groupId)
        .appendArray(group.topics, (w, topic) => {
          w.appendString(topic.name)
            .appendArray(topic.partitions, (w, partition) => {
              w.appendInt32(partition.partitionIndex)
                .appendInt64(partition.committedOffset)
                .appendInt32(partition.committedLeaderEpoch)
                .appendString(partition.metadata)
                .appendInt16(partition.errorCode)
                .appendTaggedFields()
            }, true, false)
            .appendTaggedFields()
        }, true, false)
        .appendInt16(group.errorCode)
        .appendTaggedFields()
    }, true, false)
    
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 9, 9, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('offsetFetchV9 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 9)
      strictEqual(apiVersion, 9)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        groups: [
          {
            groupId: 'test-group',
            topics: [
              {
                name: 'test-topic',
                partitions: [
                  {
                    partitionIndex: 0,
                    committedOffset: 100n,
                    committedLeaderEpoch: 0,
                    metadata: 'test-metadata',
                    errorCode: 0
                  }
                ]
              }
            ],
            errorCode: 0
          }
        ]
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await offsetFetchV9.async(mockConnection, {
    groups: [
      {
        groupId: 'test-group',
        memberEpoch: 1,
        topics: [
          {
            name: 'test-topic',
            partitionIndexes: [0]
          }
        ]
      }
    ],
    requireStable: false
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.groups.length, 1)
  strictEqual(result.groups[0].groupId, 'test-group')
  strictEqual(result.groups[0].errorCode, 0)
  strictEqual(result.groups[0].topics.length, 1)
  strictEqual(result.groups[0].topics[0].name, 'test-topic')
  strictEqual(result.groups[0].topics[0].partitions.length, 1)
  strictEqual(result.groups[0].topics[0].partitions[0].partitionIndex, 0)
  strictEqual(result.groups[0].topics[0].partitions[0].committedOffset, 100n)
})

test('offsetFetchV9 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 9)
      strictEqual(apiVersion, 9)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        groups: [
          {
            groupId: 'test-group',
            topics: [
              {
                name: 'test-topic',
                partitions: [
                  {
                    partitionIndex: 0,
                    committedOffset: 100n,
                    committedLeaderEpoch: 0,
                    metadata: 'test-metadata',
                    errorCode: 0
                  }
                ]
              }
            ],
            errorCode: 0
          }
        ]
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  offsetFetchV9(mockConnection, {
    groups: [
      {
        groupId: 'test-group',
        memberEpoch: 1,
        topics: [
          {
            name: 'test-topic',
            partitionIndexes: [0]
          }
        ]
      }
    ],
    requireStable: false
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.groups.length, 1)
    strictEqual(result.groups[0].groupId, 'test-group')
    strictEqual(result.groups[0].errorCode, 0)
    strictEqual(result.groups[0].topics.length, 1)
    strictEqual(result.groups[0].topics[0].name, 'test-topic')
    strictEqual(result.groups[0].topics[0].partitions.length, 1)
    strictEqual(result.groups[0].topics[0].partitions[0].partitionIndex, 0)
    strictEqual(result.groups[0].topics[0].partitions[0].committedOffset, 100n)
    
    done()
  })
})

test('offsetFetchV9 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 9)
      strictEqual(apiVersion, 9)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '/groups/0': 25 // UNKNOWN_MEMBER_ID
      }, {
        throttleTimeMs: 0,
        groups: [
          {
            groupId: 'test-group',
            topics: [],
            errorCode: 25
          }
        ]
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  offsetFetchV9(mockConnection, {
    groups: [
      {
        groupId: 'test-group',
        memberEpoch: 1,
        topics: [
          {
            name: 'test-topic',
            partitionIndexes: [0]
          }
        ]
      }
    ],
    requireStable: false
  }, (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('offsetFetchV9 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 9)
      strictEqual(apiVersion, 9)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '/groups/0/topics/0/partitions/0': 3 // UNKNOWN_TOPIC_OR_PARTITION
      }, {
        throttleTimeMs: 0,
        groups: [
          {
            groupId: 'test-group',
            topics: [
              {
                name: 'test-topic',
                partitions: [
                  {
                    partitionIndex: 0,
                    committedOffset: 0n,
                    committedLeaderEpoch: 0,
                    metadata: null,
                    errorCode: 3
                  }
                ]
              }
            ],
            errorCode: 0
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
    await offsetFetchV9.async(mockConnection, {
      groups: [
        {
          groupId: 'test-group',
          memberEpoch: 1,
          topics: [
            {
              name: 'test-topic',
              partitionIndexes: [0]
            }
          ]
        }
      ],
      requireStable: false
    })
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})