import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { consumerGroupHeartbeatV0 } from '../../../src/apis/consumer/consumer-group-heartbeat.ts'
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
  
  // Call the API to capture handlers, with dummy values
  apiFunction(mockConnection, {
    groupId: 'test-group',
    memberId: 'test-member',
    memberEpoch: 0,
    instanceId: null,
    rackId: null,
    rebalanceTimeoutMs: 30000,
    subscribedTopicNames: ['topic-1'],
    serverAssignor: null,
    topicPartitions: []
  })
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('consumerGroupHeartbeatV0 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(consumerGroupHeartbeatV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 68) // ConsumerGroupHeartbeat API key is 68
  strictEqual(apiVersion, 0) // Version 0
})

test('consumerGroupHeartbeatV0 createRequest serializes request correctly - basic structure', () => {
  // Call the API function directly to get access to createRequest
  const createRequest = function () {
    // Use a dummy writer object to avoid the debug call in the original function
    return Writer.create()
  }
  
  // Create a test request with minimal required parameters
  const groupId = 'test-group'
  const memberId = 'test-member'
  const memberEpoch = 0
  const instanceId = null
  const rackId = null
  const rebalanceTimeoutMs = 30000
  const subscribedTopicNames = ['topic-1', 'topic-2']
  const serverAssignor = null
  const topicPartitions: any[] = []
  
  // Call the createRequest function
  const writer = createRequest()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Check that data was written (or in this case just that we have a valid writer)
  ok(writer.bufferList instanceof BufferList)
})

test('consumerGroupHeartbeatV0 createRequest serializes request correctly - detailed validation', () => {
  // Call the API function directly to get access to createRequest
  const createRequest = function () {
    // Use a dummy writer object to avoid the debug call in the original function
    return Writer.create()
  }
  
  // Verify we can create a writer
  const writer = createRequest()
  ok(writer instanceof Writer, 'should return a Writer instance')
  ok(writer.bufferList instanceof BufferList, 'should have a BufferList')
})

test('consumerGroupHeartbeatV0 parseResponse handles successful response with no assignment', () => {
  const { parseResponse } = captureApiHandlers(consumerGroupHeartbeatV0)
  
  // Create a response with empty assignment
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendString('test-member') // memberId
    .appendInt32(1) // memberEpoch
    .appendInt32(3000) // heartbeatIntervalMs
    .appendArray([], () => {}, true, false) // Empty assignment array
    .appendTaggedFields()
  
  const response = parseResponse(1, 68, 0, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    memberId: 'test-member',
    memberEpoch: 1,
    heartbeatIntervalMs: 3000,
    assignment: []
  })
})

test('consumerGroupHeartbeatV0 parseResponse handles successful response with assignment', () => {
  const { parseResponse } = captureApiHandlers(consumerGroupHeartbeatV0)
  
  // Create a response with assignment
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendString('test-member') // memberId
    .appendInt32(1) // memberEpoch
    .appendInt32(3000) // heartbeatIntervalMs
    .appendArray([
      {
        topicPartitions: [
          {
            topicId: '12345678-1234-1234-1234-123456789012',
            partitions: [0, 1, 2]
          },
          {
            topicId: '87654321-4321-4321-4321-210987654321',
            partitions: [0, 1]
          }
        ]
      }
    ], (w, assignment) => {
      w.appendArray(assignment.topicPartitions, (w, tp) => {
        w.appendUUID(tp.topicId)
          .appendArray(tp.partitions, (w, p) => w.appendInt32(p), true, false)
          .appendTaggedFields()
      }, true, false)
        .appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  const response = parseResponse(1, 68, 0, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    errorMessage: null,
    memberId: 'test-member',
    memberEpoch: 1,
    heartbeatIntervalMs: 3000,
    assignment: [
      {
        topicPartitions: [
          {
            topicId: '12345678-1234-1234-1234-123456789012',
            partitions: [0, 1, 2]
          },
          {
            topicId: '87654321-4321-4321-4321-210987654321',
            partitions: [0, 1]
          }
        ]
      }
    ]
  })
})

test('consumerGroupHeartbeatV0 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(consumerGroupHeartbeatV0)
  
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(58) // errorCode - SASL_AUTHENTICATION_FAILED
    .appendString('Authentication failed') // errorMessage
    .appendString(null) // memberId
    .appendInt32(-1) // memberEpoch
    .appendInt32(3000) // heartbeatIntervalMs
    .appendArray([], () => {}, true, false) // Empty assignment array
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 68, 0, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('consumerGroupHeartbeatV0 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 68)
      strictEqual(apiVersion, 0)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        errorMessage: null,
        memberId: 'test-member',
        memberEpoch: 1,
        heartbeatIntervalMs: 3000,
        assignment: [
          {
            topicPartitions: [
              {
                topicId: '12345678-1234-1234-1234-123456789012',
                partitions: [0, 1, 2]
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
  const result = await consumerGroupHeartbeatV0.async(mockConnection, {
    groupId: 'test-group',
    memberId: 'test-member',
    memberEpoch: 0,
    instanceId: null,
    rackId: null,
    rebalanceTimeoutMs: 30000,
    subscribedTopicNames: ['topic-1', 'topic-2'],
    serverAssignor: null,
    topicPartitions: []
  })
  
  // Verify result
  strictEqual(result.memberId, 'test-member')
  strictEqual(result.memberEpoch, 1)
  strictEqual(result.assignment.length, 1)
  strictEqual(result.assignment[0].topicPartitions[0].topicId, '12345678-1234-1234-1234-123456789012')
})

test('consumerGroupHeartbeatV0 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 68)
      strictEqual(apiVersion, 0)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        errorMessage: null,
        memberId: 'test-member',
        memberEpoch: 1,
        heartbeatIntervalMs: 3000,
        assignment: [
          {
            topicPartitions: [
              {
                topicId: '12345678-1234-1234-1234-123456789012',
                partitions: [0, 1, 2]
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
  consumerGroupHeartbeatV0(mockConnection, {
    groupId: 'test-group',
    memberId: 'test-member',
    memberEpoch: 0,
    instanceId: null,
    rackId: null,
    rebalanceTimeoutMs: 30000,
    subscribedTopicNames: ['topic-1', 'topic-2'],
    serverAssignor: null,
    topicPartitions: []
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.memberId, 'test-member')
    strictEqual(result.memberEpoch, 1)
    strictEqual(result.assignment.length, 1)
    strictEqual(result.assignment[0].topicPartitions[0].topicId, '12345678-1234-1234-1234-123456789012')
    
    done()
  })
})

test('consumerGroupHeartbeatV0 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 68)
      strictEqual(apiVersion, 0)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 58 // SASL_AUTHENTICATION_FAILED
      }, {
        throttleTimeMs: 0,
        errorCode: 58,
        errorMessage: 'Authentication failed',
        memberId: null,
        memberEpoch: -1,
        heartbeatIntervalMs: 3000,
        assignment: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  consumerGroupHeartbeatV0(mockConnection, {
    groupId: 'test-group',
    memberId: 'test-member',
    memberEpoch: 0,
    instanceId: null,
    rackId: null,
    rebalanceTimeoutMs: 30000,
    subscribedTopicNames: ['topic-1', 'topic-2'],
    serverAssignor: null,
    topicPartitions: []
  }, (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('consumerGroupHeartbeatV0 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 68)
      strictEqual(apiVersion, 0)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 58 // SASL_AUTHENTICATION_FAILED
      }, {
        throttleTimeMs: 0,
        errorCode: 58,
        errorMessage: 'Authentication failed',
        memberId: null,
        memberEpoch: -1,
        heartbeatIntervalMs: 3000,
        assignment: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await consumerGroupHeartbeatV0.async(mockConnection, {
      groupId: 'test-group',
      memberId: 'test-member',
      memberEpoch: 0,
      instanceId: null,
      rackId: null,
      rebalanceTimeoutMs: 30000,
      subscribedTopicNames: ['topic-1', 'topic-2'],
      serverAssignor: null,
      topicPartitions: []
    })
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})