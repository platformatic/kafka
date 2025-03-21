import BufferList from 'bl'
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { describeClusterV1 } from '../../../src/apis/admin/describe-cluster.ts'
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

test('describeClusterV1 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeClusterV1)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('describeClusterV1 createRequest serializes parameters correctly', () => {
  const { createRequest } = captureApiHandlers(describeClusterV1)
  
  // Simple test case
  const includeClusterAuthorizedOperations = true
  const endpointType = 1
  
  // Call the function
  const writer = createRequest(includeClusterAuthorizedOperations, endpointType)
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Verify it has content
  ok(writer.bufferList.length > 0)
})

test('describeClusterV1 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(describeClusterV1)
  
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendString(null, true) // errorMessage (null)
    .appendInt8(1) // endpointType
    .appendString('test-cluster', true) // clusterId
    .appendInt32(1) // controllerId
    // Brokers array with 2 elements
    .appendArray([1, 2], (w, _, i) => {
      w.appendInt32(i) // brokerId
        .appendString(`broker-${i}`, true) // host
        .appendInt32(9092 + i) // port
        .appendString(i % 2 === 0 ? 'rack-1' : null, true) // rack (alternating between value and null)
        .appendTaggedFields() // broker tagged fields
    }, true, false)
    .appendInt32(123) // clusterAuthorizedOperations
    .appendTaggedFields() // response tagged fields
  
  const response = parseResponse(1, 60, 1, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    errorCode: 0,
    errorMessage: null,
    endpointType: 1,
    clusterId: 'test-cluster',
    controllerId: 1,
    brokers: [
      {
        brokerId: 0,
        host: 'broker-0',
        port: 9092,
        rack: 'rack-1'
      },
      {
        brokerId: 1,
        host: 'broker-1',
        port: 9093,
        rack: null
      }
    ],
    clusterAuthorizedOperations: 123
  })
})

test('describeClusterV1 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(describeClusterV1)
  
  // Create an error response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(58) // errorCode (SASL_AUTHENTICATION_FAILED)
    .appendString('Authentication failed', true) // errorMessage
    .appendInt8(1) // endpointType
    .appendString('test-cluster', true) // clusterId
    .appendInt32(1) // controllerId
    // Empty brokers array
    .appendArray([], () => {}, true, false)
    .appendInt32(0) // clusterAuthorizedOperations
    .appendTaggedFields() // response tagged fields
  
  // Should throw a ResponseError
  throws(() => {
    parseResponse(1, 60, 1, writer.bufferList)
  }, (err: any) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.errors && Object.keys(err.errors).length > 0, 'should have errors')
    ok(err.response, 'should have a response object')
    strictEqual(err.response.errorCode, 58)
    strictEqual(err.response.errorMessage, 'Authentication failed')
    return true
  })
})

test('describeClusterV1 parseResponse handles empty brokers array', () => {
  const { parseResponse } = captureApiHandlers(describeClusterV1)
  
  // Create a response with empty brokers array
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendString(null, true) // errorMessage (null)
    .appendInt8(1) // endpointType
    .appendString('test-cluster', true) // clusterId
    .appendInt32(1) // controllerId
    // Empty brokers array
    .appendArray([], () => {}, true, false)
    .appendInt32(0) // clusterAuthorizedOperations
    .appendTaggedFields() // response tagged fields
  
  const response = parseResponse(1, 60, 1, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response.brokers, [])
})

test('describeClusterV1 API with Promise', async () => {
  // Create a mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 60)
      strictEqual(apiVersion, 1)
      
      // Call the callback with a successful response
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        errorMessage: null,
        endpointType: 1,
        clusterId: 'test-cluster',
        controllerId: 1,
        brokers: [
          {
            brokerId: 1,
            host: 'broker-1',
            port: 9092,
            rack: 'rack-1'
          }
        ],
        clusterAuthorizedOperations: 123
      }
      
      cb(null, response)
      return true
    }
  }
  
  // Call the API with Promise
  const result = await describeClusterV1.async(mockConnection, {
    includeClusterAuthorizedOperations: true,
    endpointType: 1
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.errorCode, 0)
  strictEqual(result.errorMessage, null)
  strictEqual(result.clusterId, 'test-cluster')
})

test('describeClusterV1 API with callback', (t, done) => {
  // Create a mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 60)
      strictEqual(apiVersion, 1)
      
      // Call the callback with a successful response
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        errorMessage: null,
        endpointType: 1,
        clusterId: 'test-cluster',
        controllerId: 1,
        brokers: [
          {
            brokerId: 1,
            host: 'broker-1',
            port: 9092,
            rack: 'rack-1'
          }
        ],
        clusterAuthorizedOperations: 123
      }
      
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  describeClusterV1(mockConnection, {
    includeClusterAuthorizedOperations: true,
    endpointType: 1
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.errorCode, 0)
    strictEqual(result.errorMessage, null)
    strictEqual(result.clusterId, 'test-cluster')
    
    done()
  })
})

test('describeClusterV1 API error handling', async () => {
  // Create a mock connection that returns an error
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Call the callback with an error
      const error = new Error('Test error')
      cb(error)
      return true
    }
  }
  
  // Test with Promise rejection
  await rejects(async () => {
    await describeClusterV1.async(mockConnection, {
      includeClusterAuthorizedOperations: true,
      endpointType: 1
    })
  }, (err: any) => {
    ok(err instanceof Error)
    strictEqual(err.message, 'Test error')
    return true
  })
  
  // Test with callback error
  let callbackCalled = false
  describeClusterV1(mockConnection, {
    includeClusterAuthorizedOperations: true,
    endpointType: 1
  }, (err, result) => {
    callbackCalled = true
    ok(err instanceof Error)
    strictEqual(err.message, 'Test error')
    strictEqual(result, undefined)
  })
  
  // Verify callback was called
  strictEqual(callbackCalled, true)
})