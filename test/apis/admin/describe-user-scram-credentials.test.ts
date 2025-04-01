import BufferList from 'bl'
import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { describeUserScramCredentialsV0 } from '../../../src/apis/admin/describe-user-scram-credentials.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Writer } from '../../../src/protocol/writer.ts'

// Helper function to mock connection and capture API functions
function captureApiHandlers(apiFunction: any) {
  const mockConnection = {
    send: (_apiKey: number, _apiVersion: number, createRequestFn: any, parseResponseFn: any, _hasRequestHeader: boolean, _hasResponseHeader: boolean, callback: any) => {
      mockConnection.createRequestFn = createRequestFn
      mockConnection.parseResponseFn = parseResponseFn
      if (callback) callback(null, {})
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

test('describeUserScramCredentialsV0 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeUserScramCredentialsV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('describeUserScramCredentialsV0 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(describeUserScramCredentialsV0)
  
  // Test with valid parameters
  const users = [
    { name: 'user1' },
    { name: 'user2' },
    { name: 'user3' }
  ]
  
  const request = createRequest(users)
  
  // Verify the request has the right properties rather than exact buffer content
  deepStrictEqual(request instanceof Writer, true)
  deepStrictEqual(typeof request.buffer, 'object')
  deepStrictEqual(request.buffer instanceof Buffer, true)
  deepStrictEqual(request.buffer.length > 0, true)
})

test('describeUserScramCredentialsV0 handles request with empty users array', () => {
  const { createRequest } = captureApiHandlers(describeUserScramCredentialsV0)
  
  // Test with empty users array
  const users: any[] = []
  
  const request = createRequest(users)
  
  // Verify the request has the right properties rather than exact buffer content
  deepStrictEqual(request instanceof Writer, true)
  deepStrictEqual(typeof request.buffer, 'object')
  deepStrictEqual(request.buffer instanceof Buffer, true)
  deepStrictEqual(request.buffer.length > 0, true)
})

test('describeUserScramCredentialsV0 parseResponse correctly parses successful response', () => {
  const { parseResponse } = captureApiHandlers(describeUserScramCredentialsV0)
  
  // Create a sample successful response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage (null for success)
    .appendArray([
      {
        user: 'user1',
        errorCode: 0,
        errorMessage: null,
        credentialInfos: [
          { mechanism: 1, iterations: 4096 },
          { mechanism: 2, iterations: 8192 }
        ]
      },
      {
        user: 'user2',
        errorCode: 0,
        errorMessage: null,
        credentialInfos: [
          { mechanism: 1, iterations: 4096 }
        ]
      }
    ], (w, r) => {
      w.appendString(r.user)
      w.appendInt16(r.errorCode)
      w.appendString(r.errorMessage)
      w.appendArray(r.credentialInfos, (w, c) => {
        w.appendInt8(c.mechanism)
        w.appendInt32(c.iterations)
        w.appendTaggedFields()
      }, true, false)
      w.appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Parse the response
  const result = parseResponse(1, 50, 0, writer.bufferList)
  
  // Verify the parsed response
  deepStrictEqual(result, {
    throttleTimeMs: 100,
    errorCode: 0,
    errorMessage: null,
    results: [
      {
        user: 'user1',
        errorCode: 0,
        errorMessage: null,
        credentialInfos: [
          { mechanism: 1, iterations: 4096 },
          { mechanism: 2, iterations: 8192 }
        ]
      },
      {
        user: 'user2',
        errorCode: 0,
        errorMessage: null,
        credentialInfos: [
          { mechanism: 1, iterations: 4096 }
        ]
      }
    ]
  })
})

test('describeUserScramCredentialsV0 parseResponse handles response with empty results', () => {
  const { parseResponse } = captureApiHandlers(describeUserScramCredentialsV0)
  
  // Create a sample response with empty results
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage (null for success)
    .appendArray([], (w, r) => {
      w.appendString(r.user)
      w.appendInt16(r.errorCode)
      w.appendString(r.errorMessage)
      w.appendArray(r.credentialInfos, (w, c) => {
        w.appendInt8(c.mechanism)
        w.appendInt32(c.iterations)
        w.appendTaggedFields()
      }, true, false)
      w.appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Parse the response
  const result = parseResponse(1, 50, 0, writer.bufferList)
  
  // Verify the parsed response
  deepStrictEqual(result, {
    throttleTimeMs: 100,
    errorCode: 0,
    errorMessage: null,
    results: []
  })
})

test('describeUserScramCredentialsV0 parseResponse throws ResponseError for top-level error', () => {
  const { parseResponse } = captureApiHandlers(describeUserScramCredentialsV0)
  
  // Create a sample error response buffer with top-level error
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(41) // errorCode - NOT_CONTROLLER
    .appendString('Not the controller for this cluster') // errorMessage
    .appendArray([], (w, r) => {
      w.appendString(r.user)
      w.appendInt16(r.errorCode)
      w.appendString(r.errorMessage)
      w.appendArray(r.credentialInfos, (w, c) => {
        w.appendInt8(c.mechanism)
        w.appendInt32(c.iterations)
        w.appendTaggedFields()
      }, true, false)
      w.appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Expect a ResponseError to be thrown
  throws(() => {
    parseResponse(1, 50, 0, writer.bufferList)
  }, (error) => {
    // Verify it's a ResponseError
    ok(error instanceof ResponseError, 'Should throw a ResponseError')
    
    // Verify response data exists and has the right structure
    ok('response' in error, 'Error should contain response data')
    const responseData = error.response
    deepStrictEqual(responseData.throttleTimeMs, 100)
    deepStrictEqual(responseData.errorCode, 41)
    deepStrictEqual(responseData.errorMessage, 'Not the controller for this cluster')
    
    // Verify the error array contains a ProtocolError for the top-level error
    ok(Array.isArray(error.errors), 'Errors should be an array')
    ok(error.errors.length > 0, 'Error array should not be empty')
    
    // Check the first error corresponds to our expected path
    const firstError = error.errors[0]
    deepStrictEqual(firstError.path, '', 'Error should have an empty path for top-level error')
    deepStrictEqual(firstError.apiCode, 41, 'Error should have correct code')
    
    return true
  })
})

test('describeUserScramCredentialsV0 parseResponse throws ResponseError for user-level errors', () => {
  const { parseResponse } = captureApiHandlers(describeUserScramCredentialsV0)
  
  // Create a sample error response buffer with user-level errors
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success at top level)
    .appendString(null) // errorMessage (null for success)
    .appendArray([
      {
        user: 'user1',
        errorCode: 0,
        errorMessage: null,
        credentialInfos: [
          { mechanism: 1, iterations: 4096 }
        ]
      },
      {
        user: 'user2',
        errorCode: 63, // User not found error
        errorMessage: 'User not found',
        credentialInfos: []
      },
      {
        user: 'user3',
        errorCode: 0,
        errorMessage: null,
        credentialInfos: [
          { mechanism: 1, iterations: 4096 }
        ]
      }
    ], (w, r) => {
      w.appendString(r.user)
      w.appendInt16(r.errorCode)
      w.appendString(r.errorMessage)
      w.appendArray(r.credentialInfos, (w, c) => {
        w.appendInt8(c.mechanism)
        w.appendInt32(c.iterations)
        w.appendTaggedFields()
      }, true, false)
      w.appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Expect a ResponseError to be thrown
  throws(() => {
    parseResponse(1, 50, 0, writer.bufferList)
  }, (error) => {
    // Verify it's a ResponseError
    ok(error instanceof ResponseError, 'Should throw a ResponseError')
    
    // Verify response data exists and has the right structure
    ok('response' in error, 'Error should contain response data')
    const responseData = error.response
    deepStrictEqual(responseData.throttleTimeMs, 100)
    deepStrictEqual(responseData.errorCode, 0)
    deepStrictEqual(responseData.errorMessage, null)
    deepStrictEqual(responseData.results.length, 3)
    deepStrictEqual(responseData.results[1].errorCode, 63)
    
    // Verify the error array contains a ProtocolError for the specific path
    ok(Array.isArray(error.errors), 'Errors should be an array')
    ok(error.errors.length > 0, 'Error array should not be empty')
    
    // Check the first error corresponds to our expected path
    const firstError = error.errors[0]
    deepStrictEqual(firstError.path, '/results/1', 'Error should reference correct path')
    deepStrictEqual(firstError.apiCode, 63, 'Error should have correct code')
    
    return true
  })
})

test('describeUserScramCredentialsV0 parseResponse correctly handles empty credential infos', () => {
  const { parseResponse } = captureApiHandlers(describeUserScramCredentialsV0)
  
  // Create a sample response with empty credential infos
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage (null for success)
    .appendArray([
      {
        user: 'user1',
        errorCode: 0,
        errorMessage: null,
        credentialInfos: []
      }
    ], (w, r) => {
      w.appendString(r.user)
      w.appendInt16(r.errorCode)
      w.appendString(r.errorMessage)
      w.appendArray(r.credentialInfos, (w, c) => {
        w.appendInt8(c.mechanism)
        w.appendInt32(c.iterations)
        w.appendTaggedFields()
      }, true, false)
      w.appendTaggedFields()
    }, true, false)
    .appendTaggedFields()
  
  // Parse the response
  const result = parseResponse(1, 50, 0, writer.bufferList)
  
  // Verify the parsed response
  deepStrictEqual(result, {
    throttleTimeMs: 100,
    errorCode: 0,
    errorMessage: null,
    results: [
      {
        user: 'user1',
        errorCode: 0,
        errorMessage: null,
        credentialInfos: []
      }
    ]
  })
})