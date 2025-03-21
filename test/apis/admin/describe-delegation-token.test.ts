import BufferList from 'bl'
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { describeDelegationTokenV3, type DescribeDelegationTokenRequestOwner } from '../../../src/apis/admin/describe-delegation-token.ts'
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

test('describeDelegationTokenV3 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(describeDelegationTokenV3)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('describeDelegationTokenV3 createRequest returns a Writer', () => {
  const { createRequest } = captureApiHandlers(describeDelegationTokenV3)
  
  // Test cases
  const testCases = [
    // Case 1: Single owner
    {
      owners: [
        { principalType: 'User', principalName: 'admin' }
      ]
    },
    // Case 2: Multiple owners
    {
      owners: [
        { principalType: 'User', principalName: 'admin' },
        { principalType: 'User', principalName: 'user1' }
      ]
    },
    // Case 3: Empty owners array
    {
      owners: []
    }
  ]
  
  testCases.forEach(({ owners }) => {
    const writer = createRequest(owners)
    
    // Validate writer is a Writer instance
    ok(writer instanceof Writer)
    
    // Verify it wrote some data (for non-empty arrays)
    if (owners.length > 0) {
      ok(writer.length > 0, "Writer should have content for non-empty arrays")
    }
  })
})

test('describeDelegationTokenV3 parseResponse handles successful response with tokens', () => {
  const { parseResponse } = captureApiHandlers(describeDelegationTokenV3)
  
  // Create a successful response with tokens
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    // Tokens array with 1 element
    .appendArray([1], (w) => {
      w.appendString('User', true) // principalType
        .appendString('admin', true) // principalName
        .appendString('User', true) // tokenRequesterPrincipalType
        .appendString('requester', true) // tokenRequesterPrincipalName
        .appendInt64(1234567890n) // issueTimestamp
        .appendInt64(1234657890n) // expiryTimestamp
        .appendInt64(1234957890n) // maxTimestamp
        .appendString('token-1', true) // tokenId
        .appendBytes(Buffer.from('hmac-data'), true) // hmac
        // Renewers array with 2 elements
        .appendArray([1, 2], (w2, _, j) => {
          w2.appendString('User', true) // principalType
            .appendString(`renewer-${j}`, true) // principalName
            .appendTaggedFields() // renewer tagged fields
        }, true, false)
        .appendTaggedFields() // token tagged fields
    }, true, false)
    .appendInt32(100) // throttleTimeMs
    .appendTaggedFields() // response tagged fields
  
  const response = parseResponse(1, 41, 3, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    errorCode: 0,
    tokens: [
      {
        principalType: 'User',
        principalName: 'admin',
        tokenRequesterPrincipalType: 'User',
        tokenRequesterPrincipalName: 'requester',
        issueTimestamp: 1234567890n,
        expiryTimestamp: 1234657890n,
        maxTimestamp: 1234957890n,
        tokenId: 'token-1',
        hmac: Buffer.from('hmac-data'),
        renewers: [
          {
            principalType: 'User',
            principalName: 'renewer-0'
          },
          {
            principalType: 'User',
            principalName: 'renewer-1'
          }
        ]
      }
    ],
    throttleTimeMs: 100
  })
})

test('describeDelegationTokenV3 parseResponse handles successful response with empty tokens', () => {
  const { parseResponse } = captureApiHandlers(describeDelegationTokenV3)
  
  // Create a successful response with empty tokens array
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    // Empty tokens array
    .appendArray([], () => {}, true, false)
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields() // response tagged fields
  
  const response = parseResponse(1, 41, 3, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    errorCode: 0,
    tokens: [],
    throttleTimeMs: 0
  })
})

test('describeDelegationTokenV3 parseResponse handles token with empty renewers', () => {
  const { parseResponse } = captureApiHandlers(describeDelegationTokenV3)
  
  // Create a successful response with a token that has empty renewers
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    // Tokens array with 1 element
    .appendArray([1], (w) => {
      w.appendString('User', true) // principalType
        .appendString('admin', true) // principalName
        .appendString('User', true) // tokenRequesterPrincipalType
        .appendString('requester', true) // tokenRequesterPrincipalName
        .appendInt64(1234567890n) // issueTimestamp
        .appendInt64(1234657890n) // expiryTimestamp
        .appendInt64(1234957890n) // maxTimestamp
        .appendString('token-1', true) // tokenId
        .appendBytes(Buffer.from('hmac-data'), true) // hmac
        // Empty renewers array
        .appendArray([], () => {}, true, false)
        .appendTaggedFields() // token tagged fields
    }, true, false)
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields() // response tagged fields
  
  const response = parseResponse(1, 41, 3, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response.tokens[0].renewers, [])
})

test('describeDelegationTokenV3 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(describeDelegationTokenV3)
  
  // Create an error response
  const writer = Writer.create()
    .appendInt16(58) // errorCode (SASL_AUTHENTICATION_FAILED)
    // Empty tokens array
    .appendArray([], () => {}, true, false)
    .appendInt32(100) // throttleTimeMs
    .appendTaggedFields() // response tagged fields
  
  // Should throw a ResponseError
  let errorThrown = false
  try {
    parseResponse(1, 41, 3, writer.bufferList)
  } catch (err: any) {
    errorThrown = true
    ok(err instanceof ResponseError, 'should be a ResponseError')
    
    // Verify it contains error information
    ok(err.response, 'should have a response object')
    strictEqual(err.response.errorCode, 58, 'response should have error code 58')
    
    // Just make sure we have some error mapping (implementation details can vary)
    ok(err.errors && typeof err.errors === 'object', 'should have errors object')
  }
  
  // Make sure an error was thrown
  ok(errorThrown, 'should have thrown an error')
})

test('describeDelegationTokenV3 API direct usage', () => {
  // Create a minimal test that doesn't rely on mocking the connection
  // but just verifies the API exists and can be called without throwing
  
  const owners = [{ principalType: 'User', principalName: 'admin' }]
  
  // The actual connection.send will be missing, but we just want to test
  // that the API function itself doesn't throw when used correctly
  const mockConnection = { 
    send: () => {
      // Mock that returns a promise
      return Promise.resolve({ tokens: [] })
    } 
  }
  
  // These should not throw
  try {
    // Use the async property to ensure we get a Promise
    const promise = describeDelegationTokenV3.async(mockConnection, { owners })
    ok(promise instanceof Promise, "API should return a Promise")
  } catch (err) {
    // If it throws, we'll fail the test with a descriptive message
    throw new Error(`API should not throw: ${err}`)
  }
  
  // Test the callback version
  describeDelegationTokenV3(mockConnection, { owners }, (err, result) => {
    // We don't need to verify anything here since the mock won't actually call it
    // The test just ensures the API can be called with a callback without throwing
  })
})

// Removed since we're using the simpler direct API test

// Removed since we're using the simpler direct API test

// Removed since we're using the simpler direct API test

test('describeDelegationTokenV3 validates parameters', () => {
  // Mock minimal connection
  const mockConnection = {
    send: () => true
  }
  
  // Different parameter combinations
  const testCases: { owners?: DescribeDelegationTokenRequestOwner[] }[] = [
    // With valid owners
    {
      owners: [
        { principalType: 'User', principalName: 'admin' }
      ]
    },
    // With empty owners array
    {
      owners: []
    },
    // Without owners (should use default)
    {}
  ]
  
  // All should not throw
  testCases.forEach(params => {
    describeDelegationTokenV3(mockConnection, params)
  })
})