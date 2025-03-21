import BufferList from 'bl'
import { deepStrictEqual, rejects } from 'node:assert'
import test from 'node:test'
import { alterUserScramCredentialsV0 } from '../../../src/apis/admin/alter-user-scram-credentials.ts'
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

test('alterUserScramCredentialsV0 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(alterUserScramCredentialsV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createRequest with empty arrays', () => {
  const { createRequest } = captureApiHandlers(alterUserScramCredentialsV0)
  
  const writer = createRequest([], [])
  deepStrictEqual(writer instanceof Writer, true)
  deepStrictEqual(writer.length > 0, true)
})

test('createRequest with only deletions', () => {
  const { createRequest } = captureApiHandlers(alterUserScramCredentialsV0)
  
  const deletions = [
    { name: 'user1', mechanism: 1 },
    { name: 'user2', mechanism: 2 }
  ]
  
  const writer = createRequest(deletions, [])
  deepStrictEqual(writer instanceof Writer, true)
  deepStrictEqual(writer.length > 0, true)
})

test('createRequest with only upsertions', () => {
  const { createRequest } = captureApiHandlers(alterUserScramCredentialsV0)
  
  const upsertions = [
    { 
      name: 'user1', 
      mechanism: 1, 
      iterations: 4096, 
      salt: Buffer.from('salt1'), 
      saltedPassword: Buffer.from('password1') 
    },
    { 
      name: 'user2', 
      mechanism: 2, 
      iterations: 8192, 
      salt: Buffer.from('salt2'), 
      saltedPassword: Buffer.from('password2') 
    }
  ]
  
  const writer = createRequest([], upsertions)
  deepStrictEqual(writer instanceof Writer, true)
  deepStrictEqual(writer.length > 0, true)
})

test('createRequest with both deletions and upsertions', () => {
  const { createRequest } = captureApiHandlers(alterUserScramCredentialsV0)
  
  const deletions = [
    { name: 'user1', mechanism: 1 }
  ]
  
  const upsertions = [
    { 
      name: 'user2', 
      mechanism: 2, 
      iterations: 4096, 
      salt: Buffer.from('salt'), 
      saltedPassword: Buffer.from('password') 
    }
  ]
  
  const writer = createRequest(deletions, upsertions)
  deepStrictEqual(writer instanceof Writer, true)
  deepStrictEqual(writer.length > 0, true)
})

test('alterUserScramCredentialsV0 should handle a successful response', () => {
  const { parseResponse } = captureApiHandlers(alterUserScramCredentialsV0)
  
  // Create a mock successful response
  // Create a binary message buffer that matches the response format
  const rawResponseBuffer = Buffer.concat([
    // throttleTimeMs - Int32
    Buffer.from([0, 0, 0, 0]),
    // results array length (compact format) - 2 elements + 1 = 3
    Buffer.from([3]),
    
    // First result
    // user - String (compact format) - length(5) + 1 = 6, then "user1"
    Buffer.from([6]), Buffer.from("user1"),
    // errorCode - Int16 - 0 (no error)
    Buffer.from([0, 0]),
    // errorMessage - String (compact format) - null = 0
    Buffer.from([0]),
    // Tagged fields - empty = 0
    Buffer.from([0]),
    
    // Second result
    // user - String (compact format) - length(5) + 1 = 6, then "user2"
    Buffer.from([6]), Buffer.from("user2"),
    // errorCode - Int16 - 0 (no error)
    Buffer.from([0, 0]),
    // errorMessage - String (compact format) - null = 0
    Buffer.from([0]),
    // Tagged fields - empty = 0
    Buffer.from([0]),
    
    // Tagged fields at the end - empty = 0
    Buffer.from([0])
  ])
  
  const response = parseResponse(0, 51, 0, new BufferList(rawResponseBuffer))
  
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    results: [
      { user: 'user1', errorCode: 0, errorMessage: null },
      { user: 'user2', errorCode: 0, errorMessage: null }
    ]
  })
})

test('alterUserScramCredentialsV0 should handle a response with warning messages', () => {
  const { parseResponse } = captureApiHandlers(alterUserScramCredentialsV0)
  
  // Create a response with warning message but no error code
  const warningMsg = 'Some warning'
  const warningMsgLength = Buffer.from(warningMsg).length
  
  const rawResponseBuffer = Buffer.concat([
    // throttleTimeMs - Int32
    Buffer.from([0, 0, 0, 0]),
    // results array length (compact format) - 2 elements + 1 = 3
    Buffer.from([3]),
    
    // First result
    // user - String (compact format) - length(5) + 1 = 6, then "user1"
    Buffer.from([6]), Buffer.from("user1"),
    // errorCode - Int16 - 0 (no error)
    Buffer.from([0, 0]),
    // errorMessage - String (compact format) - null = 0
    Buffer.from([0]),
    // Tagged fields - empty = 0
    Buffer.from([0]),
    
    // Second result
    // user - String (compact format) - length(5) + 1 = 6, then "user2"
    Buffer.from([6]), Buffer.from("user2"),
    // errorCode - Int16 - 0 (no error)
    Buffer.from([0, 0]),
    // errorMessage - String (compact format) - length(warningMsgLength) + 1, then "Some warning"
    Buffer.from([warningMsgLength + 1]), Buffer.from(warningMsg),
    // Tagged fields - empty = 0
    Buffer.from([0]),
    
    // Tagged fields at the end - empty = 0
    Buffer.from([0])
  ])
  
  const response = parseResponse(0, 51, 0, new BufferList(rawResponseBuffer))
  
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    results: [
      { user: 'user1', errorCode: 0, errorMessage: null },
      { user: 'user2', errorCode: 0, errorMessage: 'Some warning' }
    ]
  })
})

test('alterUserScramCredentialsV0 should throw ResponseError for error codes', async () => {
  const { parseResponse } = captureApiHandlers(alterUserScramCredentialsV0)
  
  // Create a response with error code
  const errorMsg = 'Invalid credentials'
  const errorMsgLength = Buffer.from(errorMsg).length
  
  const rawResponseBuffer = Buffer.concat([
    // throttleTimeMs - Int32
    Buffer.from([0, 0, 0, 0]),
    // results array length (compact format) - 2 elements + 1 = 3
    Buffer.from([3]),
    
    // First result
    // user - String (compact format) - length(5) + 1 = 6, then "user1"
    Buffer.from([6]), Buffer.from("user1"),
    // errorCode - Int16 - 0 (no error)
    Buffer.from([0, 0]),
    // errorMessage - String (compact format) - null = 0
    Buffer.from([0]),
    // Tagged fields - empty = 0
    Buffer.from([0]),
    
    // Second result
    // user - String (compact format) - length(5) + 1 = 6, then "user2"
    Buffer.from([6]), Buffer.from("user2"),
    // errorCode - Int16 - 58 (0x3A) SASL_AUTHENTICATION_FAILED
    Buffer.from([0, 58]),
    // errorMessage - String (compact format)
    Buffer.from([errorMsgLength + 1]), Buffer.from(errorMsg),
    // Tagged fields - empty = 0
    Buffer.from([0]),
    
    // Tagged fields at the end - empty = 0
    Buffer.from([0])
  ])
  
  await rejects(
    async () => parseResponse(0, 51, 0, new BufferList(rawResponseBuffer)),
    (err) => {
      deepStrictEqual(err instanceof ResponseError, true)
      return true
    }
  )
})

test('alterUserScramCredentialsV0 should execute the API call correctly', async () => {
  // Create a mock connection that simulates a successful response
  const mockConnection = {
    send: (_apiKey: number, _apiVersion: number, createRequestFn: any, parseResponseFn: any) => {
      // Store the functions for verification
      mockConnection.createRequestFn = createRequestFn
      mockConnection.parseResponseFn = parseResponseFn
      
      // Return a promise that resolves with the mock response
      return Promise.resolve({
        throttleTimeMs: 0,
        results: [
          { user: 'user1', errorCode: 0, errorMessage: null }
        ]
      })
    },
    createRequestFn: null as any,
    parseResponseFn: null as any
  }
  
  // Test deletions
  const deletions = [{ name: 'oldUser', mechanism: 1 }]
  
  // Test upsertions
  const upsertions = [{
    name: 'newUser',
    mechanism: 2,
    iterations: 4096,
    salt: Buffer.from('salt'),
    saltedPassword: Buffer.from('password')
  }]
  
  // Execute the API
  const response = await alterUserScramCredentialsV0(
    mockConnection as any,
    deletions,
    upsertions
  )
  
  // Verify the response
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    results: [
      { user: 'user1', errorCode: 0, errorMessage: null }
    ]
  })
})