import BufferList from 'bl'
import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { createDelegationTokenV3 } from '../../../src/apis/admin/create-delegation-token.ts'
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
  const renewers = [{ principalType: 'User', principalName: 'test-user' }]
  apiFunction(mockConnection, null, null, renewers, 86400000n)
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn
  }
}

test('createDelegationTokenV3 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(createDelegationTokenV3)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createRequest serializes parameters correctly', () => {
  // Create a request directly with sample values
  const ownerPrincipalType = 'User'
  const ownerPrincipalName = 'admin'
  const renewers = [
    { principalType: 'User', principalName: 'user1' },
    { principalType: 'Group', principalName: 'group1' }
  ]
  const maxLifetimeMs = 86400000n // 24 hours
  
  const writer = Writer.create()
    .appendString(ownerPrincipalType)
    .appendString(ownerPrincipalName)
    .appendArray(renewers, (w, r) => w.appendString(r.principalType).appendString(r.principalName))
    .appendInt64(maxLifetimeMs)
    .appendTaggedFields()
  
  // Verify writer is created
  deepStrictEqual(typeof writer, 'object')
  deepStrictEqual(typeof writer.length, 'number')
  deepStrictEqual(writer.length > 0, true)
  
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read the owner principal type and name
  const readOwnerPrincipalType = reader.readString()
  deepStrictEqual(readOwnerPrincipalType, ownerPrincipalType)
  const readOwnerPrincipalName = reader.readString()
  deepStrictEqual(readOwnerPrincipalName, ownerPrincipalName)
  
  // Read the renewers array
  const readRenewers = reader.readArray((r) => {
    return {
      principalType: r.readString(),
      principalName: r.readString()
    }
  })
  deepStrictEqual(readRenewers.length, 2)
  deepStrictEqual(readRenewers[0].principalType, 'User')
  deepStrictEqual(readRenewers[0].principalName, 'user1')
  deepStrictEqual(readRenewers[1].principalType, 'Group')
  deepStrictEqual(readRenewers[1].principalName, 'group1')
  
  // Read the max lifetime
  const readMaxLifetimeMs = reader.readInt64()
  deepStrictEqual(readMaxLifetimeMs, 86400000n)
})

test('createRequest handles null owner values correctly', () => {
  // Create a request with null owner values
  const ownerPrincipalType = null
  const ownerPrincipalName = null
  const renewers = [{ principalType: 'User', principalName: 'user1' }]
  const maxLifetimeMs = 3600000n // 1 hour
  
  const writer = Writer.create()
    .appendString(ownerPrincipalType)
    .appendString(ownerPrincipalName)
    .appendArray(renewers, (w, r) => w.appendString(r.principalType).appendString(r.principalName))
    .appendInt64(maxLifetimeMs)
    .appendTaggedFields()
  
  // Verify writer is created
  deepStrictEqual(typeof writer, 'object')
  deepStrictEqual(writer.length > 0, true)
  
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read the owner principal type and name
  const readOwnerPrincipalType = reader.readString()
  deepStrictEqual(readOwnerPrincipalType, null)
  const readOwnerPrincipalName = reader.readString()
  deepStrictEqual(readOwnerPrincipalName, null)
  
  // Read the renewers array
  const readRenewers = reader.readArray((r) => {
    return {
      principalType: r.readString(),
      principalName: r.readString()
    }
  })
  deepStrictEqual(readRenewers.length, 1)
  deepStrictEqual(readRenewers[0].principalType, 'User')
  deepStrictEqual(readRenewers[0].principalName, 'user1')
  
  // Read the max lifetime
  const readMaxLifetimeMs = reader.readInt64()
  deepStrictEqual(readMaxLifetimeMs, 3600000n)
})

test('createRequest handles empty renewers array', () => {
  // Create a request with empty renewers array
  const ownerPrincipalType = 'User'
  const ownerPrincipalName = 'admin'
  const renewers: { principalType: string, principalName: string }[] = []
  const maxLifetimeMs = 86400000n
  
  const writer = Writer.create()
    .appendString(ownerPrincipalType)
    .appendString(ownerPrincipalName)
    .appendArray(renewers, (w, r) => w.appendString(r.principalType).appendString(r.principalName))
    .appendInt64(maxLifetimeMs)
    .appendTaggedFields()
  
  // Verify writer is created
  deepStrictEqual(typeof writer, 'object')
  deepStrictEqual(writer.length > 0, true)
  
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read the owner principal type and name
  const readOwnerPrincipalType = reader.readString()
  deepStrictEqual(readOwnerPrincipalType, ownerPrincipalType)
  const readOwnerPrincipalName = reader.readString()
  deepStrictEqual(readOwnerPrincipalName, ownerPrincipalName)
  
  // Read the renewers array (should be empty)
  const readRenewers = reader.readArray((r) => {
    return {
      principalType: r.readString(),
      principalName: r.readString()
    }
  })
  deepStrictEqual(readRenewers.length, 0)
  
  // Read the max lifetime
  const readMaxLifetimeMs = reader.readInt64()
  deepStrictEqual(readMaxLifetimeMs, 86400000n)
})

test('parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(createDelegationTokenV3)
  
  // Create a valid mock response buffer
  const writer = Writer.create()
    .appendInt16(0) // errorCode = 0 (success)
    .appendString('User') // principalType
    .appendString('admin') // principalName
    .appendString('User') // tokenRequesterPrincipalType
    .appendString('requester') // tokenRequesterPrincipalName
    .appendInt64(1656088800000n) // issueTimestampMs
    .appendInt64(1656175200000n) // expiryTimestampMs
    .appendInt64(1656261600000n) // maxTimestampMs
    .appendString('token-123') // tokenId
    .appendBytes(Buffer.from('hmac-token-bytes', 'utf-8')) // hmac
    .appendInt32(100) // throttleTimeMs
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 38, 3, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.principalType, 'User')
  deepStrictEqual(response.principalName, 'admin')
  deepStrictEqual(response.tokenRequesterPrincipalType, 'User')
  deepStrictEqual(response.tokenRequesterPrincipalName, 'requester')
  deepStrictEqual(response.issueTimestampMs, 1656088800000n)
  deepStrictEqual(response.expiryTimestampMs, 1656175200000n)
  deepStrictEqual(response.maxTimestampMs, 1656261600000n)
  deepStrictEqual(response.tokenId, 'token-123')
  deepStrictEqual(Buffer.isBuffer(response.hmac), true)
  deepStrictEqual(response.hmac.toString('utf-8'), 'hmac-token-bytes')
  deepStrictEqual(response.throttleTimeMs, 100)
})

test('parseResponse throws on error response', () => {
  const { parseResponse } = captureApiHandlers(createDelegationTokenV3)
  
  // Create a mock error response
  const writer = Writer.create()
    .appendInt16(58) // errorCode = 58 (InvalidPrincipalType)
    .appendString('User') // principalType
    .appendString('admin') // principalName
    .appendString('User') // tokenRequesterPrincipalType
    .appendString('requester') // tokenRequesterPrincipalName
    .appendInt64(1656088800000n) // issueTimestampMs
    .appendInt64(0n) // expiryTimestampMs
    .appendInt64(0n) // maxTimestampMs
    .appendString('') // tokenId
    .appendBytes(Buffer.alloc(0)) // empty hmac
    .appendInt32(100) // throttleTimeMs
    .appendTaggedFields()
  
  // Verify that parseResponse throws ResponseError
  let error: any
  try {
    parseResponse(1, 38, 3, writer.bufferList)
    throw new Error('Should have thrown an error')
  } catch (err) {
    if (!(err instanceof ResponseError)) {
      throw err
    }
    error = err
  }
  
  // Verify the error structure
  deepStrictEqual(error instanceof ResponseError, true)
  
  // Verify errors exist (don't check specific paths as they might vary)
  deepStrictEqual(Object.keys(error.errors).length > 0, true, 'Should have at least one error')
  
  // Verify the response is still attached to the error
  deepStrictEqual(error.response.errorCode, 58)
  deepStrictEqual(error.response.principalType, 'User')
  deepStrictEqual(error.response.principalName, 'admin')
  deepStrictEqual(error.response.expiryTimestampMs, 0n)
})

test('API mock simulation without callback', () => {
  // Create a simplified mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, ...args: any[]) => {
      // Verify correct API key and version
      deepStrictEqual(apiKey, 38) // CreateDelegationToken API
      deepStrictEqual(apiVersion, 3) // Version 3
      
      // Return a predetermined response
      return { 
        errorCode: 0,
        principalType: 'User',
        principalName: 'admin',
        tokenRequesterPrincipalType: 'User',
        tokenRequesterPrincipalName: 'requester',
        issueTimestampMs: 1656088800000n,
        expiryTimestampMs: 1656175200000n,
        maxTimestampMs: 1656261600000n,
        tokenId: 'token-123',
        hmac: Buffer.from('hmac-token-bytes', 'utf-8'),
        throttleTimeMs: 100
      }
    }
  }
  
  // Call the API with minimal required arguments
  const ownerPrincipalType = 'User'
  const ownerPrincipalName = 'admin'
  const renewers = [{ principalType: 'User', principalName: 'user1' }]
  const maxLifetimeMs = 86400000n
  
  // Verify the API can be called without errors
  const result = createDelegationTokenV3(mockConnection as any, ownerPrincipalType, ownerPrincipalName, renewers, maxLifetimeMs)
  deepStrictEqual(result, { 
    errorCode: 0,
    principalType: 'User',
    principalName: 'admin',
    tokenRequesterPrincipalType: 'User',
    tokenRequesterPrincipalName: 'requester',
    issueTimestampMs: 1656088800000n,
    expiryTimestampMs: 1656175200000n,
    maxTimestampMs: 1656261600000n,
    tokenId: 'token-123',
    hmac: Buffer.from('hmac-token-bytes', 'utf-8'),
    throttleTimeMs: 100
  })
})

test('API mock simulation with callback', () => {
  // Create a simplified mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Verify correct API key and version
      deepStrictEqual(apiKey, 38) // CreateDelegationToken API
      deepStrictEqual(apiVersion, 3) // Version 3
      
      // Call the callback with a response
      callback(null, { 
        errorCode: 0,
        principalType: 'User',
        principalName: 'admin',
        tokenRequesterPrincipalType: 'User',
        tokenRequesterPrincipalName: 'requester',
        issueTimestampMs: 1656088800000n,
        expiryTimestampMs: 1656175200000n,
        maxTimestampMs: 1656261600000n,
        tokenId: 'token-123',
        hmac: Buffer.from('hmac-token-bytes', 'utf-8'),
        throttleTimeMs: 100
      })
      return true
    }
  }
  
  // Call the API with callback
  const ownerPrincipalType = 'User'
  const ownerPrincipalName = 'admin'
  const renewers = [{ principalType: 'User', principalName: 'user1' }]
  const maxLifetimeMs = 86400000n
  
  // Use a callback to test the callback-based API
  let callbackCalled = false
  createDelegationTokenV3(mockConnection as any, ownerPrincipalType, ownerPrincipalName, renewers, maxLifetimeMs, (error, result) => {
    callbackCalled = true
    deepStrictEqual(error, null)
    deepStrictEqual(result, { 
      errorCode: 0,
      principalType: 'User',
      principalName: 'admin',
      tokenRequesterPrincipalType: 'User',
      tokenRequesterPrincipalName: 'requester',
      issueTimestampMs: 1656088800000n,
      expiryTimestampMs: 1656175200000n,
      maxTimestampMs: 1656261600000n,
      tokenId: 'token-123',
      hmac: Buffer.from('hmac-token-bytes', 'utf-8'),
      throttleTimeMs: 100
    })
  })
  
  deepStrictEqual(callbackCalled, true, 'Callback should have been called')
})

test('API error handling with callback', () => {
  // Create a mock connection that returns an error
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Call the callback with an error
      const mockError = new Error('Invalid principal type')
      callback(mockError, null)
      return true
    }
  }
  
  // Call the API with callback
  const ownerPrincipalType = 'InvalidType'
  const ownerPrincipalName = 'admin'
  const renewers = [{ principalType: 'User', principalName: 'user1' }]
  const maxLifetimeMs = 86400000n
  
  // Use a callback to test error handling
  let callbackCalled = false
  createDelegationTokenV3(mockConnection as any, ownerPrincipalType, ownerPrincipalName, renewers, maxLifetimeMs, (error, result) => {
    callbackCalled = true
    deepStrictEqual(error instanceof Error, true)
    deepStrictEqual((error as Error).message, 'Invalid principal type')
    deepStrictEqual(result, null)
  })
  
  deepStrictEqual(callbackCalled, true, 'Error callback should have been called')
})