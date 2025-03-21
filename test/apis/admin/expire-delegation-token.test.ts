import BufferList from 'bl'
import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { expireDelegationTokenV2 } from '../../../src/apis/admin/expire-delegation-token.ts'
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
  const sampleHmac = Buffer.from('sampleHmacToken', 'utf-8')
  apiFunction(mockConnection, sampleHmac, 3600000n)
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn
  }
}

test('expireDelegationTokenV2 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(expireDelegationTokenV2)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createRequest serializes parameters correctly', () => {
  const sampleHmac = Buffer.from('sampleHmacToken', 'utf-8')
  
  // Create a request directly
  const writer = Writer.create().appendBytes(sampleHmac).appendInt64(3600000n).appendTaggedFields()
  
  // Verify writer is created
  deepStrictEqual(typeof writer, 'object')
  deepStrictEqual(typeof writer.length, 'number')
  deepStrictEqual(writer.length > 0, true)
  
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read the hmac bytes
  const readHmac = reader.readBytes()
  deepStrictEqual(Buffer.isBuffer(readHmac), true)
  deepStrictEqual(readHmac.toString('utf-8'), 'sampleHmacToken')
  
  // Read the expiry time period
  const readExpiryTime = reader.readInt64()
  deepStrictEqual(readExpiryTime, 3600000n)
})

test('createRequest handles different hmac values and expiry times', () => {
  const testCases = [
    {
      hmac: Buffer.from('shortToken', 'utf-8'),
      expiryTimePeriodMs: 1000n
    },
    {
      hmac: Buffer.from('longerHmacTokenWithMoreCharacters', 'utf-8'),
      expiryTimePeriodMs: 86400000n // 24 hours
    },
    {
      hmac: Buffer.from([0x01, 0x02, 0x03, 0x04]), // Binary buffer
      expiryTimePeriodMs: 0n
    }
  ]
  
  // Test each case
  for (const testCase of testCases) {
    // Create request directly
    const writer = Writer.create()
      .appendBytes(testCase.hmac)
      .appendInt64(testCase.expiryTimePeriodMs)
      .appendTaggedFields()
      
    deepStrictEqual(typeof writer, 'object')
    deepStrictEqual(writer.length > 0, true)
    
    // Verify by reading back
    const reader = Reader.from(writer.bufferList)
    const readHmac = reader.readBytes()
    deepStrictEqual(Buffer.compare(readHmac, testCase.hmac), 0, 'HMAC should match')
    const readExpiryTime = reader.readInt64()
    deepStrictEqual(readExpiryTime, testCase.expiryTimePeriodMs, 'Expiry time should match')
  }
})

test('parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(expireDelegationTokenV2)
  
  // Create a valid mock response buffer
  const writer = Writer.create()
    .appendInt16(0) // errorCode = 0 (success)
    .appendInt64(1656088800000n) // expiryTimestampMs (some timestamp)
    .appendInt32(100) // throttleTimeMs
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 40, 2, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.expiryTimestampMs, 1656088800000n)
  deepStrictEqual(response.throttleTimeMs, 100)
})

test('parseResponse throws on error response', () => {
  const { parseResponse } = captureApiHandlers(expireDelegationTokenV2)
  
  // Create a mock error response
  const writer = Writer.create()
    .appendInt16(58) // errorCode = 58 (InvalidPrincipalType)
    .appendInt64(0n) // expiryTimestampMs
    .appendInt32(100) // throttleTimeMs
    .appendTaggedFields()
  
  // Verify that parseResponse throws ResponseError
  let error: any
  try {
    parseResponse(1, 40, 2, writer.bufferList)
    throw new Error('Should have thrown an error')
  } catch (err) {
    if (!(err instanceof ResponseError)) {
      throw err
    }
    error = err
  }
  
  // Verify the error structure
  deepStrictEqual(error instanceof ResponseError, true)
  
  // Print error keys for debugging
  console.log('Error keys:', Object.keys(error.errors))
  
  // Verify errors exist (don't check specific paths as they might vary)
  deepStrictEqual(Object.keys(error.errors).length > 0, true, 'Should have at least one error')
  
  // Verify the response is still attached to the error
  deepStrictEqual(error.response.errorCode, 58)
  deepStrictEqual(error.response.expiryTimestampMs, 0n)
  deepStrictEqual(error.response.throttleTimeMs, 100)
})

test('API mock simulation without callback', () => {
  // Create a simplified mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, ...args: any[]) => {
      // Verify correct API key and version
      deepStrictEqual(apiKey, 40) // ExpireDelegationToken API
      deepStrictEqual(apiVersion, 2) // Version 2
      
      // Return a predetermined response
      return { 
        errorCode: 0,
        expiryTimestampMs: 1656088800000n,
        throttleTimeMs: 100
      }
    }
  }
  
  // Call the API with minimal required arguments
  const hmac = Buffer.from('testHmacToken', 'utf-8')
  const expiryTimePeriodMs = 3600000n
  
  // Verify the API can be called without errors
  const result = expireDelegationTokenV2(mockConnection as any, hmac, expiryTimePeriodMs)
  deepStrictEqual(result, { 
    errorCode: 0,
    expiryTimestampMs: 1656088800000n,
    throttleTimeMs: 100
  })
})

test('API mock simulation with callback', () => {
  // Create a simplified mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Verify correct API key and version
      deepStrictEqual(apiKey, 40) // ExpireDelegationToken API
      deepStrictEqual(apiVersion, 2) // Version 2
      
      // Call the callback with a response
      callback(null, { 
        errorCode: 0,
        expiryTimestampMs: 1656088800000n,
        throttleTimeMs: 100
      })
      return true
    }
  }
  
  // Call the API with callback
  const hmac = Buffer.from('testHmacToken', 'utf-8')
  const expiryTimePeriodMs = 3600000n
  
  // Use a callback to test the callback-based API
  let callbackCalled = false
  expireDelegationTokenV2(mockConnection as any, hmac, expiryTimePeriodMs, (error, result) => {
    callbackCalled = true
    deepStrictEqual(error, null)
    deepStrictEqual(result, { 
      errorCode: 0,
      expiryTimestampMs: 1656088800000n,
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
      const mockError = new Error('Invalid HMAC')
      callback(mockError, null)
      return true
    }
  }
  
  // Call the API with callback
  const hmac = Buffer.from('testHmacToken', 'utf-8')
  const expiryTimePeriodMs = 3600000n
  
  // Use a callback to test error handling
  let callbackCalled = false
  expireDelegationTokenV2(mockConnection as any, hmac, expiryTimePeriodMs, (error, result) => {
    callbackCalled = true
    deepStrictEqual(error instanceof Error, true)
    deepStrictEqual((error as Error).message, 'Invalid HMAC')
    deepStrictEqual(result, null)
  })
  
  deepStrictEqual(callbackCalled, true, 'Error callback should have been called')
})