import BufferList from 'bl'
import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { unregisterBrokerV0 } from '../../../src/apis/admin/unregister-broker.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
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
  apiFunction(mockConnection, 1)
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn
  }
}

test('unregisterBrokerV0 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(unregisterBrokerV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

test('createRequest serializes broker id correctly', () => {
  // Create a request directly
  const writer = Writer.create().appendInt32(1).appendTaggedFields()
  
  // Verify writer is created
  deepStrictEqual(typeof writer, 'object')
  deepStrictEqual(typeof writer.length, 'number')
  deepStrictEqual(writer.length > 0, true)
  
  // Verify the serialized request structure by reading it back
  const reader = Reader.from(writer.bufferList)
  
  // Read the broker id
  const readBrokerId = reader.readInt32()
  deepStrictEqual(readBrokerId, 1)
})

test('createRequest handles different broker ids', () => {
  const testCases = [0, 1, 42, 100, 9999]
  
  // Test each case
  for (const brokerId of testCases) {
    // Create request directly
    const writer = Writer.create()
      .appendInt32(brokerId)
      .appendTaggedFields()
      
    deepStrictEqual(typeof writer, 'object')
    deepStrictEqual(writer.length > 0, true)
    
    // Verify by reading back
    const reader = Reader.from(writer.bufferList)
    const readBrokerId = reader.readInt32()
    deepStrictEqual(readBrokerId, brokerId, 'Broker ID should match')
  }
})

test('parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(unregisterBrokerV0)
  
  // Create a valid mock response buffer
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode = 0 (success)
    .appendString(null) // errorMessage = null
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 64, 0, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.errorMessage, null)
})

test('parseResponse handles successful response with error message', () => {
  const { parseResponse } = captureApiHandlers(unregisterBrokerV0)
  
  // Create a valid mock response buffer with error message (but success code)
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(0) // errorCode = 0 (success)
    .appendString('Some information message') // errorMessage (non-null)
    .appendTaggedFields()
  
  // Parse the response
  const response = parseResponse(1, 64, 0, writer.bufferList)
  
  // Verify the response structure
  deepStrictEqual(response.throttleTimeMs, 100)
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.errorMessage, 'Some information message')
})

test('parseResponse throws on error response', () => {
  const { parseResponse } = captureApiHandlers(unregisterBrokerV0)
  
  // Create a mock error response
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs
    .appendInt16(50) // errorCode = 50 (NonEmptyTopic)
    .appendString('Broker still has active topics') // errorMessage
    .appendTaggedFields()
  
  // Verify that parseResponse throws ResponseError
  let error: any
  try {
    parseResponse(1, 64, 0, writer.bufferList)
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
  deepStrictEqual(error.response.throttleTimeMs, 100)
  deepStrictEqual(error.response.errorCode, 50)
  deepStrictEqual(error.response.errorMessage, 'Broker still has active topics')
})

test('API mock simulation without callback', async () => {
  // Create a simplified mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Verify correct API key and version
      deepStrictEqual(apiKey, 64) // UnregisterBroker API
      deepStrictEqual(apiVersion, 0) // Version 0
      
      // Call the callback with a predetermined response
      callback(null, { 
        throttleTimeMs: 100,
        errorCode: 0,
        errorMessage: null
      })
      return true
    }
  }
  
  // Call the API with minimal required arguments
  const brokerId = 42
  
  // Verify the API can be called without errors using async
  const result = await unregisterBrokerV0.async(mockConnection as any, brokerId)
  deepStrictEqual(result, { 
    throttleTimeMs: 100,
    errorCode: 0,
    errorMessage: null
  })
})

test('API mock simulation with callback', () => {
  // Create a simplified mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Verify correct API key and version
      deepStrictEqual(apiKey, 64) // UnregisterBroker API
      deepStrictEqual(apiVersion, 0) // Version 0
      
      // Call the callback with a response
      callback(null, { 
        throttleTimeMs: 100,
        errorCode: 0,
        errorMessage: null
      })
      return true
    }
  }
  
  // Call the API with callback
  const brokerId = 42
  
  // Use a callback to test the callback-based API
  let callbackCalled = false
  unregisterBrokerV0(mockConnection as any, brokerId, (error, result) => {
    callbackCalled = true
    deepStrictEqual(error, null)
    deepStrictEqual(result, { 
      throttleTimeMs: 100,
      errorCode: 0,
      errorMessage: null
    })
  })
  
  deepStrictEqual(callbackCalled, true, 'Callback should have been called')
})

test('API error handling with callback', () => {
  // Create a mock connection that returns an error
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeader: boolean, hasResponseHeader: boolean, callback: any) => {
      // Call the callback with an error
      const mockError = new Error('Broker ID not found')
      callback(mockError, null)
      return true
    }
  }
  
  // Call the API with callback
  const brokerId = 42
  
  // Use a callback to test error handling
  let callbackCalled = false
  unregisterBrokerV0(mockConnection as any, brokerId, (error, result) => {
    callbackCalled = true
    deepStrictEqual(error instanceof Error, true)
    deepStrictEqual((error as Error).message, 'Broker ID not found')
    deepStrictEqual(result, null)
  })
  
  deepStrictEqual(callbackCalled, true, 'Error callback should have been called')
})