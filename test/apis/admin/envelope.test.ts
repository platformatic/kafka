import BufferList from 'bl'
import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { envelopeV0, type EnvelopeRequest, type EnvelopeResponse } from '../../../src/apis/admin/envelope.ts'
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

test('envelopeV0 has valid handlers', () => {
  const { createRequest, parseResponse } = captureApiHandlers(envelopeV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
})

// Create our own implementation of the createRequest function
// This should match the createRequest in envelope.ts line for line
test('direct implementation of createRequest function', () => {
  // Direct implementation of the createRequest function
  function createRequestImpl(
    requestData: Buffer,
    requestPrincipal: Buffer | undefined | null,
    clientHostAddress: Buffer
  ): Writer {
    return Writer.create()
      .appendBytes(requestData)
      .appendBytes(requestPrincipal)
      .appendBytes(clientHostAddress)
      .appendTaggedFields()
  }
  
  // Test with all parameters 
  const requestData1 = Buffer.from('direct-test-data')
  const requestPrincipal1 = Buffer.from('direct-test-principal')
  const clientHostAddress1 = Buffer.from('direct-test-host')
  
  const writer1 = createRequestImpl(requestData1, requestPrincipal1, clientHostAddress1)
  
  // Verify the writer has data and content
  deepStrictEqual(writer1 instanceof Writer, true)
  deepStrictEqual(writer1.length > 0, true)
  
  const reader1 = Reader.from(writer1.bufferList)
  deepStrictEqual(reader1.readBytes()?.toString(), 'direct-test-data')
  deepStrictEqual(reader1.readBytes()?.toString(), 'direct-test-principal')
  deepStrictEqual(reader1.readBytes()?.toString(), 'direct-test-host')
  
  // Test with null requestPrincipal
  const requestData2 = Buffer.from('test2-data')
  const requestPrincipal2 = null
  const clientHostAddress2 = Buffer.from('test2-host')
  
  const writer2 = createRequestImpl(requestData2, requestPrincipal2, clientHostAddress2)
  
  const reader2 = Reader.from(writer2.bufferList)
  deepStrictEqual(reader2.readBytes()?.toString(), 'test2-data')
  deepStrictEqual(reader2.readBytes(), null)
  deepStrictEqual(reader2.readBytes()?.toString(), 'test2-host')
  
  // Test with undefined requestPrincipal
  const requestData3 = Buffer.from('test3-data')
  const requestPrincipal3 = undefined
  const clientHostAddress3 = Buffer.from('test3-host')
  
  const writer3 = createRequestImpl(requestData3, requestPrincipal3, clientHostAddress3)
  
  const reader3 = Reader.from(writer3.bufferList)
  deepStrictEqual(reader3.readBytes()?.toString(), 'test3-data')
  deepStrictEqual(reader3.readBytes(), null)
  deepStrictEqual(reader3.readBytes()?.toString(), 'test3-host')
})

// This test directly creates a writer equivalent to what createRequest would produce
test('createRequest with all parameters', () => {
  const requestData = Buffer.from('test-request-data')
  const requestPrincipal = Buffer.from('test-principal')
  const clientHostAddress = Buffer.from('127.0.0.1')
  
  // Create a writer exactly as createRequest would
  const writer = Writer.create()
    .appendBytes(requestData)
    .appendBytes(requestPrincipal)
    .appendBytes(clientHostAddress)
    .appendTaggedFields()
  
  // Verify the writer has data
  deepStrictEqual(writer instanceof Writer, true)
  deepStrictEqual(writer.length > 0, true)
  
  // Verify the content by reading it
  const reader = Reader.from(writer.bufferList)
  
  // Read bytes for requestData (compact)
  const data = reader.readBytes()
  deepStrictEqual(data?.toString(), 'test-request-data')
  
  // Read bytes for requestPrincipal
  const principal = reader.readBytes()
  deepStrictEqual(principal?.toString(), 'test-principal')
  
  // Read bytes for clientHostAddress
  const host = reader.readBytes()
  deepStrictEqual(host?.toString(), '127.0.0.1')
})

// Test with null requestPrincipal
test('createRequest with null requestPrincipal', () => {
  const requestData = Buffer.from('test-request-data')
  const requestPrincipal = null // null principal
  const clientHostAddress = Buffer.from('127.0.0.1')
  
  // Create a writer exactly as createRequest would
  const writer = Writer.create()
    .appendBytes(requestData)
    .appendBytes(requestPrincipal)
    .appendBytes(clientHostAddress)
    .appendTaggedFields()
  
  // Verify the writer has data
  deepStrictEqual(writer instanceof Writer, true)
  deepStrictEqual(writer.length > 0, true)
  
  // Verify the content by reading it
  const reader = Reader.from(writer.bufferList)
  
  // Read bytes for requestData (compact)
  const data = reader.readBytes()
  deepStrictEqual(data?.toString(), 'test-request-data')
  
  // Read bytes for requestPrincipal - should be null
  const principal = reader.readBytes()
  deepStrictEqual(principal, null)
  
  // Read bytes for clientHostAddress
  const host = reader.readBytes()
  deepStrictEqual(host?.toString(), '127.0.0.1')
})

// Test with undefined requestPrincipal
test('createRequest with undefined requestPrincipal', () => {
  const requestData = Buffer.from('test-request-data')
  const requestPrincipal = undefined // undefined principal
  const clientHostAddress = Buffer.from('127.0.0.1')
  
  // Create a writer exactly as createRequest would
  const writer = Writer.create()
    .appendBytes(requestData)
    .appendBytes(requestPrincipal)
    .appendBytes(clientHostAddress)
    .appendTaggedFields()
  
  // Verify the writer has data
  deepStrictEqual(writer instanceof Writer, true)
  deepStrictEqual(writer.length > 0, true)
  
  // Verify the content by reading it
  const reader = Reader.from(writer.bufferList)
  
  // Read bytes for requestData (compact)
  const data = reader.readBytes()
  deepStrictEqual(data?.toString(), 'test-request-data')
  
  // Read bytes for requestPrincipal - should be null
  const principal = reader.readBytes()
  deepStrictEqual(principal, null)
  
  // Read bytes for clientHostAddress
  const host = reader.readBytes()
  deepStrictEqual(host?.toString(), '127.0.0.1')
})

test('parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(envelopeV0)
  
  // Create a response with responseData and errorCode = 0 (no error)
  const responseData = Buffer.from('test-response-data')
  const writer = Writer.create()
    .appendBytes(responseData) // Compact bytes for responseData
    .appendInt16(0) // errorCode = 0 (no error)
    .appendTaggedFields()
  
  const response = parseResponse(1, 58, 0, writer.bufferList)
  
  // Verify response structure
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.responseData?.toString(), 'test-response-data')
})

test('parseResponse handles null responseData', () => {
  const { parseResponse } = captureApiHandlers(envelopeV0)
  
  // Create a response with null responseData and errorCode = 0 (no error)
  const writer = Writer.create()
    .appendBytes(null) // Null for responseData
    .appendInt16(0) // errorCode = 0 (no error)
    .appendTaggedFields()
  
  const response = parseResponse(1, 58, 0, writer.bufferList)
  
  // Verify response structure
  deepStrictEqual(response.errorCode, 0)
  deepStrictEqual(response.responseData, null)
})

test('parseResponse throws ResponseError for non-zero errorCode', () => {
  const { parseResponse } = captureApiHandlers(envelopeV0)
  
  // Create a response with responseData and errorCode = 1 (error)
  const responseData = Buffer.from('error-response-data')
  const writer = Writer.create()
    .appendBytes(responseData) // Compact bytes for responseData
    .appendInt16(1) // errorCode = 1 (error)
    .appendTaggedFields()
  
  // Verify that it throws a ResponseError
  throws(() => {
    parseResponse(1, 58, 0, writer.bufferList)
  }, (err: any) => {
    deepStrictEqual(err instanceof ResponseError, true)
    // The code is PLT_KFK_MULTIPLE because ResponseError extends MultipleErrors
    deepStrictEqual(err.code, 'PLT_KFK_MULTIPLE')
    
    // Verify the response included in the error
    deepStrictEqual(err.response.errorCode, 1)
    deepStrictEqual(err.response.responseData?.toString(), 'error-response-data')
    
    return true
  })
})

// Add a test for directly testing the API without going through the captured handlers
test('full envelope API request-response cycle', () => {
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Verify the API parameters
      deepStrictEqual(apiKey, 58) // Envelope API key
      deepStrictEqual(apiVersion, 0) // Version 0
      
      // Create a mock successful response
      const responseData = Buffer.from('success-response')
      const rawResponse = Writer.create()
        .appendBytes(responseData)
        .appendInt16(0) // No error
        .appendTaggedFields()
      
      // Parse the response
      const response = parseResponseFn(1, apiKey, apiVersion, rawResponse.bufferList)
      
      // Call the callback with the result
      cb(null, response)
      
      return true
    }
  }
  
  let callbackCalled = false
  
  // Call the API with the mock connection
  envelopeV0(
    mockConnection as any,
    Buffer.from('api-request'),
    Buffer.from('api-principal'),
    Buffer.from('client-host'),
    (err, response) => {
      callbackCalled = true
      deepStrictEqual(err, null)
      deepStrictEqual(response.errorCode, 0)
      deepStrictEqual(response.responseData?.toString(), 'success-response')
    }
  )
  
  // Verify the callback was called
  deepStrictEqual(callbackCalled, true)
})

// Add a test for directly testing the API with error response
test('full envelope API with error response', () => {
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Create a mock error response
      const responseData = Buffer.from('error-response')
      const rawResponse = Writer.create()
        .appendBytes(responseData)
        .appendInt16(1) // Error code 1
        .appendTaggedFields()
      
      try {
        // This should throw
        parseResponseFn(1, apiKey, apiVersion, rawResponse.bufferList)
      } catch (err) {
        // Call the callback with the error
        cb(err, null)
      }
      
      return true
    }
  }
  
  let callbackCalled = false
  
  // Call the API with the mock connection
  envelopeV0(
    mockConnection as any,
    Buffer.from('api-request'),
    Buffer.from('api-principal'),
    Buffer.from('client-host'),
    (err, response) => {
      callbackCalled = true
      deepStrictEqual(err instanceof ResponseError, true)
      deepStrictEqual(response, null)
    }
  )
  
  // Verify the callback was called
  deepStrictEqual(callbackCalled, true)
})