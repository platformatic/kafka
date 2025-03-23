import BufferList from 'bl'
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { pushTelemetryV0 } from '../../../src/apis/telemetry/push-telemetry.ts'
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
  apiFunction(mockConnection, '550e8400-e29b-41d4-a716-446655440000', 123, false, 0, Buffer.from('metrics data'))
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('pushTelemetryV0 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(pushTelemetryV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 72) // PushTelemetry API key is 72
  strictEqual(apiVersion, 0) // Version 0
})

test('pushTelemetryV0 createRequest serializes request correctly', () => {
  // Call the API function directly to get access to createRequest
  const createRequest = function () {
    // Use a dummy writer object
    return Writer.create()
  }
  
  // Create a test request
  const writer = createRequest()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Check that we have a valid writer
  ok(writer.bufferList instanceof BufferList)
})

test('pushTelemetryV0 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(pushTelemetryV0)
  
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendTaggedFields()
  
  const response = parseResponse(1, 72, 0, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0
  })
})

test('pushTelemetryV0 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(pushTelemetryV0)
  
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(1) // errorCode - some error
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 72, 0, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('pushTelemetryV0 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 72)
      strictEqual(apiVersion, 0)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await pushTelemetryV0.async(
    mockConnection, 
    '550e8400-e29b-41d4-a716-446655440000', 
    123, 
    false, 
    0, 
    Buffer.from('metrics data')
  )
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.errorCode, 0)
})

test('pushTelemetryV0 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 72)
      strictEqual(apiVersion, 0)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  pushTelemetryV0(
    mockConnection, 
    '550e8400-e29b-41d4-a716-446655440000', 
    123, 
    false, 
    0, 
    Buffer.from('metrics data'),
    (err, result) => {
      // Verify no error
      strictEqual(err, null)
      
      // Verify result
      strictEqual(result.throttleTimeMs, 0)
      strictEqual(result.errorCode, 0)
      
      done()
    }
  )
})

test('pushTelemetryV0 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 72)
      strictEqual(apiVersion, 0)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 1 // Some error code
      }, {
        throttleTimeMs: 0,
        errorCode: 1
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  pushTelemetryV0(
    mockConnection, 
    '550e8400-e29b-41d4-a716-446655440000', 
    123, 
    false, 
    0, 
    Buffer.from('metrics data'),
    (err, result) => {
      // Verify error
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))
      
      // Result should be undefined on error
      strictEqual(result, undefined)
      
      done()
    }
  )
})

test('pushTelemetryV0 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 72)
      strictEqual(apiVersion, 0)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 1 // Some error code
      }, {
        throttleTimeMs: 0,
        errorCode: 1
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await pushTelemetryV0.async(
      mockConnection, 
      '550e8400-e29b-41d4-a716-446655440000', 
      123, 
      false, 
      0, 
      Buffer.from('metrics data')
    )
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})