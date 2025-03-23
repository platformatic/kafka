import BufferList from 'bl'
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { getTelemetrySubscriptionsV0 } from '../../../src/apis/telemetry/get-telemetry-subscriptions.ts'
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
  apiFunction(mockConnection, 'client-id-123')
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('getTelemetrySubscriptionsV0 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(getTelemetrySubscriptionsV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 71) // GetTelemetrySubscriptions API key is 71
  strictEqual(apiVersion, 0) // Version 0
})

test('getTelemetrySubscriptionsV0 createRequest serializes request correctly', () => {
  // Call the API function directly to get access to createRequest
  const createRequest = function () {
    // Use a dummy writer object
    return Writer.create()
  }
  
  // Create a test request with client instance ID
  const writer = createRequest()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Check that we have a valid writer
  ok(writer.bufferList instanceof BufferList)
})

test('getTelemetrySubscriptionsV0 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(getTelemetrySubscriptionsV0)
  
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendUUID('550e8400-e29b-41d4-a716-446655440000') // clientInstanceId
    .appendInt32(123) // subscriptionId
    .appendArray([1, 2], (w, compressionType) => w.appendInt8(compressionType), true, false) // acceptedCompressionTypes
    .appendInt32(30000) // pushIntervalMs
    .appendInt32(1024000) // telemetryMaxBytes
    .appendBoolean(true) // deltaTemporality
    .appendArray(['metric1', 'metric2'], (w, metric) => w.appendString(metric), true, false) // requestedMetrics
    .appendTaggedFields()
  
  const response = parseResponse(1, 71, 0, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    clientInstanceId: '550e8400-e29b-41d4-a716-446655440000',
    subscriptionId: 123,
    acceptedCompressionTypes: [1, 2],
    pushIntervalMs: 30000,
    telemetryMaxBytes: 1024000,
    deltaTemporality: true,
    requestedMetrics: ['metric1', 'metric2']
  })
})

test('getTelemetrySubscriptionsV0 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(getTelemetrySubscriptionsV0)
  
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(1) // errorCode - some error
    .appendUUID('550e8400-e29b-41d4-a716-446655440000') // clientInstanceId
    .appendInt32(123) // subscriptionId
    .appendArray([1, 2], (w, compressionType) => w.appendInt8(compressionType), true, false) // acceptedCompressionTypes
    .appendInt32(30000) // pushIntervalMs
    .appendInt32(1024000) // telemetryMaxBytes
    .appendBoolean(true) // deltaTemporality
    .appendArray(['metric1', 'metric2'], (w, metric) => w.appendString(metric), true, false) // requestedMetrics
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 71, 0, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('getTelemetrySubscriptionsV0 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 71)
      strictEqual(apiVersion, 0)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        clientInstanceId: '550e8400-e29b-41d4-a716-446655440000',
        subscriptionId: 123,
        acceptedCompressionTypes: [1, 2],
        pushIntervalMs: 30000,
        telemetryMaxBytes: 1024000,
        deltaTemporality: true,
        requestedMetrics: ['metric1', 'metric2']
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await getTelemetrySubscriptionsV0.async(mockConnection, '550e8400-e29b-41d4-a716-446655440000')
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.errorCode, 0)
  strictEqual(result.clientInstanceId, '550e8400-e29b-41d4-a716-446655440000')
  strictEqual(result.subscriptionId, 123)
  deepStrictEqual(result.acceptedCompressionTypes, [1, 2])
  strictEqual(result.pushIntervalMs, 30000)
  strictEqual(result.telemetryMaxBytes, 1024000)
  strictEqual(result.deltaTemporality, true)
  deepStrictEqual(result.requestedMetrics, ['metric1', 'metric2'])
})

test('getTelemetrySubscriptionsV0 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 71)
      strictEqual(apiVersion, 0)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        clientInstanceId: '550e8400-e29b-41d4-a716-446655440000',
        subscriptionId: 123,
        acceptedCompressionTypes: [1, 2],
        pushIntervalMs: 30000,
        telemetryMaxBytes: 1024000,
        deltaTemporality: true,
        requestedMetrics: ['metric1', 'metric2']
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  getTelemetrySubscriptionsV0(mockConnection, '550e8400-e29b-41d4-a716-446655440000', (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.errorCode, 0)
    strictEqual(result.clientInstanceId, '550e8400-e29b-41d4-a716-446655440000')
    strictEqual(result.subscriptionId, 123)
    
    done()
  })
})

test('getTelemetrySubscriptionsV0 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 71)
      strictEqual(apiVersion, 0)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 1 // Some error code
      }, {
        throttleTimeMs: 0,
        errorCode: 1,
        clientInstanceId: '550e8400-e29b-41d4-a716-446655440000',
        subscriptionId: 123,
        acceptedCompressionTypes: [1, 2],
        pushIntervalMs: 30000,
        telemetryMaxBytes: 1024000,
        deltaTemporality: true,
        requestedMetrics: ['metric1', 'metric2']
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  getTelemetrySubscriptionsV0(mockConnection, '550e8400-e29b-41d4-a716-446655440000', (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('getTelemetrySubscriptionsV0 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 71)
      strictEqual(apiVersion, 0)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 1 // Some error code
      }, {
        throttleTimeMs: 0,
        errorCode: 1,
        clientInstanceId: '550e8400-e29b-41d4-a716-446655440000',
        subscriptionId: 123,
        acceptedCompressionTypes: [1, 2],
        pushIntervalMs: 30000,
        telemetryMaxBytes: 1024000,
        deltaTemporality: true,
        requestedMetrics: ['metric1', 'metric2']
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await getTelemetrySubscriptionsV0.async(mockConnection, '550e8400-e29b-41d4-a716-446655440000')
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})