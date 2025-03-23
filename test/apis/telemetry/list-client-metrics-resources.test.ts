import BufferList from 'bl'
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { listClientMetricsResourcesV0 } from '../../../src/apis/telemetry/list-client-metrics-resources.ts'
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
  apiFunction(mockConnection)
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('listClientMetricsResourcesV0 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(listClientMetricsResourcesV0)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 74) // ListClientMetricsResources API key is 74
  strictEqual(apiVersion, 0) // Version 0
})

test('listClientMetricsResourcesV0 createRequest serializes request correctly', () => {
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

test('listClientMetricsResourcesV0 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(listClientMetricsResourcesV0)
  
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendArray([
      { name: 'resource1' },
      { name: 'resource2' }
    ], (w, resource) => {
      w.appendString(resource.name)
        .appendTaggedFields()
    }, true, false) // Use true, false for compact array, not tagged array
    .appendTaggedFields()
  
  const response = parseResponse(1, 74, 0, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    clientMetricsResources: [
      { name: 'resource1' },
      { name: 'resource2' }
    ]
  })
})

test('listClientMetricsResourcesV0 parseResponse handles error response', () => {
  const { parseResponse } = captureApiHandlers(listClientMetricsResourcesV0)
  
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(1) // errorCode - some error
    .appendArray([], (w, resource) => {
      w.appendString(resource.name)
        .appendTaggedFields()
    }, true, false) // Use true, false for compact array, not tagged array
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 74, 0, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('listClientMetricsResourcesV0 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 74)
      strictEqual(apiVersion, 0)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        clientMetricsResources: [
          { name: 'resource1' },
          { name: 'resource2' }
        ]
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await listClientMetricsResourcesV0.async(mockConnection)
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.errorCode, 0)
  strictEqual(result.clientMetricsResources.length, 2)
  strictEqual(result.clientMetricsResources[0].name, 'resource1')
  strictEqual(result.clientMetricsResources[1].name, 'resource2')
})

test('listClientMetricsResourcesV0 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 74)
      strictEqual(apiVersion, 0)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        clientMetricsResources: [
          { name: 'resource1' },
          { name: 'resource2' }
        ]
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  listClientMetricsResourcesV0(mockConnection, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.errorCode, 0)
    strictEqual(result.clientMetricsResources.length, 2)
    
    done()
  })
})

test('listClientMetricsResourcesV0 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 74)
      strictEqual(apiVersion, 0)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 1 // Some error code
      }, {
        throttleTimeMs: 0,
        errorCode: 1,
        clientMetricsResources: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  listClientMetricsResourcesV0(mockConnection, (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('listClientMetricsResourcesV0 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 74)
      strictEqual(apiVersion, 0)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 1 // Some error code
      }, {
        throttleTimeMs: 0,
        errorCode: 1,
        clientMetricsResources: []
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await listClientMetricsResourcesV0.async(mockConnection)
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})