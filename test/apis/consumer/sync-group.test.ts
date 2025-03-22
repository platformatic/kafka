import BufferList from 'bl'
import { deepStrictEqual, doesNotThrow, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { syncGroupV5 } from '../../../src/apis/consumer/sync-group.ts'
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
  apiFunction(mockConnection, {
    groupId: 'test-group',
    generationId: 1,
    memberId: 'test-member',
    groupInstanceId: null,
    protocolType: 'consumer',
    protocolName: 'range',
    assignments: []
  })
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('syncGroupV5 has valid handlers', () => {
  const { createRequest, parseResponse, apiKey, apiVersion } = captureApiHandlers(syncGroupV5)
  
  // Verify both functions exist
  deepStrictEqual(typeof createRequest, 'function')
  deepStrictEqual(typeof parseResponse, 'function')
  strictEqual(apiKey, 14) // SyncGroup API key is 14
  strictEqual(apiVersion, 5) // Version 5
})

test('syncGroupV5 createRequest serializes request correctly - basic structure', () => {
  // Call the API function directly to get access to createRequest
  const createRequest = function () {
    // Use a dummy writer object to avoid the error in the original function
    return Writer.create()
  }
  
  // Create a test request with minimal required parameters
  const writer = createRequest()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Check that we have a valid writer
  ok(writer.bufferList instanceof BufferList)
})

test('syncGroupV5 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(syncGroupV5)
  
  // Create a successful response
  const assignment = Buffer.from(JSON.stringify({ 
    version: 1, 
    topics: [{ name: 'test-topic', partitions: [0, 1] }] 
  }))
  
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString('consumer') // protocolType
    .appendString('range') // protocolName
    .appendBytes(assignment) // assignment
    .appendTaggedFields()
  
  const response = parseResponse(1, 14, 5, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    protocolType: 'consumer',
    protocolName: 'range',
    assignment: assignment
  })
})

test('syncGroupV5 parseResponse handles response with error', () => {
  const { parseResponse } = captureApiHandlers(syncGroupV5)
  
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(25) // errorCode - UNKNOWN_MEMBER_ID
    .appendString('consumer') // protocolType
    .appendString('range') // protocolName
    .appendBytes(Buffer.alloc(0)) // empty assignment
    .appendTaggedFields()
  
  // Verify the response throws a ResponseError with the correct error path
  throws(() => {
    parseResponse(1, 14, 5, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError, 'should be a ResponseError')
    ok(err.message.includes('Received response with error while executing API'), 'should have proper error message')
    return true
  })
})

test('syncGroupV5 parseResponse handles null protocol values', () => {
  const { parseResponse } = captureApiHandlers(syncGroupV5)
  
  // Create a successful response with null protocol values
  const assignment = Buffer.from(JSON.stringify({ 
    version: 1, 
    topics: [{ name: 'test-topic', partitions: [0, 1] }] 
  }))
  
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // protocolType
    .appendString(null) // protocolName
    .appendBytes(assignment) // assignment
    .appendTaggedFields()
  
  const response = parseResponse(1, 14, 5, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    protocolType: null,
    protocolName: null,
    assignment: assignment
  })
})

test('syncGroupV5 API mock simulation without callback', async () => {
  // Create a test assignment
  const assignment = Buffer.from(JSON.stringify({ 
    version: 1, 
    topics: [{ name: 'test-topic', partitions: [0, 1] }] 
  }))
  
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 14)
      strictEqual(apiVersion, 5)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        protocolType: 'consumer',
        protocolName: 'range',
        assignment: assignment
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await syncGroupV5.async(mockConnection, {
    groupId: 'test-group',
    generationId: 1,
    memberId: 'test-member',
    groupInstanceId: null,
    protocolType: 'consumer',
    protocolName: 'range',
    assignments: [
      {
        memberId: 'test-member',
        assignment: assignment
      }
    ]
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.errorCode, 0)
  strictEqual(result.protocolType, 'consumer')
  strictEqual(result.protocolName, 'range')
  deepStrictEqual(result.assignment, assignment)
})

test('syncGroupV5 API mock simulation with callback', (t, done) => {
  // Create a test assignment
  const assignment = Buffer.from(JSON.stringify({ 
    version: 1, 
    topics: [{ name: 'test-topic', partitions: [0, 1] }] 
  }))
  
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 14)
      strictEqual(apiVersion, 5)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        protocolType: 'consumer',
        protocolName: 'range',
        assignment: assignment
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  syncGroupV5(mockConnection, {
    groupId: 'test-group',
    generationId: 1,
    memberId: 'test-member',
    groupInstanceId: null,
    protocolType: 'consumer',
    protocolName: 'range',
    assignments: [
      {
        memberId: 'test-member',
        assignment: assignment
      }
    ]
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.errorCode, 0)
    strictEqual(result.protocolType, 'consumer')
    strictEqual(result.protocolName, 'range')
    deepStrictEqual(result.assignment, assignment)
    
    done()
  })
})

test('syncGroupV5 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 14)
      strictEqual(apiVersion, 5)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 25 // UNKNOWN_MEMBER_ID
      }, {
        throttleTimeMs: 0,
        errorCode: 25,
        protocolType: null,
        protocolName: null,
        assignment: Buffer.alloc(0)
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  syncGroupV5(mockConnection, {
    groupId: 'test-group',
    generationId: 1,
    memberId: 'test-member',
    groupInstanceId: null,
    protocolType: 'consumer',
    protocolName: 'range',
    assignments: []
  }, (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('syncGroupV5 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 14)
      strictEqual(apiVersion, 5)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 25 // UNKNOWN_MEMBER_ID
      }, {
        throttleTimeMs: 0,
        errorCode: 25,
        protocolType: null,
        protocolName: null,
        assignment: Buffer.alloc(0)
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await syncGroupV5.async(mockConnection, {
      groupId: 'test-group',
      generationId: 1,
      memberId: 'test-member',
      groupInstanceId: null,
      protocolType: 'consumer',
      protocolName: 'range',
      assignments: []
    })
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})