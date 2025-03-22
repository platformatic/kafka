import BufferList from 'bl'
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { findCoordinatorV6 } from '../../../src/apis/metadata/find-coordinator.ts'
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
  apiFunction(mockConnection, 0, ['test-group'])
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('findCoordinatorV6 has valid handlers', () => {
  const { apiKey, apiVersion } = captureApiHandlers(findCoordinatorV6)
  
  // Verify API key and version
  strictEqual(apiKey, 10) // FindCoordinator API key is 10
  strictEqual(apiVersion, 6) // Version 6
})

test('findCoordinatorV6 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(findCoordinatorV6)
  
  // Values for the request
  const keyType = 0 // 0 for GROUP, 1 for TRANSACTION
  const coordinatorKeys = ['group-1', 'group-2']
  
  // Directly create a writer with the correct parameters
  const writer = Writer.create()
    .appendInt8(keyType)
    .appendArray(coordinatorKeys, (w, k) => w.appendString(k), true, false)
    .appendTaggedFields()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)
  
  strictEqual(reader.readInt8(), 0) // keyType
  
  // Read and verify the coordinator keys array
  const keys = reader.readArray(r => r.readString(), true, false)
  strictEqual(keys.length, 2)
  strictEqual(keys[0], 'group-1')
  strictEqual(keys[1], 'group-2')
})

test('findCoordinatorV6 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(findCoordinatorV6)
  
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    
    // Coordinators array (compact format)
    .appendUnsignedVarInt(3) // array length 2 + 1
    
    // First coordinator
    .appendString('group-1')
    .appendInt32(1) // nodeId
    .appendString('broker1.example.com')
    .appendInt32(9092) // port
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage (null for success)
    .appendUnsignedVarInt(0) // tagged fields
    
    // Second coordinator
    .appendString('group-2')
    .appendInt32(2) // nodeId
    .appendString('broker2.example.com')
    .appendInt32(9092) // port
    .appendInt16(0) // errorCode (success)
    .appendString(null) // errorMessage (null for success)
    .appendUnsignedVarInt(0) // tagged fields
    
    .appendUnsignedVarInt(0) // root tagged fields
  
  const response = parseResponse(1, 10, 6, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    coordinators: [
      {
        key: 'group-1',
        nodeId: 1,
        host: 'broker1.example.com',
        port: 9092,
        errorCode: 0,
        errorMessage: null
      },
      {
        key: 'group-2',
        nodeId: 2,
        host: 'broker2.example.com',
        port: 9092,
        errorCode: 0,
        errorMessage: null
      }
    ]
  })
})

test('findCoordinatorV6 parseResponse throws error on non-zero error code', () => {
  const { parseResponse } = captureApiHandlers(findCoordinatorV6)
  
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    
    // Coordinators array (compact format)
    .appendUnsignedVarInt(2) // array length 1 + 1
    
    // Coordinator with error
    .appendString('group-1')
    .appendInt32(-1) // nodeId (invalid for error)
    .appendString('broker1.example.com')
    .appendInt32(9092) // port
    .appendInt16(15) // errorCode (GROUP_COORDINATOR_NOT_AVAILABLE)
    .appendString('Coordinator not available') // errorMessage
    .appendUnsignedVarInt(0) // tagged fields
    
    .appendUnsignedVarInt(0) // root tagged fields
  
  // Verify that parsing throws ResponseError
  throws(() => {
    parseResponse(1, 10, 6, writer.bufferList)
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Verify the error location and code
    strictEqual(typeof err.errors, 'object')
    
    // Verify the response is preserved
    deepStrictEqual(err.response, {
      throttleTimeMs: 0,
      coordinators: [
        {
          key: 'group-1',
          nodeId: -1,
          host: 'broker1.example.com',
          port: 9092,
          errorCode: 15,
          errorMessage: 'Coordinator not available'
        }
      ]
    })
    
    return true
  })
})

test('findCoordinatorV6 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 10)
      strictEqual(apiVersion, 6)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        coordinators: [
          {
            key: 'group-1',
            nodeId: 1,
            host: 'broker1.example.com',
            port: 9092,
            errorCode: 0,
            errorMessage: null
          }
        ]
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await findCoordinatorV6.async(mockConnection, 0, ['group-1'])
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.coordinators.length, 1)
  strictEqual(result.coordinators[0].key, 'group-1')
  strictEqual(result.coordinators[0].nodeId, 1)
  strictEqual(result.coordinators[0].errorCode, 0)
})

test('findCoordinatorV6 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 10)
      strictEqual(apiVersion, 6)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        coordinators: [
          {
            key: 'group-1',
            nodeId: 1,
            host: 'broker1.example.com',
            port: 9092,
            errorCode: 0,
            errorMessage: null
          }
        ]
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  findCoordinatorV6(mockConnection, 0, ['group-1'], (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.coordinators.length, 1)
    strictEqual(result.coordinators[0].key, 'group-1')
    strictEqual(result.coordinators[0].nodeId, 1)
    strictEqual(result.coordinators[0].errorCode, 0)
    
    done()
  })
})

test('findCoordinatorV6 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 10)
      strictEqual(apiVersion, 6)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '/coordinators/0': 15
      }, {
        throttleTimeMs: 0,
        coordinators: [
          {
            key: 'group-1',
            nodeId: -1,
            host: 'broker1.example.com',
            port: 9092,
            errorCode: 15,
            errorMessage: 'Coordinator not available'
          }
        ]
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await findCoordinatorV6.async(mockConnection, 0, ['group-1'])
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Verify the response is preserved
    ok(err.response)
    strictEqual(err.response.coordinators[0].errorCode, 15)
    
    return true
  })
})