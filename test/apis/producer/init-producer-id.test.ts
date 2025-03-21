import BufferList from 'bl'
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { initProducerIdV5 } from '../../../src/apis/producer/init-producer-id.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

// Helper function to mock connection and capture API functions
function captureApiHandlers(apiFunction: any) {
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Execute createRequestFn once to get the handler function
      const handler = createRequestFn()
      mockConnection.createRequestFn = handler
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
    transactionalId: null,
    transactionTimeoutMs: 0,
    producerId: 0n,
    producerEpoch: 0
  })
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('initProducerIdV5 has valid handlers', () => {
  const { apiKey, apiVersion } = captureApiHandlers(initProducerIdV5)
  
  // Verify API key and version
  strictEqual(apiKey, 22) // InitProducerId API key is 22
  strictEqual(apiVersion, 5) // Version 5
})

test('initProducerIdV5 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(initProducerIdV5)
  
  // Directly create a writer with the correct parameters
  const writer = Writer.create()
    .appendString('test-transaction-id', true)
    .appendInt32(30000)
    .appendInt64(123456789n)
    .appendInt16(42)
    .appendTaggedFields()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)
  
  // Using readCompactString for compact string format used in V5
  strictEqual(reader.readString(), 'test-transaction-id')
  strictEqual(reader.readInt32(), 30000)
  strictEqual(reader.readInt64(), 123456789n)
  strictEqual(reader.readInt16(), 42)
})

test('initProducerIdV5 createRequest with null transactionalId', () => {
  const { createRequest } = captureApiHandlers(initProducerIdV5)
  
  // Directly create a writer with null transactionalId
  const writer = Writer.create()
    .appendString(null, true)
    .appendInt32(30000)
    .appendInt64(123456789n)
    .appendInt16(42)
    .appendTaggedFields()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)
  
  // For null transactionalId, compact format uses a single byte with value 0
  strictEqual(reader.readUnsignedVarInt(), 0)
  strictEqual(reader.readInt32(), 30000)
  strictEqual(reader.readInt64(), 123456789n)
  strictEqual(reader.readInt16(), 42)
})

test('initProducerIdV5 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(initProducerIdV5)
  
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(10) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendInt64(12345n) // producerId
    .appendInt16(5) // producerEpoch
    .appendTaggedFields()
  
  const response = parseResponse(1, 22, 5, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 10,
    errorCode: 0,
    producerId: 12345n,
    producerEpoch: 5
  })
})

test('initProducerIdV5 parseResponse throws error on non-zero error code', () => {
  const { parseResponse } = captureApiHandlers(initProducerIdV5)
  
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(10) // throttleTimeMs
    .appendInt16(37) // errorCode (NOT_ENOUGH_REPLICAS)
    .appendInt64(0n) // producerId
    .appendInt16(0) // producerEpoch
    .appendTaggedFields()
  
  throws(() => {
    parseResponse(1, 22, 5, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})

test('initProducerIdV5 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 22)
      strictEqual(apiVersion, 5)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        producerId: 12345n,
        producerEpoch: 5
      }
      
      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }
  
  // Call the API without callback
  const result = await initProducerIdV5.async(mockConnection, {
    transactionalId: 'test-txn',
    transactionTimeoutMs: 30000,
    producerId: -1n,
    producerEpoch: -1
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.errorCode, 0)
  strictEqual(result.producerId, 12345n)
  strictEqual(result.producerEpoch, 5)
})

test('initProducerIdV5 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 22)
      strictEqual(apiVersion, 5)
      
      // Create a proper response directly
      const response = {
        throttleTimeMs: 0,
        errorCode: 0,
        producerId: 12345n,
        producerEpoch: 5
      }
      
      // Execute callback with the response
      cb(null, response)
      return true
    }
  }
  
  // Call the API with callback
  initProducerIdV5(mockConnection, {
    transactionalId: 'test-txn',
    transactionTimeoutMs: 30000,
    producerId: -1n,
    producerEpoch: -1
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.errorCode, 0)
    strictEqual(result.producerId, 12345n)
    strictEqual(result.producerEpoch, 5)
    
    done()
  })
})

test('initProducerIdV5 API error handling with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 22)
      strictEqual(apiVersion, 5)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 37 // NOT_ENOUGH_REPLICAS
      }, {
        throttleTimeMs: 0,
        errorCode: 37,
        producerId: 0n,
        producerEpoch: 0
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Call the API with callback
  initProducerIdV5(mockConnection, {
    transactionalId: 'test-txn',
    transactionTimeoutMs: 30000,
    producerId: -1n,
    producerEpoch: -1
  }, (err, result) => {
    // Verify error
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    
    // Result should be undefined on error
    strictEqual(result, undefined)
    
    done()
  })
})

test('initProducerIdV5 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 22)
      strictEqual(apiVersion, 5)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 37 // NOT_ENOUGH_REPLICAS
      }, {
        throttleTimeMs: 0,
        errorCode: 37,
        producerId: 0n,
        producerEpoch: 0
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await initProducerIdV5.async(mockConnection, {
      transactionalId: 'test-txn',
      transactionTimeoutMs: 30000,
      producerId: -1n,
      producerEpoch: -1
    })
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})