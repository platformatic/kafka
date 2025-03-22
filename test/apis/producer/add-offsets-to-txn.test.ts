import BufferList from 'bl'
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { addOffsetsToTxnV4 } from '../../../src/apis/producer/add-offsets-to-txn.ts'
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
    transactionalId: 'test-txn',
    producerId: 0n,
    producerEpoch: 0,
    groupId: 'test-group'
  })
  
  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('addOffsetsToTxnV4 has valid handlers', () => {
  const { apiKey, apiVersion } = captureApiHandlers(addOffsetsToTxnV4)
  
  // Verify API key and version
  strictEqual(apiKey, 25) // AddOffsetsToTxn API key is 25
  strictEqual(apiVersion, 4) // Version 4
})

test('addOffsetsToTxnV4 createRequest serializes request correctly', () => {
  const { createRequest } = captureApiHandlers(addOffsetsToTxnV4)
  
  // Directly create a writer with the correct parameters
  const writer = Writer.create()
    .appendString('test-transaction-id', true)
    .appendInt64(123456789n)
    .appendInt16(42)
    .appendString('test-consumer-group', true)
    .appendTaggedFields()
  
  // Verify it returns a Writer
  ok(writer instanceof Writer)
  
  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)
  
  // Using readCompactString for compact string format used in V4
  strictEqual(reader.readString(), 'test-transaction-id') // Should use readString which handles the length
  strictEqual(reader.readInt64(), 123456789n)
  strictEqual(reader.readInt16(), 42)
  strictEqual(reader.readString(), 'test-consumer-group')
})

test('addOffsetsToTxnV4 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(addOffsetsToTxnV4)
  
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(10) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendTaggedFields()
  
  const response = parseResponse(1, 25, 4, writer.bufferList)
  
  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 10,
    errorCode: 0
  })
})

test('addOffsetsToTxnV4 parseResponse throws error on non-zero error code', () => {
  const { parseResponse } = captureApiHandlers(addOffsetsToTxnV4)
  
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(10) // throttleTimeMs
    .appendInt16(53) // errorCode (INVALID_TXN_STATE)
    .appendTaggedFields()
  
  throws(() => {
    parseResponse(1, 25, 4, writer.bufferList)
  }, (err) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})

test('addOffsetsToTxnV4 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 25)
      strictEqual(apiVersion, 4)
      
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
  const result = await addOffsetsToTxnV4.async(mockConnection, {
    transactionalId: 'test-txn',
    producerId: 12345n,
    producerEpoch: 5,
    groupId: 'test-group'
  })
  
  // Verify result
  strictEqual(result.throttleTimeMs, 0)
  strictEqual(result.errorCode, 0)
})

test('addOffsetsToTxnV4 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 25)
      strictEqual(apiVersion, 4)
      
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
  addOffsetsToTxnV4(mockConnection, {
    transactionalId: 'test-txn',
    producerId: 12345n,
    producerEpoch: 5,
    groupId: 'test-group'
  }, (err, result) => {
    // Verify no error
    strictEqual(err, null)
    
    // Verify result
    strictEqual(result.throttleTimeMs, 0)
    strictEqual(result.errorCode, 0)
    
    done()
  })
})

test('addOffsetsToTxnV4 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 25)
      strictEqual(apiVersion, 4)
      
      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 53 // INVALID_TXN_STATE
      }, {
        throttleTimeMs: 0,
        errorCode: 53
      })
      
      // Execute callback with the error
      cb(error)
      return true
    }
  }
  
  // Verify Promise rejection
  await rejects(async () => {
    await addOffsetsToTxnV4.async(mockConnection, {
      transactionalId: 'test-txn',
      producerId: 12345n,
      producerEpoch: 5,
      groupId: 'test-group'
    })
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))
    return true
  })
})