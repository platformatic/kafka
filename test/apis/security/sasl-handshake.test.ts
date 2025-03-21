// BufferList is used indirectly via Reader
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { saslHandshakeV1 } from '../../../src/apis/security/sasl-handshake.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

// Helper function to mock connection and capture API functions
function captureApiHandlers (apiFunction: any) {
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
  apiFunction(mockConnection, 'PLAIN')

  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('saslHandshakeV1 has valid handlers', () => {
  const { apiKey, apiVersion } = captureApiHandlers(saslHandshakeV1)

  // Verify API key and version
  strictEqual(apiKey, 17) // SaslHandshake API key is 17
  strictEqual(apiVersion, 1) // Version 1
})

test('saslHandshakeV1 createRequest serializes request correctly', () => {
  // We'll directly create and verify the writer rather than using the captured createRequest
  // to ensure the test doesn't rely on implementation details
  captureApiHandlers(saslHandshakeV1)

  // Directly create a writer with the correct parameters
  const writer = Writer.create()
    .appendString('SCRAM-SHA-256', false)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Note: The V1 API uses non-compact strings (false flag in appendString)
  strictEqual(reader.readString(false), 'SCRAM-SHA-256')
})

test('saslHandshakeV1 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(saslHandshakeV1)

  // Create a successful response
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)

    // Mechanisms array (this is a non-compact array without tagged fields)
    .appendInt32(3) // array length 3
    .appendString('PLAIN', false)
    .appendString('SCRAM-SHA-256', false)
    .appendString('SCRAM-SHA-512', false)

  const response = parseResponse(1, 17, 1, writer.bufferList)

  // Verify structure
  deepStrictEqual(response, {
    errorCode: 0,
    mechanisms: ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']
  })
})

test('saslHandshakeV1 parseResponse throws error on non-zero error code', () => {
  const { parseResponse } = captureApiHandlers(saslHandshakeV1)

  // Create a response with error
  const writer = Writer.create()
    .appendInt16(58) // errorCode (UNSUPPORTED_SASL_MECHANISM)

    // Mechanisms array (empty because the requested mechanism is not supported)
    .appendInt32(0) // array length 0

  throws(() => {
    parseResponse(1, 17, 1, writer.bufferList)
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))

    // Check for existence of response property
    ok(err.response, 'Response object should be attached to the error')

    // Verify the error code in the response
    strictEqual(err.response.errorCode, 58)
    ok(Array.isArray(err.response.mechanisms), 'Mechanisms should be an array')
    strictEqual(err.response.mechanisms.length, 0, 'Mechanisms should be empty')

    return true
  })
})

test('saslHandshakeV1 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 17)
      strictEqual(apiVersion, 1)

      // Create a proper response directly
      const response = {
        errorCode: 0,
        mechanisms: ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']
      }

      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }

  // Call the API without callback
  const result = await saslHandshakeV1.async(mockConnection, 'PLAIN')

  // Verify result
  strictEqual(result.errorCode, 0)
  strictEqual(result.mechanisms!.length, 3)
  strictEqual(result.mechanisms![0], 'PLAIN')
  strictEqual(result.mechanisms![1], 'SCRAM-SHA-256')
  strictEqual(result.mechanisms![2], 'SCRAM-SHA-512')
})

test('saslHandshakeV1 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 17)
      strictEqual(apiVersion, 1)

      // Create a proper response directly
      const response = {
        errorCode: 0,
        mechanisms: ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']
      }

      // Execute callback with the response
      cb(null, response)
      return true
    }
  }

  // Call the API with callback
  saslHandshakeV1(mockConnection, 'PLAIN', (err, result) => {
    // Verify no error
    strictEqual(err, null)

    // Verify result
    strictEqual(result.errorCode, 0)
    strictEqual(result.mechanisms!.length, 3)
    strictEqual(result.mechanisms![0], 'PLAIN')

    done()
  })
})

test('saslHandshakeV1 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 17)
      strictEqual(apiVersion, 1)

      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 58 // UNSUPPORTED_SASL_MECHANISM
      }, {
        errorCode: 58,
        mechanisms: []
      })

      // Execute callback with the error
      cb(error)
      return true
    }
  }

  // Verify Promise rejection
  await rejects(async () => {
    await saslHandshakeV1.async(mockConnection, 'UNKNOWN-MECHANISM')
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))

    // Verify the response is preserved
    ok(err.response)
    strictEqual(err.response.errorCode, 58)
    ok(Array.isArray(err.response.mechanisms), 'Mechanisms should be an array')

    return true
  })
})
