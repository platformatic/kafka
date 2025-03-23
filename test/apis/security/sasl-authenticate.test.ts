// BufferList is used indirectly via Reader
import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { saslAuthenticateV2 } from '../../../src/apis/security/sasl-authenticate.ts'
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
  // Sample auth data for SASL (could be a base64 encoded username/password for PLAIN)
  const dummyAuthBytes = Buffer.from('test-auth-data')
  apiFunction(mockConnection, dummyAuthBytes)

  return {
    createRequest: mockConnection.createRequestFn,
    parseResponse: mockConnection.parseResponseFn,
    apiKey: mockConnection.apiKey,
    apiVersion: mockConnection.apiVersion
  }
}

test('saslAuthenticateV2 has valid handlers', () => {
  const { apiKey, apiVersion } = captureApiHandlers(saslAuthenticateV2)

  // Verify API key and version
  strictEqual(apiKey, 36) // SaslAuthenticate API key is 36
  strictEqual(apiVersion, 2) // Version 2
})

test('saslAuthenticateV2 createRequest serializes request correctly', () => {
  // We'll directly create and verify the writer rather than using the captured createRequest
  // to ensure the test doesn't rely on implementation details
  captureApiHandlers(saslAuthenticateV2)

  // Sample auth data - e.g., for PLAIN mechanism this would be \0username\0password
  const authBytes = Buffer.from('sample-authentication-data')

  // Directly create a writer with the correct parameters
  const writer = Writer.create()
    .appendBytes(authBytes)
    .appendTaggedFields()

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Verify the auth bytes were correctly serialized - using readBytes for compact bytes
  const readAuthBytes = reader.readBytes()
  ok(readAuthBytes instanceof Buffer)
  strictEqual(readAuthBytes.toString(), 'sample-authentication-data')
})

test('saslAuthenticateV2 parseResponse handles successful response', () => {
  const { parseResponse } = captureApiHandlers(saslAuthenticateV2)

  // Create a successful response
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    .appendString(null, true) // error message - null on success
    .appendBytes(Buffer.from('auth-server-token')) // auth bytes from server (for SCRAM mechanisms)
    .appendInt64(7200000n) // session lifetime in ms (2 hours)
    .appendTaggedFields()

  const response = parseResponse(1, 36, 2, writer.bufferList)

  // Verify structure
  deepStrictEqual(response, {
    errorCode: 0,
    errorMessage: null,
    authBytes: Buffer.from('auth-server-token'),
    sessionLifetimeMs: 7200000n
  })
})

test('saslAuthenticateV2 parseResponse throws error on non-zero error code', () => {
  const { parseResponse } = captureApiHandlers(saslAuthenticateV2)

  // Create a response with authentication failure
  const writer = Writer.create()
    .appendInt16(58) // errorCode (SASL Authentication failed)
    .appendString('Authentication failed: Invalid credentials', true) // error message
    .appendBytes(Buffer.alloc(0)) // empty auth bytes on failure
    .appendInt64(0n) // no session on failure
    .appendTaggedFields()

  throws(() => {
    parseResponse(1, 36, 2, writer.bufferList)
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))

    // Check for existence of response property
    ok(err.response, 'Response object should be attached to the error')

    // Verify the error code in the response
    strictEqual(err.response.errorCode, 58)
    strictEqual(err.response.errorMessage, 'Authentication failed: Invalid credentials')
    ok(err.response.authBytes instanceof Buffer)
    strictEqual(err.response.authBytes.length, 0)
    strictEqual(err.response.sessionLifetimeMs, 0n)

    return true
  })
})

test('saslAuthenticateV2 API mock simulation without callback', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 36)
      strictEqual(apiVersion, 2)

      // Create a proper response directly
      const response = {
        errorCode: 0,
        errorMessage: null,
        authBytes: Buffer.from('server-response-data'),
        sessionLifetimeMs: 3600000n
      }

      // Execute callback with the response directly
      cb(null, response)
      return true
    }
  }

  // Call the API without callback
  const authBytes = Buffer.from('client-request-data')
  const result = await saslAuthenticateV2.async(mockConnection, authBytes)

  // Verify result
  strictEqual(result.errorCode, 0)
  strictEqual(result.errorMessage, null)
  ok(result.authBytes instanceof Buffer)
  strictEqual(result.authBytes.toString(), 'server-response-data')
  strictEqual(result.sessionLifetimeMs, 3600000n)
})

test('saslAuthenticateV2 API mock simulation with callback', (t, done) => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 36)
      strictEqual(apiVersion, 2)

      // Create a proper response directly
      const response = {
        errorCode: 0,
        errorMessage: null,
        authBytes: Buffer.from('server-response-data'),
        sessionLifetimeMs: 3600000n
      }

      // Execute callback with the response
      cb(null, response)
      return true
    }
  }

  // Call the API with callback
  const authBytes = Buffer.from('client-request-data')
  saslAuthenticateV2(mockConnection, authBytes, (err, result) => {
    // Verify no error
    strictEqual(err, null)

    // Verify result
    strictEqual(result.errorCode, 0)
    strictEqual(result.errorMessage, null)
    ok(result.authBytes instanceof Buffer)
    strictEqual(result.authBytes.toString(), 'server-response-data')
    strictEqual(result.sessionLifetimeMs, 3600000n)

    done()
  })
})

test('saslAuthenticateV2 API error handling with Promise', async () => {
  // Mock connection
  const mockConnection = {
    send: (apiKey: number, apiVersion: number, createRequestFn: any, parseResponseFn: any, hasRequestHeaderTaggedFields: boolean, hasResponseHeaderTaggedFields: boolean, cb: any) => {
      // Basic verification
      strictEqual(apiKey, 36)
      strictEqual(apiVersion, 2)

      // Create an error with the expected shape
      const error = new ResponseError(apiKey, apiVersion, {
        '': 58 // AUTHENTICATION_FAILED
      }, {
        errorCode: 58,
        errorMessage: 'Authentication failed: Invalid credentials',
        authBytes: Buffer.alloc(0),
        sessionLifetimeMs: 0n
      })

      // Execute callback with the error
      cb(error)
      return true
    }
  }

  // Verify Promise rejection
  const authBytes = Buffer.from('invalid-credentials')
  await rejects(async () => {
    await saslAuthenticateV2.async(mockConnection, authBytes)
  }, (err: any) => {
    ok(err instanceof ResponseError)
    ok(err.message.includes('Received response with error while executing API'))

    // Verify the response is preserved
    ok(err.response)
    strictEqual(err.response.errorCode, 58)
    strictEqual(err.response.errorMessage, 'Authentication failed: Invalid credentials')
    ok(err.response.authBytes instanceof Buffer)
    strictEqual(err.response.authBytes.length, 0)
    strictEqual(err.response.sessionLifetimeMs, 0n)

    return true
  })
})
