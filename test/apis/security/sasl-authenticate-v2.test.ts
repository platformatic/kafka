import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, saslAuthenticateV2, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = saslAuthenticateV2

test('createRequest serializes auth bytes correctly', () => {
  // Sample auth data - e.g., for PLAIN mechanism this would be \0username\0password
  const authBytes = Buffer.from('sample-authentication-data')

  const writer = createRequest(authBytes)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Verify the complete request structure with deepStrictEqual
  deepStrictEqual(
    {
      authBytes: reader.readBytes()
    },
    {
      authBytes
    },
    'Serialized request should match expected structure'
  )
})

test('createRequest handles empty auth bytes', () => {
  // Empty auth data - edge case
  const authBytes = Buffer.alloc(0)

  const writer = createRequest(authBytes)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Verify the complete request structure with deepStrictEqual
  deepStrictEqual(
    {
      authBytes: reader.readBytes()
    },
    {
      authBytes // empty buffer
    },
    'Serialized request with empty buffer should match expected structure'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    .appendString(null, true) // error message - null on success
    .appendBytes(Buffer.from('auth-server-token')) // auth bytes from server (for SCRAM mechanisms)
    .appendInt64(7200000n) // session lifetime in ms (2 hours)
    .appendTaggedFields()

  const response = parseResponse(1, 36, 2, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    errorCode: 0,
    errorMessage: null,
    authBytes: Buffer.from('auth-server-token'),
    sessionLifetimeMs: 7200000n
  })
})

test('parseResponse handles response with detailed error message', () => {
  // Create a response with error details
  const writer = Writer.create()
    .appendInt16(58) // errorCode (SASL authentication failed)
    .appendString('Authentication failed: Invalid credentials', true) // error message
    .appendBytes(Buffer.alloc(0)) // empty auth bytes on failure
    .appendInt64(0n) // no session on failure
    .appendTaggedFields()

  throws(
    () => {
      parseResponse(1, 36, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check for existence of response property
      ok(err.response, 'Response object should be attached to the error')

      // Verify the response structure is preserved
      deepStrictEqual(err.response, {
        errorCode: 58,
        errorMessage: 'Authentication failed: Invalid credentials',
        authBytes: Buffer.alloc(0),
        sessionLifetimeMs: 0n
      })

      return true
    }
  )
})
