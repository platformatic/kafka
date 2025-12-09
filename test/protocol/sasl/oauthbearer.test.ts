import { ifError, strictEqual } from 'node:assert'
import test from 'node:test'
import { type SaslAuthenticateResponse } from '../../../src/apis/security/sasl-authenticate-v2.ts'
import {
  type CallbackWithPromise,
  type Connection,
  type saslAuthenticateV2,
  saslOAuthBearer
} from '../../../src/index.ts'

// Test OAuthBearer SASL authentication
test('authenticate should create proper payload with the token - promise', async () => {
  // Create a fake response
  const fakeResponse = {
    errorCode: 0,
    errorMessage: null,
    authBytes: Buffer.from('success'),
    sessionLifetimeMs: 3600000n
  }

  // Create a custom mock for saslAuthenticateV2.async
  let calledCount = 0
  let passedPayload: Buffer | null = null

  // Replace with our mock
  function api (_: Connection, payload: Buffer | null, callback: CallbackWithPromise<SaslAuthenticateResponse>) {
    calledCount++
    passedPayload = payload
    callback(null, fakeResponse)
  }

  // Create mock connection
  const mockConnection = {}

  // Call the authenticate function
  const result = await saslOAuthBearer.authenticate(
    api as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
    mockConnection as any,
    'token',
    {}
  )

  // Verify the function was called
  strictEqual(calledCount, 1, 'saslAuthenticateV2.async should be called once')

  // Verify payload format
  strictEqual(passedPayload!.toString(), 'n,,\x01auth=Bearer token\x01\x01', 'Auth bytes should be the token')

  // Verify result
  strictEqual(result, fakeResponse, 'Response should be passed through')
})

// Test Plain SASL authentication
test('authenticate should create proper payload with the token - callback', (_, done) => {
  // Create a fake response
  const fakeResponse = {
    errorCode: 0,
    errorMessage: null,
    authBytes: Buffer.from('success'),
    sessionLifetimeMs: 3600000n
  }

  // Create a custom mock for saslAuthenticateV2.async
  let calledCount = 0
  let passedPayload: Buffer | null = null

  // Replace with our mock
  function api (_: Connection, payload: Buffer | null, callback: CallbackWithPromise<SaslAuthenticateResponse>) {
    calledCount++
    passedPayload = payload
    callback(null, fakeResponse)
  }

  // Create mock connection
  const mockConnection = {}

  // Call the authenticate function
  saslOAuthBearer.authenticate(
    api as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
    mockConnection as any,
    'token',
    {},
    (error, result) => {
      ifError(error)

      // Verify the function was called
      strictEqual(calledCount, 1, 'saslAuthenticateV2.async should be called once')

      // Verify payload format
      strictEqual(passedPayload!.toString(), 'n,,\x01auth=Bearer token\x01\x01', 'Auth bytes should be the token')

      // Verify result
      strictEqual(result, fakeResponse, 'Response should be passed through')

      done()
    }
  )
})
