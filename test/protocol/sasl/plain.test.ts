import { ifError, strictEqual } from 'node:assert'
import test from 'node:test'
import { type SaslAuthenticateResponse } from '../../../src/apis/security/sasl-authenticate-v2.ts'
import { type CallbackWithPromise, type Connection, type saslAuthenticateV2, saslPlain } from '../../../src/index.ts'

// Test Plain SASL authentication
test('authenticate should create proper payload with username and password - promise', async () => {
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
  const result = await saslPlain.authenticate(
    api as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
    mockConnection as any,
    'testuser',
    'testpass'
  )

  // Verify the function was called
  strictEqual(calledCount, 1, 'saslAuthenticateV2.async should be called once')

  // Verify payload format
  const parts = passedPayload!.toString().split('\0')
  strictEqual(parts[0], '', 'First part should be empty')
  strictEqual(parts[1], 'testuser', 'Second part should be the username')
  strictEqual(parts[2], 'testpass', 'Third part should be the password')

  // Verify result
  strictEqual(result, fakeResponse, 'Response should be passed through')
})

// Test Plain SASL authentication
test('authenticate should create proper payload with username and password - callback', (_, done) => {
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
  saslPlain.authenticate(
    api as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
    mockConnection as any,
    'testuser',
    'testpass',
    (...args) => {
      const [error, result] = args
      ifError(error)

      // Verify the function was called
      strictEqual(calledCount, 1, 'saslAuthenticateV2.async should be called once')

      // Verify payload format
      const parts = passedPayload!.toString().split('\0')
      strictEqual(parts[0], '', 'First part should be empty')
      strictEqual(parts[1], 'testuser', 'Second part should be the username')
      strictEqual(parts[2], 'testpass', 'Third part should be the password')

      // Verify result
      strictEqual(result, fakeResponse, 'Response should be passed through')

      done()
    }
  )
})
