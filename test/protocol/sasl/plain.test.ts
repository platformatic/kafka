import { strictEqual } from 'node:assert'
import test from 'node:test'
import { type Connection, type saslAuthenticateV2, saslPlain } from '../../../src/index.ts'

// Test Plain SASL authentication
test('authenticate should create proper payload with username and password', async () => {
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
  const api = {
    async (_: Connection, payload: Buffer | null) {
      calledCount++
      passedPayload = payload
      return fakeResponse
    }
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
