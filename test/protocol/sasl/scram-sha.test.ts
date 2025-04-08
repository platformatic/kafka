import { deepStrictEqual, match, ok, rejects, strictEqual } from 'node:assert'
import test from 'node:test'
import { AuthenticationError, type Connection, type saslAuthenticateV2, saslScramSha } from '../../../src/index.ts'

const { authenticate, createNonce, h, hi, hmac, parseParameters, sanitizeString, ScramAlgorithms, xor } = saslScramSha

// Test the ScramAlgorithms constants are properly defined
test('ScramAlgorithms should have correct definitions', () => {
  // SHA-256
  strictEqual(ScramAlgorithms['SHA-256'].keyLength, 32)
  strictEqual(ScramAlgorithms['SHA-256'].algorithm, 'sha256')
  strictEqual(ScramAlgorithms['SHA-256'].minIterations, 4096)

  // SHA-512
  strictEqual(ScramAlgorithms['SHA-512'].keyLength, 64)
  strictEqual(ScramAlgorithms['SHA-512'].algorithm, 'sha512')
  strictEqual(ScramAlgorithms['SHA-512'].minIterations, 4096)
})

// Test createNonce function
test('createNonce should generate a random string encoded in base64url', () => {
  const nonce1 = createNonce()
  const nonce2 = createNonce()

  // Verify nonces are different
  ok(nonce1 !== nonce2, 'Nonces should be unique')

  // Verify format (base64url encoding with length of 22-24 chars for 16 bytes)
  match(nonce1, /^[A-Za-z0-9_-]{22,24}$/, 'Nonce should be base64url encoded')
  match(nonce2, /^[A-Za-z0-9_-]{22,24}$/, 'Nonce should be base64url encoded')
})

// Test sanitizeString function
test('sanitizeString should escape special characters', () => {
  strictEqual(sanitizeString('normal'), 'normal', 'Normal string should not be modified')
  strictEqual(sanitizeString('a=b'), 'a=3Db', 'Equal sign should be replaced with =3D')
  strictEqual(sanitizeString('a,b'), 'a=2Cb', 'Comma should be replaced with =2C')
  strictEqual(sanitizeString('a=b,c'), 'a=3Db=2Cc', 'Multiple special chars should be replaced')
  strictEqual(sanitizeString('a=b=c,d'), 'a=3Db=3Dc=2Cd', 'Multiple occurrences should be replaced')
})

// Test parseParameters function
test('parseParameters should parse comma-separated key=value pairs', () => {
  // Use only valid parameters with the expected format key=value
  const input = Buffer.from('a=1,b=2,c=value')
  const result = parseParameters(input)

  // Check parsed values
  strictEqual(result.a, '1')
  strictEqual(result.b, '2')
  strictEqual(result.c, 'value')
  strictEqual(result.__original, 'a=1,b=2,c=value')
})

// Test h (hash) function
test('h should hash data using the algorithm from the definition', () => {
  const sha256Def = ScramAlgorithms['SHA-256']
  const sha512Def = ScramAlgorithms['SHA-512']
  const testData = 'test'

  // SHA-256 should produce a 32-byte hash
  const hash256 = h(sha256Def, testData)
  strictEqual(hash256.length, 32, 'SHA-256 should produce a 32-byte hash')

  // SHA-512 should produce a 64-byte hash
  const hash512 = h(sha512Def, testData)
  strictEqual(hash512.length, 64, 'SHA-512 should produce a 64-byte hash')

  // Should produce consistent results for same input
  deepStrictEqual(h(sha256Def, testData), h(sha256Def, testData), 'Hash should be consistent')

  // Should work with Buffer input
  const bufferHash = h(sha256Def, Buffer.from(testData))
  deepStrictEqual(bufferHash, hash256, 'Should produce same result for string or Buffer input')
})

// Test hi (PBKDF2) function
test('hi should derive key using PBKDF2', () => {
  const sha256Def = ScramAlgorithms['SHA-256']
  const password = 'password'
  const salt = Buffer.from('salt')
  const iterations = 1

  // Should return a buffer of the expected length
  const key = hi(sha256Def, password, salt, iterations)
  strictEqual(key.length, sha256Def.keyLength, 'Key length should match algorithm definition')

  // Different iterations should produce different results
  const key2 = hi(sha256Def, password, salt, 2)
  ok(!key.equals(key2), 'Different iterations should produce different keys')

  // Different definitions should produce different results
  const key512 = hi(ScramAlgorithms['SHA-512'], password, salt, iterations)
  strictEqual(key512.length, ScramAlgorithms['SHA-512'].keyLength, 'Key length should match SHA-512 definition')
  ok(!key.equals(key512), 'Different algorithms should produce different keys')
})

// Test hmac function
test('hmac should create HMAC using the algorithm from the definition', () => {
  const sha256Def = ScramAlgorithms['SHA-256']
  const key = Buffer.from('key')
  const data = 'data'

  // Should return buffer of the expected length
  const result = hmac(sha256Def, key, data)
  strictEqual(result.length, 32, 'SHA-256 HMAC should be 32 bytes')

  // Should be consistent for same inputs
  deepStrictEqual(hmac(sha256Def, key, data), hmac(sha256Def, key, data), 'HMAC should be consistent')

  // Should work with Buffer data
  deepStrictEqual(
    hmac(sha256Def, key, data),
    hmac(sha256Def, key, Buffer.from(data)),
    'Should handle both string and Buffer data'
  )

  // Different definitions should produce different results
  const hmac512 = hmac(ScramAlgorithms['SHA-512'], key, data)
  strictEqual(hmac512.length, 64, 'SHA-512 HMAC should be 64 bytes')
})

// Test xor function
test('xor should XOR two buffers', () => {
  // Simple test case: XOR of same bytes is 0
  const a = Buffer.from([1, 2, 3])
  const b = Buffer.from([1, 2, 3])
  const result = xor(a, b)

  deepStrictEqual(result, Buffer.from([0, 0, 0]), 'XOR of same values should be zeros')

  // More complex case
  const c = Buffer.from([0x0f, 0xf0, 0x55])
  const d = Buffer.from([0xf0, 0x0f, 0xaa])
  const expected = Buffer.from([0xff, 0xff, 0xff]) // 0x0F ^ 0xF0 = 0xFF, etc.

  deepStrictEqual(xor(c, d), expected, 'XOR result should be computed correctly')
})

// Test xor throws when buffer lengths don't match
test('xor should throw when buffer lengths are different', () => {
  const a = Buffer.from([1, 2, 3])
  const b = Buffer.from([1, 2])

  try {
    xor(a, b)
    // Should not reach here
    ok(false, 'xor should have thrown an error')
  } catch (err) {
    ok(err instanceof AuthenticationError, 'Should throw AuthenticationError')
    strictEqual((err as Error).message, 'Buffers must have the same length.')
  }
})

// Test authenticate should verify minimum iterations
test('authenticate should check for minimum required iterations', async () => {
  // Replace with implementation that returns matching nonce but too few iterations
  let callCount = 0
  const api = {
    async (_: Connection, payload: Buffer | null) {
      callCount++

      if (callCount === 1) {
        // Extract client nonce from first message
        const clientNonce = payload!.toString().split('r=')[1]

        // First call - return server response with matching nonce but too few iterations
        return {
          errorCode: 0,
          errorMessage: null,
          authBytes: Buffer.from(`r=${clientNonce}server,s=c2FsdA==,i=100`), // i=100 is below minimum
          sessionLifetimeMs: 3600000n
        }
      }

      // Should not get here
      return {} as any
    }
  }

  const mockConnection = {}

  // Should throw appropriate error
  await rejects(
    async () => {
      await authenticate(
        api as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
        mockConnection as any,
        'SHA-256',
        'testuser',
        'testpass'
      )
    },
    (err: Error) => {
      strictEqual(err instanceof AuthenticationError, true)
      strictEqual(err.message, 'Algorithm SHA-256 requires at least 4096 iterations, while 100 were requested.')
      return true
    }
  )
})

// Test error handling for unsupported algorithms
test('authenticate should throw for unsupported algorithm', async () => {
  const mockConnection = {}

  // Try with an invalid algorithm
  await rejects(
    async () => {
      await authenticate(
        {} as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
        mockConnection as any,
        'INVALID-ALGO' as any,
        'testuser',
        'testpass'
      )
    },
    (err: Error) => {
      strictEqual(err instanceof AuthenticationError, true)
      strictEqual(err.message, 'Unsupported SCRAM algorithm INVALID-ALGO')
      return true
    }
  )
})

// Test we can send a message and extract the format
test('authenticate should include username in initial message', async () => {
  let firstMessage: string = ''

  // Replace the saslAuthenticateV2 function to catch the first message
  const api = {
    async (_: Connection, payload: Buffer | null) {
      firstMessage = payload!.toString()

      // Throw error to stop the authentication process
      throw new Error('Stop the test early')
    }
  }

  const mockConnection = {}

  // Call authenticate, expecting it to fail
  try {
    await authenticate(
      api as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
      mockConnection as any,
      'SHA-256',
      'testuser',
      'testpass'
    )
  } catch (err) {
    // Expected error, ignore it
  }

  // Just check it has the username
  ok(firstMessage.includes('n=testuser'), 'Initial message should include username')
})

// Test special character escaping in username
test('authenticate should escape special characters in username', async () => {
  let firstMessage: string = ''

  // Replace the saslAuthenticateV2 function to catch the first message
  const api = {
    async (_: Connection, payload: Buffer | null) {
      firstMessage = payload!.toString()

      // Throw error to stop the authentication process
      throw new Error('Stop the test early')
    }
  }

  const mockConnection = {}

  // Call authenticate with special characters in username, expecting it to fail
  try {
    await authenticate(
      api as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
      mockConnection as any,
      'SHA-256',
      'user=with,chars',
      'testpass'
    )
  } catch (err) {
    // Expected error, ignore it
  }

  // Verify special characters are escaped
  const escapedEqual = '=3D' // = becomes =3D
  const escapedComma = '=2C' // , becomes =2C
  ok(firstMessage.includes(escapedEqual), 'Equal sign should be escaped')
  ok(firstMessage.includes(escapedComma), 'Comma should be escaped')
})

// Test error handling for server nonce mismatch
test('authenticate should check server nonce starts with client nonce', async () => {
  // Replace with implementation that returns invalid nonce
  const api = {
    async (_connection: Connection, _buffer: Buffer | null) {
      // First call - return server response with different nonce
      return {
        errorCode: 0,
        errorMessage: null,
        authBytes: Buffer.from('r=different-nonce,s=c2FsdA==,i=4096'),
        sessionLifetimeMs: 3600000n
      }
    }
  }

  const mockConnection = {}

  // Should throw appropriate error
  await rejects(
    async () => {
      await authenticate(
        api as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
        mockConnection as any,
        'SHA-256',
        'testuser',
        'testpass'
      )
    },
    (err: Error) => {
      strictEqual(err instanceof AuthenticationError, true)
      strictEqual(err.message, 'Server nonce does not start with client nonce.')
      return true
    }
  )
})

// Test handling of error messages from the server
test('authenticate should handle server error messages', async () => {
  // Mock function with two different responses
  let callCount = 0

  // Replace with implementation that returns error in second response
  const api = {
    async (_: Connection, payload: Buffer | null) {
      callCount++

      if (callCount === 1) {
        // First call - normal response with a nonce that matches client-nonce
        // Extract client nonce from payload (format: "n,,n=user,r=client-nonce")
        const clientNonce = payload!.toString().split('r=')[1]

        return {
          errorCode: 0,
          errorMessage: null,
          authBytes: Buffer.from(`r=${clientNonce}1234,s=c2FsdA==,i=4096`),
          sessionLifetimeMs: 3600000n
        }
      } else {
        // Second call - return error
        return {
          errorCode: 0,
          errorMessage: null,
          authBytes: Buffer.from('e=Authentication failed'),
          sessionLifetimeMs: 3600000n
        }
      }
    }
  }

  const mockConnection = {}

  // Should throw with server's error message
  await rejects(
    async () => {
      await authenticate(
        api as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
        mockConnection as any,
        'SHA-256',
        'testuser',
        'testpass'
      )
    },
    (err: Error) => {
      strictEqual(err instanceof AuthenticationError, true)
      strictEqual(err.message, 'Authentication failed')
      return true
    }
  )
})

// Test validate server signature
test('authenticate should check server signature validity', async () => {
  // Mock function with two different responses
  let callCount = 0

  // Replace with implementation that returns invalid server signature
  const api = {
    async (_: Connection, payload: Buffer | null) {
      callCount++

      if (callCount === 1) {
        // First call - normal response with a nonce that matches client-nonce
        // Extract client nonce from payload
        const clientNonce = payload!.toString().split('r=')[1]

        return {
          errorCode: 0,
          errorMessage: null,
          authBytes: Buffer.from(`r=${clientNonce}1234,s=c2FsdA==,i=4096`),
          sessionLifetimeMs: 3600000n
        }
      } else {
        // Second call - return invalid signature
        return {
          errorCode: 0,
          errorMessage: null,
          // v= is the server signature verification - incorrect value
          authBytes: Buffer.from('v=invalidSignature'),
          sessionLifetimeMs: 3600000n
        }
      }
    }
  }

  const mockConnection = {}

  // Should throw about invalid server signature
  await rejects(
    async () => {
      await authenticate(
        api as unknown as saslAuthenticateV2.SASLAuthenticationAPI,
        mockConnection as any,
        'SHA-256',
        'testuser',
        'testpass'
      )
    },
    (err: Error) => {
      strictEqual(err instanceof AuthenticationError, true)
      strictEqual(err.message, 'Invalid server signature.')
      return true
    }
  )
})
