import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { alterUserScramCredentialsV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = alterUserScramCredentialsV0

test('createRequest serializes deletions correctly', () => {
  const deletions = [
    {
      name: 'testuser',
      mechanism: 0 // SCRAM-SHA-256
    }
  ]

  const writer = createRequest(deletions, [])

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read deletions array
  const deletionsArray = reader.readArray(() => {
    const name = reader.readString()
    const mechanism = reader.readInt8()
    return { name, mechanism }
  })

  // Read upsertions array (should be empty)
  const upsertionsArray = reader.readArray(() => {
    // This should not be executed since the array is empty
    return {}
  })

  // Verify serialized data
  deepStrictEqual(
    {
      deletions: deletionsArray,
      upsertions: upsertionsArray
    },
    {
      deletions: [
        {
          name: 'testuser',
          mechanism: 0
        }
      ],
      upsertions: []
    },
    'Serialized deletions should match expected values'
  )
})

test('createRequest serializes upsertions correctly', () => {
  const upsertions = [
    {
      name: 'testuser',
      mechanism: 0, // SCRAM-SHA-256
      iterations: 4096,
      salt: Buffer.from('salt123'),
      saltedPassword: Buffer.from('hashedpassword123')
    }
  ]

  const writer = createRequest([], upsertions)
  const reader = Reader.from(writer)

  // Read deletions array (should be empty)
  reader.readArray(() => {})

  // Read upsertions array
  const upsertionsArray = reader.readArray(() => {
    const name = reader.readString()
    const mechanism = reader.readInt8()
    const iterations = reader.readInt32()
    const salt = reader.readBytes()
    const saltedPassword = reader.readBytes()
    return { name, mechanism, iterations, salt, saltedPassword }
  })

  // Verify serialized data
  deepStrictEqual(upsertionsArray[0].name, 'testuser', 'Username should be serialized correctly')

  deepStrictEqual(upsertionsArray[0].mechanism, 0, 'Mechanism should be serialized correctly')

  deepStrictEqual(upsertionsArray[0].iterations, 4096, 'Iterations should be serialized correctly')

  ok(Buffer.compare(upsertionsArray[0].salt, Buffer.from('salt123')) === 0, 'Salt should be serialized correctly')

  ok(
    Buffer.compare(upsertionsArray[0].saltedPassword, Buffer.from('hashedpassword123')) === 0,
    'Salted password should be serialized correctly'
  )
})

test('createRequest serializes both deletions and upsertions correctly', () => {
  const deletions = [
    {
      name: 'deleteuser',
      mechanism: 0 // SCRAM-SHA-256
    }
  ]
  const upsertions = [
    {
      name: 'upsertuser',
      mechanism: 0, // SCRAM-SHA-256
      iterations: 4096,
      salt: Buffer.from('salt123'),
      saltedPassword: Buffer.from('hashedpassword123')
    }
  ]

  const writer = createRequest(deletions, upsertions)
  const reader = Reader.from(writer)

  // Read deletions array
  const deletionsArray = reader.readArray(() => {
    const name = reader.readString()
    const mechanism = reader.readInt8()
    return { name, mechanism }
  })

  // Read upsertions array
  const upsertionsArray = reader.readArray(() => {
    const name = reader.readString()
    const mechanism = reader.readInt8()
    const iterations = reader.readInt32()
    const salt = reader.readBytes()
    const saltedPassword = reader.readBytes()
    return { name, mechanism, iterations, salt, saltedPassword }
  })

  // Verify both arrays were serialized correctly
  deepStrictEqual(deletionsArray[0].name, 'deleteuser', 'Deletion username should be serialized correctly')

  deepStrictEqual(upsertionsArray[0].name, 'upsertuser', 'Upsertion username should be serialized correctly')
})

test('createRequest serializes multiple deletions correctly', () => {
  const deletions = [
    {
      name: 'user1',
      mechanism: 0 // SCRAM-SHA-256
    },
    {
      name: 'user2',
      mechanism: 1 // SCRAM-SHA-512
    }
  ]

  const writer = createRequest(deletions, [])
  const reader = Reader.from(writer)

  // Read deletions array
  const deletionsArray = reader.readArray(() => {
    const name = reader.readString()
    const mechanism = reader.readInt8()
    return { name, mechanism }
  })

  // Skip upsertions array
  reader.readArray(() => {
    return {}
  })

  // Verify multiple deletions
  deepStrictEqual(
    deletionsArray,
    [
      {
        name: 'user1',
        mechanism: 0
      },
      {
        name: 'user2',
        mechanism: 1
      }
    ],
    'Multiple deletions should be serialized correctly'
  )
})

test('createRequest serializes multiple upsertions correctly', () => {
  const upsertions = [
    {
      name: 'user1',
      mechanism: 0,
      iterations: 4096,
      salt: Buffer.from('salt1'),
      saltedPassword: Buffer.from('hashedpw1')
    },
    {
      name: 'user2',
      mechanism: 1,
      iterations: 8192,
      salt: Buffer.from('salt2'),
      saltedPassword: Buffer.from('hashedpw2')
    }
  ]

  const writer = createRequest([], upsertions)
  const reader = Reader.from(writer)

  // Skip deletions array
  reader.readArray(() => {
    return {}
  })

  // Read upsertions array
  const upsertionsArray = reader.readArray(() => {
    const name = reader.readString()
    const mechanism = reader.readInt8()
    const iterations = reader.readInt32()
    reader.readBytes()
    reader.readBytes()
    return { name, mechanism, iterations }
  })

  // Verify multiple upsertions (excluding binary data for simplicity)
  deepStrictEqual(
    upsertionsArray.map(u => ({ name: u.name, mechanism: u.mechanism, iterations: u.iterations })),
    [
      {
        name: 'user1',
        mechanism: 0,
        iterations: 4096
      },
      {
        name: 'user2',
        mechanism: 1,
        iterations: 8192
      }
    ],
    'Multiple upsertions should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          user: 'testuser',
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendString(result.user).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 51, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      results: [
        {
          user: 'testuser',
          errorCode: 0,
          errorMessage: null
        }
      ]
    },
    'Response should match expected structure'
  )
})

test('parseResponse handles multiple results correctly', () => {
  // Create a response with multiple results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          user: 'user1',
          errorCode: 0,
          errorMessage: null
        },
        {
          user: 'user2',
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendString(result.user).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 51, 0, Reader.from(writer))

  // Verify multiple results
  deepStrictEqual(
    response.results.map(r => r.user),
    ['user1', 'user2'],
    'Response should correctly parse multiple results'
  )
})

test('parseResponse handles error responses', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          user: 'testuser',
          errorCode: 58, // UNSUPPORTED_SASL_MECHANISM
          errorMessage: 'Unsupported SASL mechanism'
        }
      ],
      (w, result) => {
        w.appendString(result.user).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 51, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(
        err.response.results[0],
        {
          user: 'testuser',
          errorCode: 58,
          errorMessage: 'Unsupported SASL mechanism'
        },
        'Error response should preserve the original error details'
      )

      return true
    }
  )
})

test('parseResponse handles mixed results with success and errors', () => {
  // Create a response with mixed results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendArray(
      [
        {
          user: 'gooduser',
          errorCode: 0,
          errorMessage: null
        },
        {
          user: 'baduser',
          errorCode: 58, // UNSUPPORTED_SASL_MECHANISM
          errorMessage: 'Unsupported SASL mechanism'
        }
      ],
      (w, result) => {
        w.appendString(result.user).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 51, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify response contains both successful and error results
      deepStrictEqual(
        err.response.results,
        [
          {
            user: 'gooduser',
            errorCode: 0,
            errorMessage: null
          },
          {
            user: 'baduser',
            errorCode: 58,
            errorMessage: 'Unsupported SASL mechanism'
          }
        ],
        'Response should contain both successful and error results'
      )

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs)
    .appendArray(
      [
        {
          user: 'testuser',
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendString(result.user).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 51, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
