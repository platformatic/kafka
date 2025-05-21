import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, describeUserScramCredentialsV0 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeUserScramCredentialsV0

test('createRequest serializes empty users array correctly', () => {
  const users: [] = []

  const writer = createRequest(users)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read users array
  const usersArray = reader.readArray(() => {
    const name = reader.readString()
    return { name }
  })

  // Verify serialized data
  deepStrictEqual(usersArray, [], 'Empty users array should be serialized correctly')
})

test('createRequest serializes single user correctly', () => {
  const users = [
    {
      name: 'user1'
    }
  ]

  const writer = createRequest(users)
  const reader = Reader.from(writer)

  // Read users array
  const usersArray = reader.readArray(() => {
    const name = reader.readString()
    return { name }
  })

  // Verify serialized data
  deepStrictEqual(
    usersArray,
    [
      {
        name: 'user1'
      }
    ],
    'Single user should be serialized correctly'
  )
})

test('createRequest serializes multiple users correctly', () => {
  const users = [
    {
      name: 'user1'
    },
    {
      name: 'user2'
    },
    {
      name: 'admin'
    }
  ]

  const writer = createRequest(users)
  const reader = Reader.from(writer)

  // Read users array
  const usersArray = reader.readArray(() => {
    const name = reader.readString()
    return { name }
  })

  // Verify multiple users
  deepStrictEqual(
    usersArray,
    [
      {
        name: 'user1'
      },
      {
        name: 'user2'
      },
      {
        name: 'admin'
      }
    ],
    'Multiple users should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no user results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty results array
    .appendTaggedFields()

  const response = parseResponse(1, 50, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      errorMessage: null,
      results: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a response with user credentials', () => {
  // Create a successful response with user credentials
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          user: 'user1',
          errorCode: 0,
          errorMessage: null,
          credentialInfos: [
            {
              mechanism: 0, // SCRAM-SHA-256
              iterations: 4096
            }
          ]
        }
      ],
      (w, result) => {
        w.appendString(result.user)
          .appendInt16(result.errorCode)
          .appendString(result.errorMessage)
          .appendArray(result.credentialInfos, (w, info) => {
            w.appendInt8(info.mechanism).appendInt32(info.iterations)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 50, 0, Reader.from(writer))

  // Verify user credentials structure
  deepStrictEqual(response.results.length, 1, 'Should have one user result')

  const result = response.results[0]
  deepStrictEqual(result.user, 'user1', 'User name should be parsed correctly')
  deepStrictEqual(result.errorCode, 0, 'Error code should be parsed correctly')
  deepStrictEqual(result.errorMessage, null, 'Error message should be parsed correctly')

  // Verify credential infos
  deepStrictEqual(result.credentialInfos.length, 1, 'Should have one credential info')
  deepStrictEqual(result.credentialInfos[0].mechanism, 0, 'Mechanism should be parsed correctly')
  deepStrictEqual(result.credentialInfos[0].iterations, 4096, 'Iterations should be parsed correctly')
})

test('parseResponse correctly processes a response with multiple users', () => {
  // Create a response with multiple users
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          user: 'user1',
          errorCode: 0,
          errorMessage: null,
          credentialInfos: [
            {
              mechanism: 0, // SCRAM-SHA-256
              iterations: 4096
            }
          ]
        },
        {
          user: 'user2',
          errorCode: 0,
          errorMessage: null,
          credentialInfos: [
            {
              mechanism: 1, // SCRAM-SHA-512
              iterations: 8192
            }
          ]
        }
      ],
      (w, result) => {
        w.appendString(result.user)
          .appendInt16(result.errorCode)
          .appendString(result.errorMessage)
          .appendArray(result.credentialInfos, (w, info) => {
            w.appendInt8(info.mechanism).appendInt32(info.iterations)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 50, 0, Reader.from(writer))

  // Verify multiple users
  deepStrictEqual(response.results.length, 2, 'Should have two user results')

  // Verify user names
  deepStrictEqual(
    response.results.map(r => r.user),
    ['user1', 'user2'],
    'User names should be parsed correctly'
  )

  // Verify credential mechanisms
  deepStrictEqual(
    response.results.map(r => r.credentialInfos[0].mechanism),
    [0, 1],
    'Credential mechanisms should be parsed correctly'
  )

  // Verify credential iterations
  deepStrictEqual(
    response.results.map(r => r.credentialInfos[0].iterations),
    [4096, 8192],
    'Credential iterations should be parsed correctly'
  )
})

test('parseResponse correctly processes a response with multiple credential infos', () => {
  // Create a response with multiple credential infos per user
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          user: 'user1',
          errorCode: 0,
          errorMessage: null,
          credentialInfos: [
            {
              mechanism: 0, // SCRAM-SHA-256
              iterations: 4096
            },
            {
              mechanism: 1, // SCRAM-SHA-512
              iterations: 8192
            }
          ]
        }
      ],
      (w, result) => {
        w.appendString(result.user)
          .appendInt16(result.errorCode)
          .appendString(result.errorMessage)
          .appendArray(result.credentialInfos, (w, info) => {
            w.appendInt8(info.mechanism).appendInt32(info.iterations)
          })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 50, 0, Reader.from(writer))

  // Verify multiple credential infos
  deepStrictEqual(response.results[0].credentialInfos.length, 2, 'Should have two credential infos for the user')

  // Verify credential mechanisms
  deepStrictEqual(
    response.results[0].credentialInfos.map(info => info.mechanism),
    [0, 1],
    'Credential mechanisms should be parsed correctly'
  )

  // Verify credential iterations
  deepStrictEqual(
    response.results[0].credentialInfos.map(info => info.iterations),
    [4096, 8192],
    'Credential iterations should be parsed correctly'
  )
})

test('parseResponse throws ResponseError on top-level error', () => {
  // Create a response with a top-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(58) // NOT_AUTHORIZED
    .appendString('Not authorized to describe SCRAM credentials') // errorMessage
    .appendArray([], () => {}) // Empty results array
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 50, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify top-level error details
      deepStrictEqual(err.response.errorCode, 58, 'Top-level error code should be preserved in the response')

      deepStrictEqual(
        err.response.errorMessage,
        'Not authorized to describe SCRAM credentials',
        'Error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse throws ResponseError on user-level error', () => {
  // Create a response with a user-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // No top-level error
    .appendString(null) // No top-level error message
    .appendArray(
      [
        {
          user: 'non-existent-user',
          errorCode: 68, // RESOURCE_NOT_FOUND
          errorMessage: 'User not found',
          credentialInfos: []
        }
      ],
      (w, result) => {
        w.appendString(result.user)
          .appendInt16(result.errorCode)
          .appendString(result.errorMessage)
          .appendArray([], () => {}) // Empty credential infos
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 50, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify user-level error details
      deepStrictEqual(
        err.response.results[0].errorCode,
        68,
        'User-level error code should be preserved in the response'
      )

      deepStrictEqual(
        err.response.results[0].errorMessage,
        'User not found',
        'User-level error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty results array
    .appendTaggedFields()

  const response = parseResponse(1, 50, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
