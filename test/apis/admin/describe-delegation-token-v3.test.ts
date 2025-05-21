import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, describeDelegationTokenV3 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeDelegationTokenV3

test('createRequest serializes empty owners array correctly', () => {
  const owners: [] = []

  const writer = createRequest(owners)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read owners array
  const ownersArray = reader.readArray(() => {
    const principalType = reader.readString()
    const principalName = reader.readString()
    return { principalType, principalName }
  })

  // Verify serialized data
  deepStrictEqual(ownersArray, [], 'Empty owners array should be serialized correctly')
})

test('createRequest serializes single owner correctly', () => {
  const owners = [
    {
      principalType: 'User',
      principalName: 'user1'
    }
  ]

  const writer = createRequest(owners)
  const reader = Reader.from(writer)

  // Read owners array
  const ownersArray = reader.readArray(() => {
    const principalType = reader.readString()
    const principalName = reader.readString()
    return { principalType, principalName }
  })

  // Verify serialized data
  deepStrictEqual(
    ownersArray,
    [
      {
        principalType: 'User',
        principalName: 'user1'
      }
    ],
    'Single owner should be serialized correctly'
  )
})

test('createRequest serializes multiple owners correctly', () => {
  const owners = [
    {
      principalType: 'User',
      principalName: 'user1'
    },
    {
      principalType: 'User',
      principalName: 'user2'
    },
    {
      principalType: 'Group',
      principalName: 'group1'
    }
  ]

  const writer = createRequest(owners)
  const reader = Reader.from(writer)

  // Read owners array
  const ownersArray = reader.readArray(() => {
    const principalType = reader.readString()
    const principalName = reader.readString()
    return { principalType, principalName }
  })

  // Verify multiple owners
  deepStrictEqual(
    ownersArray,
    [
      {
        principalType: 'User',
        principalName: 'user1'
      },
      {
        principalType: 'User',
        principalName: 'user2'
      },
      {
        principalType: 'Group',
        principalName: 'group1'
      }
    ],
    'Multiple owners should be serialized correctly'
  )
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no tokens
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendArray([], () => {}) // Empty tokens array
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields()

  const response = parseResponse(1, 41, 3, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      errorCode: 0,
      tokens: [],
      throttleTimeMs: 0
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a response with a token', () => {
  const hmacBuffer = Buffer.from([1, 2, 3, 4, 5])

  // Create a successful response with a token
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendArray(
      [
        {
          principalType: 'User',
          principalName: 'user1',
          tokenRequesterPrincipalType: 'User',
          tokenRequesterPrincipalName: 'admin',
          issueTimestamp: BigInt(1630000000000),
          expiryTimestamp: BigInt(1640000000000),
          maxTimestamp: BigInt(1640000000000),
          tokenId: 'token1',
          hmac: hmacBuffer,
          renewers: [
            {
              principalType: 'User',
              principalName: 'admin'
            }
          ]
        }
      ],
      (w, token) => {
        w.appendString(token.principalType)
          .appendString(token.principalName)
          .appendString(token.tokenRequesterPrincipalType)
          .appendString(token.tokenRequesterPrincipalName)
          .appendInt64(token.issueTimestamp)
          .appendInt64(token.expiryTimestamp)
          .appendInt64(token.maxTimestamp)
          .appendString(token.tokenId)
          .appendBytes(token.hmac)
          .appendArray(token.renewers, (w, renewer) => {
            w.appendString(renewer.principalType).appendString(renewer.principalName)
          })
      }
    )
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields()

  const response = parseResponse(1, 41, 3, Reader.from(writer))

  // Verify token fields
  deepStrictEqual(response.tokens.length, 1, 'Should have one token')
  deepStrictEqual(response.tokens[0].principalType, 'User', 'Principal type should be parsed correctly')
  deepStrictEqual(response.tokens[0].principalName, 'user1', 'Principal name should be parsed correctly')
  deepStrictEqual(
    response.tokens[0].tokenRequesterPrincipalType,
    'User',
    'Token requester principal type should be parsed correctly'
  )
  deepStrictEqual(
    response.tokens[0].tokenRequesterPrincipalName,
    'admin',
    'Token requester principal name should be parsed correctly'
  )
  deepStrictEqual(
    response.tokens[0].issueTimestamp,
    BigInt(1630000000000),
    'Issue timestamp should be parsed correctly'
  )
  deepStrictEqual(
    response.tokens[0].expiryTimestamp,
    BigInt(1640000000000),
    'Expiry timestamp should be parsed correctly'
  )
  deepStrictEqual(response.tokens[0].maxTimestamp, BigInt(1640000000000), 'Max timestamp should be parsed correctly')
  deepStrictEqual(response.tokens[0].tokenId, 'token1', 'Token ID should be parsed correctly')

  // Check hmac buffer
  ok(Buffer.isBuffer(response.tokens[0].hmac), 'HMAC should be a Buffer')
  deepStrictEqual(Array.from(response.tokens[0].hmac), Array.from(hmacBuffer), 'HMAC buffer should be parsed correctly')

  // Check renewers
  deepStrictEqual(
    response.tokens[0].renewers,
    [
      {
        principalType: 'User',
        principalName: 'admin'
      }
    ],
    'Renewers should be parsed correctly'
  )
})

test('parseResponse correctly processes a response with multiple tokens', () => {
  const hmacBuffer1 = Buffer.from([1, 2, 3, 4, 5])
  const hmacBuffer2 = Buffer.from([6, 7, 8, 9, 10])

  // Create a response with multiple tokens
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendArray(
      [
        {
          principalType: 'User',
          principalName: 'user1',
          tokenRequesterPrincipalType: 'User',
          tokenRequesterPrincipalName: 'admin',
          issueTimestamp: BigInt(1630000000000),
          expiryTimestamp: BigInt(1640000000000),
          maxTimestamp: BigInt(1640000000000),
          tokenId: 'token1',
          hmac: hmacBuffer1,
          renewers: []
        },
        {
          principalType: 'User',
          principalName: 'user2',
          tokenRequesterPrincipalType: 'User',
          tokenRequesterPrincipalName: 'admin',
          issueTimestamp: BigInt(1631000000000),
          expiryTimestamp: BigInt(1641000000000),
          maxTimestamp: BigInt(1641000000000),
          tokenId: 'token2',
          hmac: hmacBuffer2,
          renewers: [
            {
              principalType: 'User',
              principalName: 'admin'
            },
            {
              principalType: 'User',
              principalName: 'user1'
            }
          ]
        }
      ],
      (w, token) => {
        w.appendString(token.principalType)
          .appendString(token.principalName)
          .appendString(token.tokenRequesterPrincipalType)
          .appendString(token.tokenRequesterPrincipalName)
          .appendInt64(token.issueTimestamp)
          .appendInt64(token.expiryTimestamp)
          .appendInt64(token.maxTimestamp)
          .appendString(token.tokenId)
          .appendBytes(token.hmac)
          .appendArray(token.renewers, (w, renewer) => {
            w.appendString(renewer.principalType).appendString(renewer.principalName)
          })
      }
    )
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields()

  const response = parseResponse(1, 41, 3, Reader.from(writer))

  // Verify number of tokens
  deepStrictEqual(response.tokens.length, 2, 'Should have two tokens')

  // Verify token IDs
  deepStrictEqual(
    response.tokens.map(t => t.tokenId),
    ['token1', 'token2'],
    'Token IDs should be parsed correctly'
  )

  // Verify second token renewers
  deepStrictEqual(
    response.tokens[1].renewers,
    [
      {
        principalType: 'User',
        principalName: 'admin'
      },
      {
        principalType: 'User',
        principalName: 'user1'
      }
    ],
    'Multiple renewers should be parsed correctly'
  )
})

test('parseResponse correctly processes a response with multiple renewers', () => {
  const hmacBuffer = Buffer.from([1, 2, 3, 4, 5])

  // Create a response with a token that has multiple renewers
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendArray(
      [
        {
          principalType: 'User',
          principalName: 'user1',
          tokenRequesterPrincipalType: 'User',
          tokenRequesterPrincipalName: 'admin',
          issueTimestamp: BigInt(1630000000000),
          expiryTimestamp: BigInt(1640000000000),
          maxTimestamp: BigInt(1640000000000),
          tokenId: 'token1',
          hmac: hmacBuffer,
          renewers: [
            {
              principalType: 'User',
              principalName: 'admin'
            },
            {
              principalType: 'User',
              principalName: 'user2'
            },
            {
              principalType: 'Group',
              principalName: 'group1'
            }
          ]
        }
      ],
      (w, token) => {
        w.appendString(token.principalType)
          .appendString(token.principalName)
          .appendString(token.tokenRequesterPrincipalType)
          .appendString(token.tokenRequesterPrincipalName)
          .appendInt64(token.issueTimestamp)
          .appendInt64(token.expiryTimestamp)
          .appendInt64(token.maxTimestamp)
          .appendString(token.tokenId)
          .appendBytes(token.hmac)
          .appendArray(token.renewers, (w, renewer) => {
            w.appendString(renewer.principalType).appendString(renewer.principalName)
          })
      }
    )
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields()

  const response = parseResponse(1, 41, 3, Reader.from(writer))

  // Verify renewers
  deepStrictEqual(
    response.tokens[0].renewers,
    [
      {
        principalType: 'User',
        principalName: 'admin'
      },
      {
        principalType: 'User',
        principalName: 'user2'
      },
      {
        principalType: 'Group',
        principalName: 'group1'
      }
    ],
    'Multiple renewers with different types should be parsed correctly'
  )
})

test('parseResponse throws ResponseError on error response', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt16(58) // INVALID_TOKEN error code
    .appendArray([], () => {}) // Empty tokens array
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 41, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error code
      deepStrictEqual(err.response.errorCode, 58, 'Error code should be preserved in the response')

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendArray([], () => {}) // Empty tokens array
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendTaggedFields()

  const response = parseResponse(1, 41, 3, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
