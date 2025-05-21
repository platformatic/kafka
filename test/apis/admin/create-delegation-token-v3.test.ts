import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { createDelegationTokenV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = createDelegationTokenV3

test('createRequest serializes basic parameters correctly', () => {
  const ownerPrincipalType = 'User'
  const ownerPrincipalName = 'owner'
  const renewers = [
    {
      principalType: 'User',
      principalName: 'renewer1'
    }
  ]
  const maxLifetimeMs = BigInt(86400000) // 24 hours

  const writer = createRequest(ownerPrincipalType, ownerPrincipalName, renewers, maxLifetimeMs)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read parameters
  const serializedOwnerPrincipalType = reader.readNullableString()
  const serializedOwnerPrincipalName = reader.readNullableString()

  // Read renewers array
  const renewersArray = reader.readArray(() => {
    const principalType = reader.readString()
    const principalName = reader.readString()
    return { principalType, principalName }
  })

  const serializedMaxLifetimeMs = reader.readInt64()

  // Verify serialized data
  deepStrictEqual(
    {
      ownerPrincipalType: serializedOwnerPrincipalType,
      ownerPrincipalName: serializedOwnerPrincipalName,
      renewers: renewersArray,
      maxLifetimeMs: serializedMaxLifetimeMs
    },
    {
      ownerPrincipalType: 'User',
      ownerPrincipalName: 'owner',
      renewers: [
        {
          principalType: 'User',
          principalName: 'renewer1'
        }
      ],
      maxLifetimeMs: BigInt(86400000)
    },
    'Serialized data should match expected values'
  )
})

test('createRequest serializes null owner principal correctly', () => {
  const ownerPrincipalType = null
  const ownerPrincipalName = null
  const renewers = [
    {
      principalType: 'User',
      principalName: 'renewer1'
    }
  ]
  const maxLifetimeMs = BigInt(86400000)

  const writer = createRequest(ownerPrincipalType, ownerPrincipalName, renewers, maxLifetimeMs)
  const reader = Reader.from(writer)

  // Read parameters
  const serializedOwnerPrincipalType = reader.readNullableString()
  const serializedOwnerPrincipalName = reader.readNullableString()

  // Skip renewers array and maxLifetimeMs
  reader.readArray(() => {
    reader.readString()
    reader.readString()
    return {}
  })
  reader.readInt64()

  // Verify null values are correctly serialized
  deepStrictEqual(
    {
      ownerPrincipalType: serializedOwnerPrincipalType,
      ownerPrincipalName: serializedOwnerPrincipalName
    },
    {
      ownerPrincipalType: null,
      ownerPrincipalName: null
    },
    'Null owner principal values should be serialized correctly'
  )
})

test('createRequest serializes multiple renewers correctly', () => {
  const ownerPrincipalType = 'User'
  const ownerPrincipalName = 'owner'
  const renewers = [
    {
      principalType: 'User',
      principalName: 'renewer1'
    },
    {
      principalType: 'User',
      principalName: 'renewer2'
    },
    {
      principalType: 'Group',
      principalName: 'admin-group'
    }
  ]
  const maxLifetimeMs = BigInt(86400000)

  const writer = createRequest(ownerPrincipalType, ownerPrincipalName, renewers, maxLifetimeMs)
  const reader = Reader.from(writer)

  // Skip owner principal
  reader.readNullableString()
  reader.readNullableString()

  // Read renewers array
  const renewersArray = reader.readArray(() => {
    const principalType = reader.readString()
    const principalName = reader.readString()
    return { principalType, principalName }
  })

  // Skip maxLifetimeMs
  reader.readInt64()

  // Verify multiple renewers
  deepStrictEqual(
    renewersArray,
    [
      {
        principalType: 'User',
        principalName: 'renewer1'
      },
      {
        principalType: 'User',
        principalName: 'renewer2'
      },
      {
        principalType: 'Group',
        principalName: 'admin-group'
      }
    ],
    'Multiple renewers should be serialized correctly'
  )
})

test('createRequest serializes empty renewers list correctly', () => {
  const ownerPrincipalType = 'User'
  const ownerPrincipalName = 'owner'
  const maxLifetimeMs = BigInt(86400000)

  const writer = createRequest(ownerPrincipalType, ownerPrincipalName, [], maxLifetimeMs)
  const reader = Reader.from(writer)

  // Skip owner principal
  reader.readNullableString()
  reader.readNullableString()

  // Read renewers array
  const renewersArray = reader.readArray(() => {
    reader.readString()
    reader.readString()
    return {}
  })

  // Verify empty renewers array
  deepStrictEqual(renewersArray.length, 0, 'Empty renewers array should be serialized correctly')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendString('User') // principalType
    .appendString('owner') // principalName
    .appendString('User') // tokenRequesterPrincipalType
    .appendString('requester') // tokenRequesterPrincipalName
    .appendInt64(BigInt(1621234567890)) // issueTimestampMs
    .appendInt64(BigInt(1621320967890)) // expiryTimestampMs
    .appendInt64(BigInt(1621407367890)) // maxTimestampMs
    .appendString('token-id-1234') // tokenId
    .appendBytes(Buffer.from('hmac-data')) // hmac
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields()

  const response = parseResponse(1, 38, 3, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    {
      errorCode: response.errorCode,
      principalType: response.principalType,
      principalName: response.principalName,
      tokenRequesterPrincipalType: response.tokenRequesterPrincipalType,
      tokenRequesterPrincipalName: response.tokenRequesterPrincipalName,
      issueTimestampMs: response.issueTimestampMs,
      expiryTimestampMs: response.expiryTimestampMs,
      maxTimestampMs: response.maxTimestampMs,
      tokenId: response.tokenId,
      throttleTimeMs: response.throttleTimeMs
    },
    {
      errorCode: 0,
      principalType: 'User',
      principalName: 'owner',
      tokenRequesterPrincipalType: 'User',
      tokenRequesterPrincipalName: 'requester',
      issueTimestampMs: BigInt(1621234567890),
      expiryTimestampMs: BigInt(1621320967890),
      maxTimestampMs: BigInt(1621407367890),
      tokenId: 'token-id-1234',
      throttleTimeMs: 0
    },
    'Response should match expected structure'
  )

  // Verify hmac separately (Buffer comparison)
  ok(Buffer.compare(response.hmac, Buffer.from('hmac-data')) === 0, 'HMAC should be parsed correctly')
})

test('parseResponse handles error responses', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt16(37) // errorCode - CLUSTER_AUTHORIZATION_FAILED
    .appendString('User') // principalType (irrelevant due to error)
    .appendString('owner') // principalName (irrelevant due to error)
    .appendString('User') // tokenRequesterPrincipalType (irrelevant due to error)
    .appendString('requester') // tokenRequesterPrincipalName (irrelevant due to error)
    .appendInt64(BigInt(0)) // issueTimestampMs (irrelevant due to error)
    .appendInt64(BigInt(0)) // expiryTimestampMs (irrelevant due to error)
    .appendInt64(BigInt(0)) // maxTimestampMs (irrelevant due to error)
    .appendString('') // tokenId (irrelevant due to error)
    .appendBytes(Buffer.from('')) // hmac (irrelevant due to error)
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 38, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response is preserved
      deepStrictEqual(err.response.errorCode, 37, 'Error code should be preserved in the response')

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt16(0) // errorCode
    .appendString('User') // principalType
    .appendString('owner') // principalName
    .appendString('User') // tokenRequesterPrincipalType
    .appendString('requester') // tokenRequesterPrincipalName
    .appendInt64(BigInt(1621234567890)) // issueTimestampMs
    .appendInt64(BigInt(1621320967890)) // expiryTimestampMs
    .appendInt64(BigInt(1621407367890)) // maxTimestampMs
    .appendString('token-id-1234') // tokenId
    .appendBytes(Buffer.from('hmac-data')) // hmac
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendTaggedFields()

  const response = parseResponse(1, 38, 3, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
