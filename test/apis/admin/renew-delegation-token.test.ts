import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, renewDelegationTokenV2 } from '../../../src/index.ts'

const { createRequest, parseResponse } = renewDelegationTokenV2

test('createRequest serializes parameters correctly', () => {
  const hmac = Buffer.from([1, 2, 3, 4, 5])
  const renewPeriodMs = BigInt(86400000) // 1 day in milliseconds

  const writer = createRequest(hmac, renewPeriodMs)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read hmac buffer
  const serializedHmac = reader.readBytes()

  // Read renewPeriodMs
  const serializedRenewPeriodMs = reader.readInt64()

  // Verify serialized data
  ok(Buffer.isBuffer(serializedHmac), 'HMAC should be serialized as a Buffer')
  deepStrictEqual(Array.from(serializedHmac), [1, 2, 3, 4, 5], 'HMAC content should match')

  deepStrictEqual(serializedRenewPeriodMs, BigInt(86400000), 'Renew period should be serialized correctly')
})

test('createRequest handles different HMAC lengths correctly', () => {
  const hmac = Buffer.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
  const renewPeriodMs = BigInt(86400000)

  const writer = createRequest(hmac, renewPeriodMs)
  const reader = Reader.from(writer)

  // Read hmac buffer
  const serializedHmac = reader.readBytes()

  // Verify hmac length
  deepStrictEqual(serializedHmac.length, 10, 'HMAC of different length should be serialized correctly')
  deepStrictEqual(Array.from(serializedHmac), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'HMAC content should match exactly')
})

test('createRequest handles different renew periods correctly', () => {
  const hmac = Buffer.from([1, 2, 3, 4, 5])
  const renewPeriodMs = BigInt(3600000) // 1 hour in milliseconds

  const writer = createRequest(hmac, renewPeriodMs)
  const reader = Reader.from(writer)

  // Skip hmac
  reader.readBytes()

  // Read renewPeriodMs
  const serializedRenewPeriodMs = reader.readInt64()

  // Verify renew period
  deepStrictEqual(serializedRenewPeriodMs, BigInt(3600000), 'Different renew period should be serialized correctly')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    .appendInt64(BigInt(1630000000000)) // expiryTimestampMs
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields()

  const response = parseResponse(1, 39, 2, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response.errorCode, 0, 'Error code should be 0 for success')
  deepStrictEqual(response.expiryTimestampMs, BigInt(1630000000000), 'Expiry timestamp should be parsed correctly')
  deepStrictEqual(response.throttleTimeMs, 0, 'Throttle time should be parsed correctly')
})

test('parseResponse throws ResponseError on error response', () => {
  // Create an error response
  const writer = Writer.create()
    .appendInt16(58) // errorCode INVALID_TOKEN
    .appendInt64(BigInt(0)) // expiryTimestampMs (irrelevant for error)
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 39, 2, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error details
      deepStrictEqual(err.response.errorCode, 58, 'Error code should be preserved in the response')

      // Verify other fields are preserved
      deepStrictEqual(
        err.response.expiryTimestampMs,
        BigInt(0),
        'Expiry timestamp should be preserved in error response'
      )

      return true
    }
  )
})

test('parseResponse handles non-zero throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    .appendInt64(BigInt(1630000000000)) // expiryTimestampMs
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendTaggedFields()

  const response = parseResponse(1, 39, 2, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})

test('parseResponse handles future expiry timestamp correctly', () => {
  // Create a response with a future expiry timestamp
  const futureTimestamp = BigInt(Date.now()) + BigInt(86400000) // Now + 1 day

  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    .appendInt64(futureTimestamp) // future expiryTimestampMs
    .appendInt32(0) // throttleTimeMs
    .appendTaggedFields()

  const response = parseResponse(1, 39, 2, Reader.from(writer))

  // Verify future timestamp
  deepStrictEqual(response.expiryTimestampMs, futureTimestamp, 'Future expiry timestamp should be parsed correctly')
})
