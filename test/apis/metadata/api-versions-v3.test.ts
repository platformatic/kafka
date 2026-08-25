import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { apiVersionsV3, protocolAPIsById, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = apiVersionsV3

function appendTaggedField (writer: Writer, tag: number, payload: Writer): Writer {
  return writer.appendUnsignedVarInt(tag).appendUnsignedVarInt(payload.length).appendFrom(payload)
}

test('createRequest serializes client software name and version correctly', () => {
  const clientName = 'test-client-name'
  const clientVersion = '2.0.0'

  const writer = createRequest(clientName, clientVersion)

  // Verify it returns a Writer instance
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all values and verify them at once
  deepStrictEqual(
    {
      clientName: reader.readString(true),
      clientVersion: reader.readString(true)
    },
    {
      clientName,
      clientVersion
    },
    'Serialized request should match expected structure'
  )
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('createRequest handles empty values', () => {
  const clientName = ''
  const clientVersion = ''

  const writer = createRequest(clientName, clientVersion)

  // Verify it returns a Writer instance
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read all values and verify them at once
  deepStrictEqual(
    {
      clientName: reader.readString(true),
      clientVersion: reader.readString(true)
    },
    {
      clientName,
      clientVersion
    },
    'Serialized request with empty values should match expected structure'
  )
  reader.readTaggedFields()
  strictEqual(reader.remaining, 0)
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    // ApiKeys array
    .appendArray(
      [
        { apiKey: 0, minVersion: 0, maxVersion: 9 },
        { apiKey: 1, minVersion: 0, maxVersion: 12 }
      ],
      (w, api) => {
        w.appendInt16(api.apiKey).appendInt16(api.minVersion).appendInt16(api.maxVersion).appendTaggedFields()
      },
      true,
      false
    )
    .appendInt32(0) // throttleTimeMs
    .appendUnsignedVarInt(0) // tagged fields

  const reader = Reader.from(writer)
  const response = parseResponse(1, 18, 3, reader)

  // Verify structure
  deepStrictEqual(response, {
    errorCode: 0,
    apiKeys: [
      {
        apiKey: 0,
        name: protocolAPIsById[0],
        minVersion: 0,
        maxVersion: 9
      },
      {
        apiKey: 1,
        name: protocolAPIsById[1],
        minVersion: 0,
        maxVersion: 12
      }
    ],
    throttleTimeMs: 0,
    supportedFeatures: [],
    finalizedFeaturesEpoch: -1n,
    finalizedFeatures: [],
    zkMigrationReady: false
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse handles response with throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    // ApiKeys array - just one API for simplicity
    .appendArray(
      [{ apiKey: 0, minVersion: 0, maxVersion: 9 }],
      (w, api) => {
        w.appendInt16(api.apiKey).appendInt16(api.minVersion).appendInt16(api.maxVersion).appendTaggedFields()
      },
      true,
      false
    )
    .appendInt32(100) // throttleTimeMs - non-zero value
    .appendUnsignedVarInt(0) // tagged fields

  const reader = Reader.from(writer)
  const response = parseResponse(1, 18, 3, reader)

  // Verify response structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    errorCode: 0,
    apiKeys: [
      {
        apiKey: 0,
        name: protocolAPIsById[0],
        minVersion: 0,
        maxVersion: 9
      }
    ],
    supportedFeatures: [],
    finalizedFeaturesEpoch: -1n,
    finalizedFeatures: [],
    zkMigrationReady: false
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt16(42) // errorCode (non-zero)
    // ApiKeys array (empty compact array)
    .appendUnsignedVarInt(1)
    .appendInt32(0) // throttleTimeMs
    .appendUnsignedVarInt(0) // tagged fields

  throws(
    () => {
      parseResponse(1, 18, 3, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that response is attached and has correct properties
      deepStrictEqual(err.response, {
        errorCode: 42,
        apiKeys: [],
        throttleTimeMs: 0
      })

      return true
    }
  )
})

test('parseResponse handles a v0-framed unsupported version response', () => {
  const reader = Reader.from(Writer.create().appendInt16(35).appendInt32(0))

  throws(
    () => parseResponse(1, 18, 3, reader),
    (err: any) => {
      strictEqual(err instanceof ResponseError, true)
      strictEqual(err.errors[0].apiId, 'UNSUPPORTED_VERSION')
      strictEqual(reader.remaining, 4)
      return true
    }
  )
})

test('parseResponse decodes known root tags interleaved with unknown tags', () => {
  const supportedFeatures = Writer.create().appendArray(
    [{ name: 'metadata.version', minVersion: 0, maxVersion: 26 }],
    (w, feature) => {
      w.appendString(feature.name).appendInt16(feature.minVersion).appendInt16(feature.maxVersion).appendTaggedFields()
    },
    true,
    false
  )
  const finalizedFeatures = Writer.create().appendArray(
    [{ name: 'metadata.version', maxVersionLevel: 26, minVersionLevel: 21 }],
    (w, feature) => {
      w.appendString(feature.name)
        .appendInt16(feature.maxVersionLevel)
        .appendInt16(feature.minVersionLevel)
        .appendTaggedFields()
    },
    true,
    false
  )
  const writer = Writer.create().appendInt16(0).appendArray([], () => {}, true, false).appendInt32(0).appendUnsignedVarInt(6)

  appendTaggedField(writer, 99, Writer.create().append(Buffer.from([1])))
  appendTaggedField(writer, 0, supportedFeatures)
  appendTaggedField(writer, 98, Writer.create().append(Buffer.from([2])))
  appendTaggedField(writer, 1, Writer.create().appendInt64(4n))
  appendTaggedField(writer, 2, finalizedFeatures)
  appendTaggedField(writer, 3, Writer.create().appendBoolean(true))

  const reader = Reader.from(writer)

  deepStrictEqual(parseResponse(1, 18, 3, reader), {
    errorCode: 0,
    apiKeys: [],
    throttleTimeMs: 0,
    supportedFeatures: [{ name: 'metadata.version', minVersion: 0, maxVersion: 26 }],
    finalizedFeaturesEpoch: 4n,
    finalizedFeatures: [{ name: 'metadata.version', maxVersionLevel: 26, minVersionLevel: 21 }],
    zkMigrationReady: true
  })
  strictEqual(reader.remaining, 0)
})
