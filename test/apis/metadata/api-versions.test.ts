import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { apiVersionsV4, protocolAPIsById, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = apiVersionsV4

test('createRequest serializes client software name and version correctly', () => {
  const clientName = 'test-client-name'
  const clientVersion = '2.0.0'

  const writer = createRequest(clientName, clientVersion)

  // Verify it returns a Writer instance
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read all values and verify them at once
  deepStrictEqual(
    {
      clientName: reader.readString(),
      clientVersion: reader.readString()
    },
    {
      clientName,
      clientVersion
    },
    'Serialized request should match expected structure'
  )
})

test('createRequest handles empty values', () => {
  const clientName = ''
  const clientVersion = ''

  const writer = createRequest(clientName, clientVersion)

  // Verify it returns a Writer instance
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read all values and verify them at once
  deepStrictEqual(
    {
      clientName: reader.readString(),
      clientVersion: reader.readString()
    },
    {
      clientName,
      clientVersion
    },
    'Serialized request with empty values should match expected structure'
  )
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
        w.appendInt16(api.apiKey).appendInt16(api.minVersion).appendInt16(api.maxVersion)
      }
    )
    .appendInt32(0) // throttleTimeMs
    .appendUnsignedVarInt(0) // tagged fields

  const response = parseResponse(1, 18, 4, writer.bufferList)

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
    throttleTimeMs: 0
  })
})

test('parseResponse handles response with throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    // ApiKeys array - just one API for simplicity
    .appendArray([{ apiKey: 0, minVersion: 0, maxVersion: 9 }], (w, api) => {
      w.appendInt16(api.apiKey).appendInt16(api.minVersion).appendInt16(api.maxVersion)
    })
    .appendInt32(100) // throttleTimeMs - non-zero value
    .appendUnsignedVarInt(0) // tagged fields

  const response = parseResponse(1, 18, 4, writer.bufferList)

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
    ]
  })
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt16(42) // errorCode (non-zero)
    // ApiKeys array (empty but in compact format)
    .appendUnsignedVarInt(0)
    .appendInt32(0) // throttleTimeMs
    .appendUnsignedVarInt(0) // tagged fields

  throws(
    () => {
      parseResponse(1, 18, 4, writer.bufferList)
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Check that response is attached and has correct properties
      deepStrictEqual(err.response, {
        errorCode: 42,
        throttleTimeMs: 0,
        apiKeys: []
      })

      return true
    }
  )
})
