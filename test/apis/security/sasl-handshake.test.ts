import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, saslHandshakeV1, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = saslHandshakeV1

test('createRequest serializes mechanism correctly', () => {
  const mechanism = 'SCRAM-SHA-256'
  const writer = createRequest(mechanism)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Note: SaslHandshake V1 uses non-compact strings (false flag in readString)
  // Verify the complete request structure with deepStrictEqual
  deepStrictEqual(
    {
      mechanism: reader.readString(false) // non-compact string
    },
    {
      mechanism
    },
    'Serialized request should match expected structure'
  )
})

test('createRequest handles PLAIN mechanism', () => {
  const mechanism = 'PLAIN'
  const writer = createRequest(mechanism)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Verify the complete request structure with deepStrictEqual
  deepStrictEqual(
    {
      mechanism: reader.readString(false) // non-compact string
    },
    {
      mechanism
    },
    'Serialized request with PLAIN mechanism should match expected structure'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt16(0) // errorCode (success)
    // Mechanisms array (this is a non-compact array without tagged fields)
    .appendArray(
      ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'],
      (w, mechanism) => w.appendString(mechanism, false),
      false,
      false
    ) // non-compact array without tagged fields

  const response = parseResponse(1, 17, 1, writer.bufferList)

  // Verify structure
  deepStrictEqual(response, {
    errorCode: 0,
    mechanisms: ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']
  })
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt16(58) // errorCode (UNSUPPORTED_SASL_MECHANISM)
    // Mechanisms array (empty because the requested mechanism is not supported)
    .appendArray([], () => {}, false, false) // empty non-compact array

  throws(
    () => {
      parseResponse(1, 17, 1, writer.bufferList)
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Verify the error response details
      deepStrictEqual(err.response, {
        errorCode: 58,
        mechanisms: []
      })

      return true
    }
  )
})
