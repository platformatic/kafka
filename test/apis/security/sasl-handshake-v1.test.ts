import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, saslHandshakeV1, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = saslHandshakeV1

test('createRequest serializes mechanism correctly', () => {
  const mechanism = 'SCRAM-SHA-256'
  const writer = createRequest(mechanism)

  ok(writer instanceof Writer)
  deepStrictEqual(writer.buffer, Buffer.from('000d534352414d2d5348412d323536', 'hex'))
})

test('createRequest handles PLAIN mechanism', () => {
  const mechanism = 'PLAIN'
  const writer = createRequest(mechanism)

  ok(writer instanceof Writer)
  deepStrictEqual(writer.buffer, Buffer.from('0005504c41494e', 'hex'))
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const reader = Reader.from(Buffer.from('0000000000030005504c41494e000d534352414d2d5348412d323536000d534352414d2d5348412d353132', 'hex'))
  const response = parseResponse(1, 17, 1, reader)

  // Verify structure
  deepStrictEqual(response, {
    errorCode: 0,
    mechanisms: ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt16(33) // errorCode (UNSUPPORTED_SASL_MECHANISM)
    // Mechanisms array (empty because the requested mechanism is not supported)
    .appendArray([], () => {}, false, false) // empty non-compact array

  throws(
    () => {
      parseResponse(1, 17, saslHandshakeV1.api.version, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Verify the error response details
      deepStrictEqual(err.response, {
        errorCode: 33,
        mechanisms: []
      })

      return true
    }
  )
})
