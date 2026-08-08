import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, saslHandshakeV0, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = saslHandshakeV0

test('createRequest serializes the SASL mechanism', () => {
  const writer = createRequest('SCRAM-SHA-256')

  ok(writer instanceof Writer)
  deepStrictEqual(writer.buffer, Buffer.from('000d534352414d2d5348412d323536', 'hex'))
})

test('uses classic request and response headers without tags', () => {
  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, 'PLAIN')

  deepStrictEqual({ key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] }, {
    key: 17,
    version: 0,
    requestTags: false,
    responseTags: false
  })
})

test('parseResponse processes a successful response wire fixture', () => {
  const reader = Reader.from(Buffer.from('0000000000020005504c41494e000d534352414d2d5348412d323536', 'hex'))

  deepStrictEqual(parseResponse(1, 17, 0, reader), {
    errorCode: 0,
    mechanisms: ['PLAIN', 'SCRAM-SHA-256']
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse reports errors from the response wire fixture', () => {
  const reader = Reader.from(Buffer.from('002100000000', 'hex'))

  throws(() => parseResponse(1, 17, 0, reader), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, {
      errorCode: 33,
      mechanisms: []
    })
    strictEqual(reader.remaining, 0)
    return true
  })
})
