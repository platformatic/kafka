import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, saslAuthenticateV1, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = saslAuthenticateV1

test('createRequest serializes classic auth bytes', () => {
  const writer = createRequest(Buffer.from('secret'))

  ok(writer instanceof Writer)
  deepStrictEqual(writer.buffer, Buffer.from('00000006736563726574', 'hex'))
})

test('uses classic request and response headers without tags', () => {
  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, Buffer.from('secret'))

  deepStrictEqual({ key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] }, {
    key: 36,
    version: 1,
    requestTags: false,
    responseTags: false
  })
})

test('parseResponse processes a successful response wire fixture', () => {
  const reader = Reader.from(Buffer.from('0000ffff00000005746f6b656e00000000000003e8', 'hex'))

  deepStrictEqual(parseResponse(1, 36, 1, reader), {
    errorCode: 0,
    errorMessage: null,
    authBytes: Buffer.from('token'),
    sessionLifetimeMs: 1000n
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse reports errors from the response wire fixture', () => {
  const reader = Reader.from(Buffer.from('003affff000000000000000000000000', 'hex'))

  throws(() => parseResponse(1, 36, 1, reader), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.response, {
      errorCode: 58,
      errorMessage: null,
      authBytes: Buffer.alloc(0),
      sessionLifetimeMs: 0n
    })
    strictEqual(reader.remaining, 0)
    return true
  })
})
