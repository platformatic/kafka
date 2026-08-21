import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { findCoordinatorV3, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = findCoordinatorV3

test('createRequest serializes a group coordinator key with empty tags', () => {
  const writer = createRequest(0, ['group-1'])

  ok(writer instanceof Writer)
  deepStrictEqual(writer.buffer, Buffer.from('0867726f75702d310000', 'hex'))
})

test('createRequest serializes a transaction coordinator key with empty tags', () => {
  const writer = createRequest(1, ['transaction-1'])

  ok(writer instanceof Writer)
  deepStrictEqual(writer.buffer, Buffer.from('0e7472616e73616374696f6e2d310100', 'hex'))
})

test('createRequest uses the first key and defaults an empty key', () => {
  deepStrictEqual(createRequest(2, ['group-1', 'group-2']).buffer, Buffer.from('0867726f75702d310200', 'hex'))
  deepStrictEqual(createRequest(2, []).buffer, Buffer.from('010200', 'hex'))
})

test('uses flexible request and response headers with tags', () => {
  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, 0, ['group-1'])

  deepStrictEqual({ key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] }, {
    key: 10,
    version: 3,
    requestTags: true,
    responseTags: true
  })
})

test('parseResponse processes the singular successful response wire fixture with empty tags', () => {
  const reader = Reader.from(Buffer.from('00000000000000000000010962726f6b65722d310000238400', 'hex'))

  deepStrictEqual(parseResponse(1, 10, 3, reader), {
    throttleTimeMs: 0,
    coordinators: [{ key: '', nodeId: 1, host: 'broker-1', port: 9092, errorCode: 0, errorMessage: null }]
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse processes a throttled successful response wire fixture with empty tags', () => {
  const reader = Reader.from(Buffer.from('00000064000000000000010962726f6b65722d310000238400', 'hex'))

  deepStrictEqual(parseResponse(1, 10, 3, reader), {
    throttleTimeMs: 100,
    coordinators: [{ key: '', nodeId: 1, host: 'broker-1', port: 9092, errorCode: 0, errorMessage: null }]
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse reports coordinator errors from the response wire fixture with empty tags', () => {
  const reader = Reader.from(Buffer.from('00000000000f1a436f6f7264696e61746f72206e6f7420617661696c61626c65ffffffff010000000000', 'hex'))

  throws(() => parseResponse(1, 10, 3, reader), error => {
    ok(error instanceof ResponseError)
    deepStrictEqual(error.errors.map(({ path, apiCode }) => ({ path, apiCode })), [{ path: '/coordinators/0', apiCode: 15 }])
    deepStrictEqual(error.response, {
      throttleTimeMs: 0,
      coordinators: [{ key: '', nodeId: -1, host: '', port: 0, errorCode: 15, errorMessage: 'Coordinator not available' }]
    })
    strictEqual(reader.remaining, 0)
    return true
  })
})
