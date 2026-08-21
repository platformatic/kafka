import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { findCoordinatorV2, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { api, createRequest, parseResponse } = findCoordinatorV2

test('createRequest serializes a group coordinator key', () => {
  const writer = createRequest(0, ['group-1'])

  ok(writer instanceof Writer)
  deepStrictEqual(writer.buffer, Buffer.from('000767726f75702d3100', 'hex'))
})

test('createRequest serializes a transaction coordinator key', () => {
  const writer = createRequest(1, ['transaction-1'])

  ok(writer instanceof Writer)
  deepStrictEqual(writer.buffer, Buffer.from('000d7472616e73616374696f6e2d3101', 'hex'))
})

test('createRequest uses the first key and defaults an empty key', () => {
  deepStrictEqual(createRequest(2, ['group-1', 'group-2']).buffer, Buffer.from('000767726f75702d3102', 'hex'))
  deepStrictEqual(createRequest(2, []).buffer, Buffer.from('000002', 'hex'))
})

test('uses classic request and response headers without tags', () => {
  let sent: unknown[] = []
  api({ send: (...args: unknown[]) => { sent = args } } as never, 0, ['group-1'])

  deepStrictEqual({ key: api.key, version: api.version, requestTags: sent[4], responseTags: sent[5] }, {
    key: 10,
    version: 2,
    requestTags: false,
    responseTags: false
  })
})

test('parseResponse processes the singular successful response wire fixture', () => {
  const reader = Reader.from(Buffer.from('000000000000ffff00000001000862726f6b65722d3100002384', 'hex'))

  deepStrictEqual(parseResponse(1, 10, 2, reader), {
    throttleTimeMs: 0,
    coordinators: [{ key: '', nodeId: 1, host: 'broker-1', port: 9092, errorCode: 0, errorMessage: null }]
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse processes a throttled successful response wire fixture', () => {
  const reader = Reader.from(Buffer.from('000000640000ffff00000001000862726f6b65722d3100002384', 'hex'))

  deepStrictEqual(parseResponse(1, 10, 2, reader), {
    throttleTimeMs: 100,
    coordinators: [{ key: '', nodeId: 1, host: 'broker-1', port: 9092, errorCode: 0, errorMessage: null }]
  })
  strictEqual(reader.remaining, 0)
})

test('parseResponse reports coordinator errors from the response wire fixture', () => {
  const reader = Reader.from(Buffer.from('00000000000f0019436f6f7264696e61746f72206e6f7420617661696c61626c65ffffffff000000000000', 'hex'))

  throws(() => parseResponse(1, 10, 2, reader), error => {
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
