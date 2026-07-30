import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { apiVersionsV1, protocolAPIsById, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = apiVersionsV1

test('createRequest handles empty values', () => {
  const writer = createRequest('', '')

  ok(writer instanceof Writer)
  deepStrictEqual(writer.length, 0)
})

test('parseResponse correctly processes a successful response', () => {
  const writer = Writer.create()
    .appendInt16(0)
    .appendArray(
      [
        { apiKey: 0, minVersion: 0, maxVersion: 9 },
        { apiKey: 1, minVersion: 0, maxVersion: 12 }
      ],
      (w, api) => {
        w.appendInt16(api.apiKey).appendInt16(api.minVersion).appendInt16(api.maxVersion)
      },
      false,
      false
    )
    .appendInt32(0)

  deepStrictEqual(parseResponse(1, 18, 1, Reader.from(writer)), {
    errorCode: 0,
    apiKeys: [
      { apiKey: 0, name: protocolAPIsById[0], minVersion: 0, maxVersion: 9 },
      { apiKey: 1, name: protocolAPIsById[1], minVersion: 0, maxVersion: 12 }
    ],
    throttleTimeMs: 0
  })
})

test('parseResponse handles response with throttling', () => {
  const writer = Writer.create()
    .appendInt16(0)
    .appendArray([{ apiKey: 0, minVersion: 0, maxVersion: 9 }], (w, api) => {
      w.appendInt16(api.apiKey).appendInt16(api.minVersion).appendInt16(api.maxVersion)
    }, false, false)
    .appendInt32(100)

  deepStrictEqual(parseResponse(1, 18, 1, Reader.from(writer)), {
    errorCode: 0,
    apiKeys: [{ apiKey: 0, name: protocolAPIsById[0], minVersion: 0, maxVersion: 9 }],
    throttleTimeMs: 100
  })
})

test('parseResponse throws error on non-zero error code', () => {
  const writer = Writer.create().appendInt16(42).appendArray([], () => {}, false, false).appendInt32(0)

  throws(
    () => parseResponse(1, 18, 1, Reader.from(writer)),
    error => {
      ok(error instanceof ResponseError)
      ok(error.message.includes('Received response with error while executing API'))
      deepStrictEqual(error.response, { errorCode: 42, throttleTimeMs: 0, apiKeys: [] })
      return true
    }
  )
})
