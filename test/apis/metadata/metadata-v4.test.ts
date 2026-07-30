import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { metadataV4, Reader, Writer } from '../../../src/index.ts'

test('Metadata v4 adds auto-create topics while retaining the classic response', () => {
  const request = Reader.from(metadataV4.createRequest(['topic'], true))
  deepStrictEqual(
    request.readArray(r => r.readString(false), false, false),
    ['topic']
  )
  strictEqual(request.readBoolean(), true)
  const response = metadataV4.parseResponse(
    1,
    3,
    4,
    Reader.from(
      Writer.create()
        .appendInt32(0)
        .appendArray([], () => {}, false, false)
        .appendString(null, false)
        .appendInt32(-1)
        .appendArray(
          ['topic'],
          (w, topic) => w.appendInt16(0).appendString(topic, false).appendBoolean(true).appendArray([], () => {}, false, false),
          false,
          false
        )
    )
  )
  strictEqual(response.throttleTimeMs, 0)
  strictEqual(response.topics[0].isInternal, true)
})

test('Metadata v4 enables auto-create topics by default', () => {
  const request = Reader.from(metadataV4.createRequest(null))
  request.readNullableArray(() => '', false)
  strictEqual(request.readBoolean(), true)
})
