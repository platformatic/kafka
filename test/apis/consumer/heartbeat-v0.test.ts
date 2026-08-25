import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import * as codec from '../../../src/apis/consumer/heartbeat-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'
test('Heartbeat v0 normalizes its response throttle time', () => {
  deepStrictEqual(codec.parseResponse(1, 12, 0, Reader.from(Writer.create().appendInt16(0))), {
    throttleTimeMs: 0,
    errorCode: 0
  })
})
