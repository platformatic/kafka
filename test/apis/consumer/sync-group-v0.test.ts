import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import * as codec from '../../../src/apis/consumer/sync-group-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'
test('SyncGroup v0 normalizes protocol fields and throttle time', () => {
  deepStrictEqual(
    codec.parseResponse(1, 14, 0, Reader.from(Writer.create().appendInt16(0).appendBytes(Buffer.alloc(0), false))),
    { throttleTimeMs: 0, errorCode: 0, protocolType: null, protocolName: null, assignment: Buffer.alloc(0) }
  )
})
