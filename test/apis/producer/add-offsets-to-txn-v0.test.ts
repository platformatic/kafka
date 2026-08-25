import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import * as addOffsetsToTxnV0 from '../../../src/apis/producer/add-offsets-to-txn-v0.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('AddOffsetsToTxn v0 serializes and parses the legacy schema', () => {
  const reader = Reader.from(addOffsetsToTxnV0.createRequest('tx', 1n, 2, 'group'))
  deepStrictEqual(
    [reader.readString(false), reader.readInt64(), reader.readInt16(), reader.readString(false)],
    ['tx', 1n, 2, 'group']
  )
  strictEqual(addOffsetsToTxnV0.api.version, 0)
  deepStrictEqual(
    addOffsetsToTxnV0.parseResponse(1, 25, 0, Reader.from(Writer.create().appendInt32(0).appendInt16(0))),
    { throttleTimeMs: 0, errorCode: 0 }
  )
})

test('AddOffsetsToTxn v0 exposes protocol errors', () => {
  throws(
    () => addOffsetsToTxnV0.parseResponse(1, 25, 0, Reader.from(Writer.create().appendInt32(0).appendInt16(48))),
    ResponseError
  )
})
