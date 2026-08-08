import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import * as endTxnV2 from '../../../src/apis/producer/end-txn-v2.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('EndTxn v2 serializes and parses the legacy schema', () => {
  strictEqual(endTxnV2.api.version, 2)
  const reader = Reader.from(endTxnV2.createRequest('tx', 1n, 2, true))
  deepStrictEqual(
    [reader.readString(false), reader.readInt64(), reader.readInt16(), reader.readBoolean()],
    ['tx', 1n, 2, true]
  )
  deepStrictEqual(endTxnV2.parseResponse(1, 26, 2, Reader.from(Writer.create().appendInt32(0).appendInt16(0))), {
    throttleTimeMs: 0,
    errorCode: 0
  })
})

test('EndTxn v2 exposes protocol errors', () => {
  throws(
    () => endTxnV2.parseResponse(1, 26, 2, Reader.from(Writer.create().appendInt32(0).appendInt16(47))),
    ResponseError
  )
})
