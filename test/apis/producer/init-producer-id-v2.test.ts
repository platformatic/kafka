import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import * as initProducerIdV2 from '../../../src/apis/producer/init-producer-id-v2.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('InitProducerId v2 serializes and parses the flexible schema', () => {
  strictEqual(initProducerIdV2.api.version, 2)
  const reader = Reader.from(initProducerIdV2.createRequest(null, 30_000, 1n, 2))
  deepStrictEqual([reader.readNullableString(true), reader.readInt32(), reader.readUnsignedVarInt()], [null, 30_000, 0])
  strictEqual(reader.remaining, 0)
  deepStrictEqual(
    initProducerIdV2.parseResponse(
      1,
      22,
      2,
      Reader.from(Writer.create().appendInt32(0).appendInt16(0).appendInt64(1n).appendInt16(2).appendTaggedFields())
    ),
    { throttleTimeMs: 0, errorCode: 0, producerId: 1n, producerEpoch: 2 }
  )
})

test('InitProducerId v2 exposes protocol errors', () => {
  throws(
    () =>
      initProducerIdV2.parseResponse(
        1,
        22,
        2,
        Reader.from(Writer.create().appendInt32(0).appendInt16(49).appendInt64(-1n).appendInt16(-1).appendTaggedFields())
      ),
    ResponseError
  )
})
