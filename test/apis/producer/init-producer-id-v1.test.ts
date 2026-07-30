import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import * as initProducerIdV1 from '../../../src/apis/producer/init-producer-id-v1.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('InitProducerId v1 serializes and parses the legacy schema', () => {
  strictEqual(initProducerIdV1.api.version, 1)
  const reader = Reader.from(initProducerIdV1.createRequest(null, 30_000, 1n, 2))
  deepStrictEqual([reader.readNullableString(false), reader.readInt32()], [null, 30_000])
  strictEqual(reader.remaining, 0)
  deepStrictEqual(
    initProducerIdV1.parseResponse(
      1,
      22,
      1,
      Reader.from(Writer.create().appendInt32(0).appendInt16(0).appendInt64(1n).appendInt16(2))
    ),
    { throttleTimeMs: 0, errorCode: 0, producerId: 1n, producerEpoch: 2 }
  )
})

test('InitProducerId v1 exposes protocol errors', () => {
  throws(
    () =>
      initProducerIdV1.parseResponse(
        1,
        22,
        1,
        Reader.from(Writer.create().appendInt32(0).appendInt16(49).appendInt64(-1n).appendInt16(-1))
      ),
    ResponseError
  )
})
