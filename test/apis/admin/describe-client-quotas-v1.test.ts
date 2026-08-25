import { deepStrictEqual, ok, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer } from '../../../src/index.ts'
import * as describeClientQuotasV1 from '../../../src/apis/admin/describe-client-quotas-v1.ts'
import { ClientQuotaEntityTypes, ClientQuotaKeys, ClientQuotaMatchTypes } from '../../../src/apis/enumerations.ts'

test('DescribeClientQuotas v1 uses the flexible schema', () => {
  const request = Reader.from(
    describeClientQuotasV1.createRequest([{ entityType: ClientQuotaEntityTypes.IP, matchType: ClientQuotaMatchTypes.EXACT, match: '127.0.0.1' }], false)
  )
  deepStrictEqual(
    [
      request.readArray(r => [r.readString(), r.readInt8(), r.readNullableString()]),
      request.readBoolean(),
      request.readUnsignedVarInt()
    ],
    [[['ip', ClientQuotaMatchTypes.EXACT, '127.0.0.1']], false, 0]
  )
  const response = describeClientQuotasV1.parseResponse(
    1,
    48,
    1,
    Reader.from(
      Writer.create()
        .appendInt32(0)
        .appendInt16(0)
        .appendString(null)
        .appendArray(
          [
            {
              entity: [{ entityType: ClientQuotaEntityTypes.IP, entityName: '127.0.0.1' }],
              values: [{ key: ClientQuotaKeys.CONTROLLER_MUTATION_RATE, value: 1 }]
            }
          ],
          (writer, entry) =>
            writer
              .appendArray(entry.entity, (writer, entity) =>
                writer.appendString(entity.entityType).appendString(entity.entityName))
              .appendArray(entry.values, (writer, value) => writer.appendString(value.key).appendFloat64(value.value))
        )
        .appendTaggedFields()
    )
  )
  deepStrictEqual(response.entries, [
    { entity: [{ entityType: ClientQuotaEntityTypes.IP, entityName: '127.0.0.1' }], values: [{ key: ClientQuotaKeys.CONTROLLER_MUTATION_RATE, value: 1 }] }
  ])
})

test('DescribeClientQuotas v1 serializes a nullable exact match', () => {
  const request = Reader.from(describeClientQuotasV1.createRequest([{ entityType: 'client-id', matchType: ClientQuotaMatchTypes.EXACT, match: null }], false))

  deepStrictEqual(request.readArray(r => [r.readString(), r.readInt8(), r.readNullableString()]), [['client-id', ClientQuotaMatchTypes.EXACT, null]])
})

test('DescribeClientQuotas v1 preserves null and empty entries with flexible tags', () => {
  const nullReader = Reader.from(
    Writer.create()
      .appendInt32(0)
      .appendInt16(0)
      .appendString(null)
      .appendArray(null, () => {})
      .appendTaggedFields()
  )
  const nullEntries = describeClientQuotasV1.parseResponse(1, 48, 1, nullReader)
  const emptyReader = Reader.from(
    Writer.create()
      .appendInt32(0)
      .appendInt16(0)
      .appendString(null)
      .appendArray([], () => {})
      .appendTaggedFields()
  )
  const emptyEntries = describeClientQuotasV1.parseResponse(1, 48, 1, emptyReader)

  deepStrictEqual(nullEntries.entries, null)
  deepStrictEqual(emptyEntries.entries, [])
  strictEqual(nullReader.remaining, 0)
  strictEqual(emptyReader.remaining, 0)
})

test('DescribeClientQuotas v1 preserves errors with null entries', () => {
  throws(
    () =>
      describeClientQuotasV1.parseResponse(
        1,
        48,
        1,
        Reader.from(
          Writer.create()
            .appendInt32(0)
            .appendInt16(42)
            .appendString('quota error')
            .appendArray(null, () => {})
            .appendTaggedFields()
        )
      ),
    error => {
      ok(error instanceof ResponseError)
      deepStrictEqual(error.response, { throttleTimeMs: 0, errorCode: 42, errorMessage: 'quota error', entries: null })
      return true
    }
  )
})
