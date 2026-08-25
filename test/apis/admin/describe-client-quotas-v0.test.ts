import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer } from '../../../src/index.ts'
import * as describeClientQuotasV0 from '../../../src/apis/admin/describe-client-quotas-v0.ts'
import { ClientQuotaEntityTypes, ClientQuotaKeys, ClientQuotaMatchTypes } from '../../../src/apis/enumerations.ts'

test('DescribeClientQuotas v0 uses the classic schema', () => {
  const request = Reader.from(
    describeClientQuotasV0.createRequest([{ entityType: ClientQuotaEntityTypes.IP, matchType: ClientQuotaMatchTypes.EXACT, match: '127.0.0.1' }], false)
  )
  deepStrictEqual(
    [
      request.readArray(r => [r.readString(false), r.readInt8(), r.readNullableString(false)], false, false),
      request.readBoolean()
    ],
    [[['ip', ClientQuotaMatchTypes.EXACT, '127.0.0.1']], false]
  )
  const response = describeClientQuotasV0.parseResponse(
    1,
    48,
    0,
    Reader.from(
      Writer.create()
        .appendInt32(0)
        .appendInt16(0)
        .appendString(null, false)
        .appendArray(
          [
            {
              entity: [{ entityType: ClientQuotaEntityTypes.IP, entityName: '127.0.0.1' }],
              values: [{ key: ClientQuotaKeys.CONNECTION_CREATION_RATE, value: 1 }]
            }
          ],
          (writer, entry) =>
            writer
              .appendArray(
                entry.entity,
                (writer, entity) =>
                  writer.appendString(entity.entityType, false).appendString(entity.entityName, false),
                false,
                false
              )
              .appendArray(
                entry.values,
                (writer, value) => writer.appendString(value.key, false).appendFloat64(value.value),
                false,
                false
              ),
          false,
          false
        )
    )
  )
  deepStrictEqual(response.entries, [
    { entity: [{ entityType: ClientQuotaEntityTypes.IP, entityName: '127.0.0.1' }], values: [{ key: ClientQuotaKeys.CONNECTION_CREATION_RATE, value: 1 }] }
  ])
})

test('DescribeClientQuotas v0 preserves null and empty entries', () => {
  const nullEntries = describeClientQuotasV0.parseResponse(
    1,
    48,
    0,
    Reader.from(
      Writer.create()
        .appendInt32(0)
        .appendInt16(0)
        .appendString(null, false)
        .appendArray(null, () => {}, false, false)
    )
  )
  const emptyEntries = describeClientQuotasV0.parseResponse(
    1,
    48,
    0,
    Reader.from(
      Writer.create()
        .appendInt32(0)
        .appendInt16(0)
        .appendString(null, false)
        .appendArray([], () => {}, false, false)
    )
  )

  deepStrictEqual(nullEntries.entries, null)
  deepStrictEqual(emptyEntries.entries, [])
})

test('DescribeClientQuotas v0 preserves errors with null entries', () => {
  throws(
    () =>
      describeClientQuotasV0.parseResponse(
        1,
        48,
        0,
        Reader.from(
          Writer.create()
            .appendInt32(0)
            .appendInt16(42)
            .appendString('quota error', false)
            .appendArray(null, () => {}, false, false)
        )
      ),
    error => {
      ok(error instanceof ResponseError)
      deepStrictEqual(error.response, { throttleTimeMs: 0, errorCode: 42, errorMessage: 'quota error', entries: null })
      return true
    }
  )
})
