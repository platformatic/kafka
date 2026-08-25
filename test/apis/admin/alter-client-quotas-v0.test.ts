import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as alterClientQuotasV0 from '../../../src/apis/admin/alter-client-quotas-v0.ts'
import { ClientQuotaEntityTypes, ClientQuotaKeys } from '../../../src/apis/enumerations.ts'

test('AlterClientQuotas v0 uses the legacy schema', () => {
  const request = Reader.from(
    alterClientQuotasV0.createRequest(
      [
        {
          entities: [{ entityType: ClientQuotaEntityTypes.IP, entityName: '127.0.0.1' }],
          ops: [{ key: ClientQuotaKeys.CONNECTION_CREATION_RATE, value: 1.5, remove: false }]
        }
      ],
      true
    )
  )
  deepStrictEqual(
    request.readArray(
      r => [
        r.readArray(r => [r.readString(false), r.readNullableString(false)], false, false),
        r.readArray(r => [r.readString(false), r.readFloat64(), r.readBoolean()], false, false)
      ],
      false,
      false
    ),
    [[[[ClientQuotaEntityTypes.IP, '127.0.0.1']], [[ClientQuotaKeys.CONNECTION_CREATION_RATE, 1.5, false]]]]
  )
  deepStrictEqual(request.readBoolean(), true)

  const response = alterClientQuotasV0.parseResponse(
    1,
    49,
    0,
    Reader.from(
      Writer.create()
        .appendInt32(4)
        .appendArray(
          [{ entityType: ClientQuotaEntityTypes.IP, entityName: '127.0.0.1' }],
          (w, entity) =>
            w
              .appendInt16(0)
              .appendString(null, false)
              .appendArray(
                [entity],
                (w, entity) => w.appendString(entity.entityType, false).appendString(entity.entityName, false),
                false,
                false
              ),
          false,
          false
        )
    )
  )
  deepStrictEqual(
    [alterClientQuotasV0.api.key, alterClientQuotasV0.api.version, response],
    [
      49,
      0,
      {
        throttleTimeMs: 4,
        entries: [{ errorCode: 0, errorMessage: null, entity: [{ entityType: ClientQuotaEntityTypes.IP, entityName: '127.0.0.1' }] }]
      }
    ]
  )
})
