import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as expireDelegationTokenV1 from '../../../src/apis/admin/expire-delegation-token-v1.ts'

test('ExpireDelegationToken v1 uses the legacy schema', () => {
  const request = Reader.from(expireDelegationTokenV1.createRequest(Buffer.from([1, 2]), 10n))
  deepStrictEqual([request.readBytes(false), request.readInt64()], [Buffer.from([1, 2]), 10n])
  const response = expireDelegationTokenV1.parseResponse(
    1,
    40,
    1,
    Reader.from(Writer.create().appendInt16(0).appendInt64(20n).appendInt32(4))
  )
  deepStrictEqual(
    [expireDelegationTokenV1.api.key, expireDelegationTokenV1.api.version, response],
    [40, 1, { errorCode: 0, expiryTimestampMs: 20n, throttleTimeMs: 4 }]
  )
})
