import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as expireDelegationTokenV0 from '../../../src/apis/admin/expire-delegation-token-v0.ts'

test('ExpireDelegationToken v0 uses the legacy schema', () => {
  const request = Reader.from(expireDelegationTokenV0.createRequest(Buffer.from([1, 2]), 10n))
  deepStrictEqual([request.readBytes(false), request.readInt64()], [Buffer.from([1, 2]), 10n])
  const response = expireDelegationTokenV0.parseResponse(
    1,
    40,
    0,
    Reader.from(Writer.create().appendInt16(0).appendInt64(20n).appendInt32(4))
  )
  deepStrictEqual(
    [expireDelegationTokenV0.api.key, expireDelegationTokenV0.api.version, response],
    [40, 0, { errorCode: 0, expiryTimestampMs: 20n, throttleTimeMs: 4 }]
  )
})
