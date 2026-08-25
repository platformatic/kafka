import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as renewDelegationTokenV0 from '../../../src/apis/admin/renew-delegation-token-v0.ts'

test('RenewDelegationToken v0 uses the legacy schema', () => {
  const request = Reader.from(renewDelegationTokenV0.createRequest(Buffer.from([1, 2]), 10n))
  deepStrictEqual([request.readBytes(false), request.readInt64()], [Buffer.from([1, 2]), 10n])
  const response = renewDelegationTokenV0.parseResponse(
    1,
    39,
    0,
    Reader.from(Writer.create().appendInt16(0).appendInt64(20n).appendInt32(4))
  )
  deepStrictEqual(
    [renewDelegationTokenV0.api.key, renewDelegationTokenV0.api.version, response],
    [39, 0, { errorCode: 0, expiryTimestampMs: 20n, throttleTimeMs: 4 }]
  )
})
