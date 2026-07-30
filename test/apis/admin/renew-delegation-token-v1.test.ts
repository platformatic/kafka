import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as renewDelegationTokenV1 from '../../../src/apis/admin/renew-delegation-token-v1.ts'

test('RenewDelegationToken v1 uses the legacy schema', () => {
  const request = Reader.from(renewDelegationTokenV1.createRequest(Buffer.from([1, 2]), 10n))
  deepStrictEqual([request.readBytes(false), request.readInt64()], [Buffer.from([1, 2]), 10n])
  const response = renewDelegationTokenV1.parseResponse(
    1,
    39,
    1,
    Reader.from(Writer.create().appendInt16(0).appendInt64(20n).appendInt32(4))
  )
  deepStrictEqual(
    [renewDelegationTokenV1.api.key, renewDelegationTokenV1.api.version, response],
    [39, 1, { errorCode: 0, expiryTimestampMs: 20n, throttleTimeMs: 4 }]
  )
})
