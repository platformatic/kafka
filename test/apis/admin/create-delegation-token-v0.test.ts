import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as createDelegationTokenV0 from '../../../src/apis/admin/create-delegation-token-v0.ts'

test('CreateDelegationToken v0 uses the legacy schema', () => {
  const writer = createDelegationTokenV0.createRequest(
    null,
    null,
    [{ principalType: 'User', principalName: 'renewer' }],
    10n
  )
  const request = Reader.from(writer)
  deepStrictEqual(
    [
      request.readArray(r => [r.readString(false), r.readString(false)], false, false),
      request.readInt64()
    ],
    [[['User', 'renewer']], 10n]
  )

  const response = createDelegationTokenV0.parseResponse(
    1,
    38,
    0,
    Reader.from(
      Writer.create()
        .appendInt16(0)
        .appendString('User', false)
        .appendString('owner', false)
        .appendInt64(1n)
        .appendInt64(2n)
        .appendInt64(3n)
        .appendString('token', false)
        .appendBytes(Buffer.from([1]), false)
        .appendInt32(4)
    )
  )
  deepStrictEqual(
    [createDelegationTokenV0.api.key, createDelegationTokenV0.api.version, response.tokenId, response.tokenRequesterPrincipalType, response.throttleTimeMs],
    [38, 0, 'token', '', 4]
  )
})
