import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as createDelegationTokenV1 from '../../../src/apis/admin/create-delegation-token-v1.ts'

test('CreateDelegationToken v1 uses the legacy schema', () => {
  const writer = createDelegationTokenV1.createRequest(
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

  const response = createDelegationTokenV1.parseResponse(
    1,
    38,
    1,
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
    [createDelegationTokenV1.api.key, createDelegationTokenV1.api.version, response.tokenId, response.tokenRequesterPrincipalType, response.throttleTimeMs],
    [38, 1, 'token', '', 4]
  )
})
