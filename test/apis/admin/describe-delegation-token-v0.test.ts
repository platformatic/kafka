import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as describeDelegationTokenV0 from '../../../src/apis/admin/describe-delegation-token-v0.ts'

test('DescribeDelegationToken v0 uses the legacy schema', () => {
  const request = Reader.from(
    describeDelegationTokenV0.createRequest([{ principalType: 'User', principalName: 'owner' }])
  )
  deepStrictEqual(
    request.readArray(r => [r.readString(false), r.readString(false)], false, false),
    [['User', 'owner']]
  )
  const response = describeDelegationTokenV0.parseResponse(
    1,
    41,
    0,
    Reader.from(
      Writer.create()
        .appendInt16(0)
        .appendArray(
          [{ principalType: 'User', principalName: 'owner' }],
          (w, token) =>
            w
              .appendString(token.principalType, false)
              .appendString(token.principalName, false)
              .appendInt64(1n)
              .appendInt64(2n)
              .appendInt64(3n)
              .appendString('token', false)
              .appendBytes(Buffer.from([1]), false)
              .appendArray([], () => {}, false, false),
          false,
          false
        )
        .appendInt32(4)
    )
  )
  deepStrictEqual(
    [describeDelegationTokenV0.api.version, response.tokens[0].tokenId, response.tokens[0].tokenRequesterPrincipalType, response.throttleTimeMs],
    [0, 'token', '', 4]
  )
})
