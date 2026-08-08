import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as describeDelegationTokenV1 from '../../../src/apis/admin/describe-delegation-token-v1.ts'

test('DescribeDelegationToken v1 uses the legacy schema', () => {
  const request = Reader.from(
    describeDelegationTokenV1.createRequest([{ principalType: 'User', principalName: 'owner' }])
  )
  deepStrictEqual(
    request.readArray(r => [r.readString(false), r.readString(false)], false, false),
    [['User', 'owner']]
  )
  const response = describeDelegationTokenV1.parseResponse(
    1,
    41,
    1,
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
    [describeDelegationTokenV1.api.version, response.tokens[0].tokenId, response.tokens[0].tokenRequesterPrincipalType, response.throttleTimeMs],
    [1, 'token', '', 4]
  )
})
