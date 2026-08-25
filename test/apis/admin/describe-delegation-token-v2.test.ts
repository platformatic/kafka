import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as describeDelegationTokenV2 from '../../../src/apis/admin/describe-delegation-token-v2.ts'

test('DescribeDelegationToken v2 uses the flexible schema', () => {
  const request = Reader.from(
    describeDelegationTokenV2.createRequest([{ principalType: 'User', principalName: 'owner' }])
  )
  deepStrictEqual(
    [request.readArray(r => [r.readString(), r.readString()]), request.readUnsignedVarInt()],
    [[['User', 'owner']], 0]
  )
  const response = describeDelegationTokenV2.parseResponse(
    1,
    41,
    2,
    Reader.from(
      Writer.create()
        .appendInt16(0)
        .appendArray(
          [{ principalType: 'User', principalName: 'owner' }],
          (w, token) =>
            w
              .appendString(token.principalType)
              .appendString(token.principalName)
              .appendInt64(1n)
              .appendInt64(2n)
              .appendInt64(3n)
              .appendString('token')
              .appendBytes(Buffer.from([1]))
              .appendArray([], () => {})
        )
        .appendInt32(4)
        .appendTaggedFields()
    )
  )
  deepStrictEqual(
    [describeDelegationTokenV2.api.version, response.tokens[0].tokenId, response.tokens[0].tokenRequesterPrincipalType, response.throttleTimeMs],
    [2, 'token', '', 4]
  )
})
