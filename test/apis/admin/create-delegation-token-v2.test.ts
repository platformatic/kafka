import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { Reader, Writer } from '../../../src/index.ts'
import * as createDelegationTokenV2 from '../../../src/apis/admin/create-delegation-token-v2.ts'

test('CreateDelegationToken v2 uses the flexible schema', () => {
  const writer = createDelegationTokenV2.createRequest(null, null, [{ principalType: 'User', principalName: 'renewer' }], 10n)
  const request = Reader.from(writer)
  deepStrictEqual(
    [
      request.readArray(r => [r.readString(), r.readString()]),
      request.readInt64(),
      request.readUnsignedVarInt()
    ],
    [[['User', 'renewer']], 10n, 0]
  )

  const response = createDelegationTokenV2.parseResponse(
    1,
    38,
    2,
    Reader.from(
      Writer.create()
        .appendInt16(0)
        .appendString('User')
        .appendString('owner')
        .appendInt64(1n)
        .appendInt64(2n)
        .appendInt64(3n)
        .appendString('token')
        .appendBytes(Buffer.from([1]))
        .appendInt32(4)
        .appendTaggedFields()
    )
  )
  deepStrictEqual(
    [createDelegationTokenV2.api.key, createDelegationTokenV2.api.version, response.tokenId, response.tokenRequesterPrincipalType, response.throttleTimeMs],
    [38, 2, 'token', '', 4]
  )
})
