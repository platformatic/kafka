import { strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, saslAuthenticateV2, Writer } from '../../../src/index.ts'

test('SaslAuthenticate v2 consumes unknown root tags before errors', () => {
  const reader = Reader.from(
    Writer.create()
      .appendInt16(1)
      .appendString(null)
      .appendBytes(Buffer.alloc(0))
      .appendInt64(0n)
      .appendUnsignedVarInt(1)
      .appendUnsignedVarInt(42)
      .appendUnsignedVarInt(2)
      .append(Buffer.from([1, 2]))
  )
  throws(() => saslAuthenticateV2.parseResponse(1, 36, 2, reader), ResponseError)
  strictEqual(reader.remaining, 0)
})
