import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { readKnownTaggedFields, Reader, Writer } from '../../src/index.ts'

test('readKnownTaggedFields decodes known fields with bounded payloads and skips unknown fields', () => {
  const reader = Reader.from(
    Writer.create()
      .appendUnsignedVarInt(3)
      .appendUnsignedVarInt(0)
      .appendUnsignedVarInt(1)
      .appendInt8(7)
      .appendUnsignedVarInt(1)
      .appendUnsignedVarInt(2)
      .append(Buffer.from([1, 2]))
      .appendUnsignedVarInt(2)
      .appendUnsignedVarInt(3)
      .append(Buffer.from([3, 4, 5]))
      .appendInt8(9)
  )
  const values: number[] = []

  readKnownTaggedFields(reader, {
    0: payload => {
      values.push(payload.readInt8())
      throws(() => payload.readInt8())
    },
    1: payload => {
      values.push(payload.readInt8())
    }
  })

  deepStrictEqual(values, [7, 1])
  strictEqual(reader.readInt8(), 9)
  strictEqual(reader.remaining, 0)
})

test('readKnownTaggedFields advances past a known field when its handler does not consume it', () => {
  const reader = Reader.from(
    Writer.create()
      .appendUnsignedVarInt(1)
      .appendUnsignedVarInt(0)
      .appendUnsignedVarInt(2)
      .append(Buffer.from([1, 2]))
      .appendInt8(9)
  )

  readKnownTaggedFields(reader, { 0: () => {} })

  strictEqual(reader.readInt8(), 9)
  strictEqual(reader.remaining, 0)
})

test('readKnownTaggedFields rejects a tagged field whose declared size exceeds the response', () => {
  const reader = Reader.from(
    Writer.create().appendUnsignedVarInt(1).appendUnsignedVarInt(0).appendUnsignedVarInt(2).appendInt8(1)
  )

  throws(() => readKnownTaggedFields(reader, {}), RangeError)
})
