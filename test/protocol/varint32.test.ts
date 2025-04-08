import BufferList from 'bl'
import { strictEqual } from 'node:assert'
import test from 'node:test'
import {
  intZigZagDecode,
  intZigZagEncode,
  readUnsignedVarInt,
  readVarInt,
  sizeOfUnsignedVarInt,
  sizeOfVarInt,
  writeUnsignedVarInt,
  writeVarInt
} from '../../src/index.ts'

test('zigzag encoding (32-bit)', () => {
  strictEqual(intZigZagEncode(0), 0)
  strictEqual(intZigZagEncode(-1), 1)
  strictEqual(intZigZagEncode(1), 2)
  strictEqual(intZigZagEncode(-2), 3)
  strictEqual(intZigZagEncode(2), 4)
})

test('zigzag decoding (32-bit)', () => {
  strictEqual(intZigZagDecode(0), 0)
  strictEqual(intZigZagDecode(1), -1)
  strictEqual(intZigZagDecode(2), 1)
  strictEqual(intZigZagDecode(3), -2)
  strictEqual(intZigZagDecode(4), 2)
})

test('size of unsigned varint (32-bit)', () => {
  strictEqual(sizeOfUnsignedVarInt(0), 1)
  strictEqual(sizeOfUnsignedVarInt(127), 1)

  strictEqual(sizeOfUnsignedVarInt(128), 2)
  strictEqual(sizeOfUnsignedVarInt(16383), 2)

  strictEqual(sizeOfUnsignedVarInt(16384), 3)
  strictEqual(sizeOfUnsignedVarInt(2097151), 3)

  strictEqual(sizeOfUnsignedVarInt(2097152), 4)
  strictEqual(sizeOfUnsignedVarInt(268435455), 4)

  strictEqual(sizeOfUnsignedVarInt(268435456), 5)
  strictEqual(sizeOfUnsignedVarInt(2147483647), 5)
})

test('size of signed varint (32-bit)', () => {
  strictEqual(sizeOfVarInt(0), 1)
  strictEqual(sizeOfVarInt(-1), 1)
  strictEqual(sizeOfVarInt(1), 1)
})

test('write and read unsigned varint (32-bit)', () => {
  const values = [0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455, 268435456]

  for (const value of values) {
    const buffer = writeUnsignedVarInt(value)
    const bl = new BufferList(buffer)
    const [readValue, bytesRead] = readUnsignedVarInt(bl, 0)

    strictEqual(readValue, value, `Read value should match original: ${value}`)
    strictEqual(bytesRead, buffer.length, `Bytes read should match buffer length: ${buffer.length}`)
  }
})

test('write and read signed varint (32-bit)', () => {
  const values = [0, 1, -1, 127, -127, 128, -128, 16383, -16383, 16384, -16384]

  for (const value of values) {
    const buffer = writeVarInt(value)
    const bl = new BufferList(buffer)
    const [readValue, bytesRead] = readVarInt(bl, 0)

    strictEqual(readValue, value, `Read value should match original: ${value}`)
    strictEqual(bytesRead, buffer.length, `Bytes read should match buffer length: ${buffer.length}`)
  }
})
