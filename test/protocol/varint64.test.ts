import BufferList from 'bl'
import { strictEqual } from 'node:assert'
import test from 'node:test'
import {
  int64ZigZagDecode,
  int64ZigZagEncode,
  readUnsignedVarInt64,
  readVarInt64,
  sizeOfUnsignedVarInt64,
  sizeOfVarInt64,
  writeUnsignedVarInt64,
  writeVarInt64
} from '../../src/index.ts'

test('zigzag encoding (64-bit)', () => {
  strictEqual(int64ZigZagEncode(0n), 0n)
  strictEqual(int64ZigZagEncode(-1n), 1n)
  strictEqual(int64ZigZagEncode(1n), 2n)
  strictEqual(int64ZigZagEncode(-2n), 3n)
  strictEqual(int64ZigZagEncode(2n), 4n)
})

test('zigzag decoding (64-bit)', () => {
  strictEqual(int64ZigZagDecode(0n), 0n)
  strictEqual(int64ZigZagDecode(1n), -1n)
  strictEqual(int64ZigZagDecode(2n), 1n)
  strictEqual(int64ZigZagDecode(3n), -2n)
  strictEqual(int64ZigZagDecode(4n), 2n)
})

test('size of unsigned varint (64-bit)', () => {
  strictEqual(sizeOfUnsignedVarInt64(0n), 1)
  strictEqual(sizeOfUnsignedVarInt64(127n), 1)

  strictEqual(sizeOfUnsignedVarInt64(128n), 2)
  strictEqual(sizeOfUnsignedVarInt64(16383n), 2)

  strictEqual(sizeOfUnsignedVarInt64(2147483647n), 5)
  strictEqual(sizeOfUnsignedVarInt64(9223372036854775807n), 9)
})

test('size of signed varint (64-bit)', () => {
  strictEqual(sizeOfVarInt64(0n), 1)
  strictEqual(sizeOfVarInt64(-1n), 1)
  strictEqual(sizeOfVarInt64(1n), 1)
})

test('write and read unsigned varint (64-bit)', () => {
  const values = [0n, 1n, 127n, 128n, 16383n, 16384n, 2097151n, 2097152n, 2147483647n]

  for (const value of values) {
    const buffer = writeUnsignedVarInt64(value)
    const bl = new BufferList(buffer)
    const [readValue, bytesRead] = readUnsignedVarInt64(bl, 0)

    strictEqual(readValue, value, `Read value should match original: ${value}`)
    strictEqual(bytesRead, buffer.length, `Bytes read should match buffer length: ${buffer.length}`)
  }
})

test('write and read signed varint (64-bit)', () => {
  const values = [0n, 1n, -1n, 127n, -127n, 128n, -128n, 16383n, -16383n, 16384n, -16384n]

  for (const value of values) {
    const buffer = writeVarInt64(value)
    const bl = new BufferList(buffer)
    const [readValue, bytesRead] = readVarInt64(bl, 0)

    strictEqual(readValue, value, `Read value should match original: ${value}`)
    strictEqual(bytesRead, buffer.length, `Bytes read should match buffer length: ${buffer.length}`)
  }
})
