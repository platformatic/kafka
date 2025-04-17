import { strictEqual } from 'node:assert'
import test from 'node:test'
import {
  int64ZigZagDecode,
  int64ZigZagEncode,
  intZigZagDecode,
  intZigZagEncode,
  sizeOfUnsignedVarInt,
  sizeOfUnsignedVarInt64
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
