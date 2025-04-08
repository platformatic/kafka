import type BufferList from 'bl'

const MOST_SIGNIFICANT_BIT_FLAG = 0x80n // 128 or 1000 0000
const LEAST_SIGNIFICANT_7_BITS = 0x7fn // 127 or 0111 1111
// This is used in varlong to check if there are any other bits set after the first 7 bits,
// which means it still needs more than a byte to represent the bigint in varlong encoding
const BITS_8PLUS_MASK = 0xffffffffn - 0x7fn

export function int64ZigZagEncode (value: bigint): bigint {
  return (value << 1n) ^ (value >> 31n)
}

export function int64ZigZagDecode (value: bigint): bigint {
  return (value >> 1n) ^ -(value & 1n)
}

export function sizeOfUnsignedVarInt64 (value: bigint): number {
  let bytes = 1

  while ((value & BITS_8PLUS_MASK) !== 0n) {
    bytes++
    value >>= 7n
  }

  return bytes
}

export function sizeOfVarInt64 (value: bigint): number {
  return sizeOfUnsignedVarInt64(int64ZigZagEncode(value))
}

export function readUnsignedVarInt64 (buffer: BufferList, offset: number): [bigint, number] {
  let i = 0n
  let byte: bigint
  let value = 0n
  let position = offset

  do {
    byte = BigInt(buffer.get(position++))
    value += (byte & LEAST_SIGNIFICANT_7_BITS) << i
    i += 7n
  } while (byte >= MOST_SIGNIFICANT_BIT_FLAG)

  return [value, position - offset]
}

export function readVarInt64 (buffer: BufferList, offset: number): [bigint, number] {
  const [value, read] = readUnsignedVarInt64(buffer, offset)
  return [int64ZigZagDecode(value), read]
}

export function writeUnsignedVarInt64 (value: bigint): Buffer {
  const buffer = Buffer.alloc(sizeOfUnsignedVarInt64(value))
  let position = 0

  while ((value & BITS_8PLUS_MASK) !== 0n) {
    buffer.writeUInt8(Number((value & LEAST_SIGNIFICANT_7_BITS) | MOST_SIGNIFICANT_BIT_FLAG), position)
    position++
    value >>= 7n
  }

  buffer.writeUInt8(Number(value & LEAST_SIGNIFICANT_7_BITS), position)

  return buffer
}

export function writeVarInt64 (value: bigint): Buffer {
  return writeUnsignedVarInt64(int64ZigZagEncode(value))
}
