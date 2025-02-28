import BufferList from 'bl'

export const MOST_SIGNIFICANT_BIT_FLAG = 0x80n // 128 or 1000 0000
export const LEAST_SIGNIFICANT_7_BITS = 0x7fn // 127 or 0111 1111
// This is used in varlong to check if there are any other bits set after the first 7 bits,
// which means it still needs more than a byte to represent the bigint in varlong encoding
export const BITS_8PLUS_MASK = 0xffffffffn - 0x7fn

export function longZigZagEncode (value: bigint): bigint {
  return (value << 1n) ^ (value >> 31n)
}

export function longZigZagDecode (value: bigint): bigint {
  return (value >> 1n) ^ -(value & 1n)
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
  return [longZigZagDecode(value), read]
}

export function sizeOfUnsignedVarInt64 (value: bigint): number {
  let bytes = 1

  while ((value & BITS_8PLUS_MASK) !== 0n) {
    bytes++
    value >>= 7n
  }

  return bytes
}

export function writeUnsignedVarInt64 (buffer: Buffer, offset: number, value: bigint): number {
  let position = offset

  while ((value & BITS_8PLUS_MASK) !== 0n) {
    buffer.writeUInt8(Number((value & LEAST_SIGNIFICANT_7_BITS) | MOST_SIGNIFICANT_BIT_FLAG), position)
    position++
    value >>= 7n
  }

  buffer.writeUInt8(Number(value & LEAST_SIGNIFICANT_7_BITS), position)
  position++

  return position - offset
}

export function writeVarInt64 (buffer: Buffer, offset: number, value: bigint): number {
  return writeUnsignedVarInt64(buffer, offset, longZigZagEncode(value))
}

export function sizeOfVarInt64 (value: bigint): number {
  return sizeOfUnsignedVarInt64(longZigZagEncode(value))
}
