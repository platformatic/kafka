export const MOST_SIGNIFICANT_BIT_FLAG = 0x80 // 128 or 1000 0000
export const MOST_SIGNIFICANT_BIT_FLAG_64 = 0x80n // 128 or 1000 0000
export const LEAST_SIGNIFICANT_7_BITS = 0x7f // 127 or 0111 1111
export const LEAST_SIGNIFICANT_7_BITS_64 = 0x7fn // 127 or 0111 1111

// This is used in varint to check if there are any other bits set after the first 7 bits,
// which means it still needs more than a byte to represent the number in varint encoding
export const BITS_8PLUS_MASK = 0xffffffff - 0x7f
export const BITS_8PLUS_MASK_64 = 0xffffffffn - 0x7fn

export function intZigZagEncode (value: number): number {
  return (value << 1) ^ (value >> 31)
}

export function intZigZagDecode (value: number): number {
  return (value >> 1) ^ -(value & 1)
}

export function int64ZigZagEncode (value: bigint): bigint {
  return (value << 1n) ^ (value >> 31n)
}

export function int64ZigZagDecode (value: bigint): bigint {
  return (value >> 1n) ^ -(value & 1n)
}

export function sizeOfUnsignedVarInt (value: number): number {
  let bytes = 1

  while ((value & BITS_8PLUS_MASK) !== 0) {
    bytes++
    value >>>= 7
  }

  return bytes
}

export function sizeOfUnsignedVarInt64 (value: bigint): number {
  let bytes = 1

  while ((value & BITS_8PLUS_MASK_64) !== 0n) {
    bytes++
    value >>= 7n
  }

  return bytes
}
