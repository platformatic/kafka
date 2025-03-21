import BufferList from 'bl'

const MOST_SIGNIFICANT_BIT_FLAG = 0x80 // 128 or 1000 0000
const LEAST_SIGNIFICANT_7_BITS = 0x7f // 127 or 0111 1111
// This is used in varint to check if there are any other bits set after the first 7 bits,
// which means it still needs more than a byte to represent the number in varint encoding
const BITS_8PLUS_MASK = 0xffffffff - 0x7f

export function intZigZagEncode (value: number): number {
  return (value << 1) ^ (value >> 31)
}

export function intZigZagDecode (value: number): number {
  return (value >> 1) ^ -(value & 1)
}

export function sizeOfUnsignedVarInt (value: number): number {
  let bytes = 1

  while ((value & BITS_8PLUS_MASK) !== 0) {
    bytes++
    value >>>= 7
  }

  return bytes
}

export function sizeOfVarInt (value: number): number {
  return sizeOfUnsignedVarInt(intZigZagEncode(value))
}

export function readUnsignedVarInt (buffer: BufferList, offset: number): [number, number] {
  let i = 0
  let byte: number
  let value = 0
  let position = offset

  do {
    byte = buffer.get(position++)
    value += (byte & LEAST_SIGNIFICANT_7_BITS) << i
    i += 7
  } while (byte >= MOST_SIGNIFICANT_BIT_FLAG)

  return [value, position - offset]
}

export function readVarInt (buffer: BufferList, offset: number): [number, number] {
  const [value, read] = readUnsignedVarInt(buffer, offset)
  return [intZigZagDecode(value), read]
}

export function writeUnsignedVarInt (value: number): Buffer {
  const buffer = Buffer.alloc(sizeOfUnsignedVarInt(value))
  let position = 0

  while ((value & BITS_8PLUS_MASK) !== 0) {
    buffer.writeUInt8((value & LEAST_SIGNIFICANT_7_BITS) | MOST_SIGNIFICANT_BIT_FLAG, position)
    position++
    value >>>= 7
  }

  buffer.writeUInt8(value & LEAST_SIGNIFICANT_7_BITS, position)

  return buffer
}

export function writeVarInt (value: number): Buffer {
  return writeUnsignedVarInt(intZigZagEncode(value))
}
