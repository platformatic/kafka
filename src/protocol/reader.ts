import BufferList from 'bl'
import { ERROR_UNSUPPORTED, KafkaError } from '../error.ts'
import { INT16_SIZE, INT32_SIZE, INT64_SIZE, INT8_SIZE, UUID_SIZE } from './definitions.ts'
import { readUnsignedVarInt, readVarInt } from './varint32.ts'
import { readUnsignedVarInt64, readVarInt64 } from './varint64.ts'

export class Reader {
  buffer: BufferList
  position: number

  static from (buffer: BufferList): Reader {
    return new Reader(buffer)
  }

  constructor (buffer: BufferList) {
    this.buffer = buffer
    this.position = 0
  }

  inspect (): string {
    return this.buffer
      .shallowSlice(this.position)
      .toString('hex')
      .replaceAll(/(.{4})/g, '$1 ')
      .trim()
  }

  skip (length: number): this {
    this.position += length
    return this
  }

  peekInt8 (): number {
    return this.buffer.readInt8(this.position)
  }

  peekInt16 (): number {
    return this.buffer.readInt16BE(this.position)
  }

  peekInt32 (): number {
    return this.buffer.readInt32BE(this.position)
  }

  peekInt64 (): bigint {
    return this.buffer.readBigInt64BE(this.position)
  }

  peekUnsignedInt8 (): number {
    return this.buffer.readUInt8(this.position)
  }

  peekUnsignedInt16 (): number {
    return this.buffer.readUInt16BE(this.position)
  }

  peekUnsignedInt32 (): number {
    return this.buffer.readUInt32BE(this.position)
  }

  peekUnsignedInt64 (): bigint {
    return this.buffer.readBigUInt64BE(this.position)
  }

  peekBoolean (): boolean {
    return this.buffer.readInt8(this.position) === 1
  }

  peekVarInt (): number {
    return readVarInt(this.buffer, this.position)[0]
  }

  peekUnsignedVarInt (): number {
    return readUnsignedVarInt(this.buffer, this.position)[0]
  }

  peekVarInt64 (): bigint {
    return readVarInt64(this.buffer, this.position)[0]
  }

  peekUnsignedVarInt64 (): bigint {
    return readUnsignedVarInt64(this.buffer, this.position)[0]
  }

  peekUUID (): string {
    return this.buffer.toString('hex', this.position, this.position + UUID_SIZE)
  }

  readInt8 (): number {
    const value = this.peekInt8()
    this.position += INT8_SIZE

    return value
  }

  readInt16 (): number {
    const value = this.peekInt16()
    this.position += INT16_SIZE

    return value
  }

  readInt32 (): number {
    const value = this.peekInt32()
    this.position += INT32_SIZE

    return value
  }

  readInt64 (): bigint {
    const value = this.peekInt64()
    this.position += INT64_SIZE

    return value
  }

  readUnsignedInt8 (): number {
    const value = this.peekUnsignedInt8()
    this.position += INT8_SIZE

    return value
  }

  readUnsignedInt16 (): number {
    const value = this.peekUnsignedInt16()
    this.position += INT16_SIZE

    return value
  }

  readUnsignedInt32 (): number {
    const value = this.peekUnsignedInt32()
    this.position += INT32_SIZE

    return value
  }

  readUnsignedInt64 (): bigint {
    const value = this.peekUnsignedInt64()
    this.position += INT64_SIZE

    return value
  }

  readBoolean (): boolean {
    const value = this.peekUnsignedInt8()
    this.position += INT8_SIZE

    return value === 1
  }

  readVarInt (): number {
    const [value, read] = readVarInt(this.buffer, this.position)
    this.position += read

    return value
  }

  readUnsignedVarInt (): number {
    const [value, read] = readUnsignedVarInt(this.buffer, this.position)
    this.position += read

    return value
  }

  readVarInt64 (): bigint {
    const [value, read] = readVarInt64(this.buffer, this.position)
    this.position += read

    return value
  }

  readUnsignedVarInt64 (): bigint {
    const [value, read] = readUnsignedVarInt64(this.buffer, this.position)
    this.position += read

    return value
  }

  readUUID (): string {
    const value = this.peekUUID()
    this.position += UUID_SIZE

    return value.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5')
  }

  readCollectionSize (): number {
    const value = this.readUnsignedVarInt()

    return value === 0 ? 0 : value - 1
  }

  readNullableCollectionSize (): number | null {
    const value = this.readUnsignedVarInt()

    return value === 0 ? null : value - 1
  }

  readCompactableString (): string | null {
    const length = this.readUnsignedVarInt()

    if (length === 0) {
      return null
    }

    const str = this.buffer.toString('utf-8', this.position, this.position + length - 1)
    this.position += length - 1
    return str
  }

  readVarIntBytes (): Buffer {
    const length = this.readVarInt()
    const value = this.buffer.slice(this.position, this.position + length)

    this.position += length
    return value
  }

  // TODO(ShogunPanda): Tagged fields are not supported yet
  readTaggedFields (): void {
    if (this.readVarInt() !== 0) {
      throw new KafkaError('Tagged fields are not supported yet', { code: ERROR_UNSUPPORTED })
    }
  }
}
