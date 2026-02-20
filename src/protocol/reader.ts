import { DynamicBuffer } from '@platformatic/dynamic-buffer'
import { EMPTY_BUFFER, INT16_SIZE, INT32_SIZE, INT64_SIZE, INT8_SIZE, UUID_SIZE } from './definitions.ts'
import { Writer } from './writer.ts'

export type EntryReader<OutputType> = (reader: Reader, index: number) => OutputType

const instanceIdentifier = Symbol('plt.kafka.reader.instanceIdentifier')

export class Reader {
  buffer: DynamicBuffer
  position: number
  length: number;
  [instanceIdentifier]: boolean

  static isReader (target: any): boolean {
    return target?.[instanceIdentifier] === true
  }

  static from (buffer: Buffer | DynamicBuffer | Writer): Reader {
    if (Writer.isWriter(buffer)) {
      return new Reader((buffer as Writer).dynamicBuffer)
    } else if (Buffer.isBuffer(buffer)) {
      buffer = new DynamicBuffer(buffer)
    }

    return new Reader(buffer as DynamicBuffer)
  }

  constructor (buffer: DynamicBuffer) {
    this.buffer = buffer
    this.position = 0
    this.length = this.buffer.length
    this[instanceIdentifier] = true
  }

  get remaining (): number {
    return this.length - this.position
  }

  reset (buffer?: Buffer | DynamicBuffer) {
    if (buffer) {
      if (Buffer.isBuffer(buffer)) {
        buffer = new DynamicBuffer(buffer)
      }

      this.buffer = buffer
    }

    this.position = 0
  }

  inspect (): string {
    return this.buffer
      .subarray(this.position)
      .toString('hex')
      .replaceAll(/(.{4})/g, '$1 ')
      .trim()
  }

  skip (length: number): this {
    this.position += length
    return this
  }

  peekUnsignedInt8 (position?: number): number {
    return this.buffer.readUInt8(position ?? this.position)
  }

  peekUnsignedInt16 (position?: number): number {
    return this.buffer.readUInt16BE(position ?? this.position)
  }

  peekUnsignedInt32 (position?: number): number {
    return this.buffer.readUInt32BE(position ?? this.position)
  }

  peekUnsignedInt64 (position?: number): bigint {
    return this.buffer.readBigUInt64BE(position ?? this.position)
  }

  peekUnsignedVarInt (position?: number): number {
    return this.buffer.readUnsignedVarInt(position ?? this.position)[0]
  }

  peekUnsignedVarInt64 (position?: number): bigint {
    return this.buffer.readUnsignedVarInt64(position ?? this.position)[0]
  }

  peekInt8 (position?: number): number {
    return this.buffer.readInt8(position ?? this.position)
  }

  peekInt16 (position?: number): number {
    return this.buffer.readInt16BE(position ?? this.position)
  }

  peekInt32 (position?: number): number {
    return this.buffer.readInt32BE(position ?? this.position)
  }

  peekInt64 (position?: number): bigint {
    return this.buffer.readBigInt64BE(position ?? this.position)
  }

  peekFloat64 (position?: number): number {
    return this.buffer.readDoubleBE(position ?? this.position)
  }

  peekVarInt (position?: number): number {
    return this.buffer.readVarInt(position ?? this.position)[0]
  }

  peekVarInt64 (position?: number): bigint {
    return this.buffer.readVarInt64(position ?? this.position)[0]
  }

  peekBoolean (position?: number): boolean {
    return this.buffer.readInt8(position ?? this.position) === 1
  }

  peekUUID (position?: number): string {
    position ??= this.position
    return this.buffer.toString('hex', position, position + UUID_SIZE)
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

  readUnsignedVarInt (): number {
    const [value, read] = this.buffer.readUnsignedVarInt(this.position)
    this.position += read

    return value
  }

  readUnsignedVarInt64 (): bigint {
    const [value, read] = this.buffer.readUnsignedVarInt64(this.position)
    this.position += read

    return value
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

  readFloat64 (): number {
    const value = this.peekFloat64()
    this.position += INT64_SIZE

    return value
  }

  readVarInt (): number {
    const [value, read] = this.buffer.readVarInt(this.position)
    this.position += read

    return value
  }

  readVarInt64 (): bigint {
    const [value, read] = this.buffer.readVarInt64(this.position)
    this.position += read

    return value
  }

  readBoolean (): boolean {
    const value = this.peekUnsignedInt8()
    this.position += INT8_SIZE

    return value === 1
  }

  readNullableString (compact: boolean = true, encoding: BufferEncoding = 'utf-8'): string | null {
    let length: number

    if (compact) {
      length = this.readUnsignedVarInt()

      if (length === 0) {
        return null
      }

      length--
    } else {
      length = this.readInt16()

      if (length === -1) {
        return null
      }
    }

    const value = this.buffer.toString(encoding, this.position, this.position + length)
    this.position += length

    return value
  }

  readString (compact: boolean = true, encoding: BufferEncoding = 'utf-8'): string {
    return this.readNullableString(compact, encoding) || ''
  }

  readUUID (): string {
    const value = this.peekUUID()
    this.position += UUID_SIZE

    return value.replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, '$1-$2-$3-$4-$5')
  }

  readNullableBytes (compact: boolean = true): Buffer | null {
    let length: number

    if (compact) {
      length = this.readUnsignedVarInt()

      if (length === 0) {
        return null
      }

      length--
    } else {
      length = this.readInt32()

      if (length === -1) {
        return null
      }
    }

    const value = this.buffer.slice(this.position, this.position + length)
    this.position += length

    return value
  }

  readBytes (compact: boolean = true): Buffer {
    return this.readNullableBytes(compact) || EMPTY_BUFFER
  }

  readVarIntBytes (): Buffer {
    let length = this.readVarInt()
    if (length === -1) {
      length = 0
    }
    const value = this.buffer.slice(this.position, this.position + length)

    this.position += length
    return value
  }

  readNullableArray<OutputType> (
    reader: EntryReader<OutputType>,
    compact: boolean = true,
    discardTrailingTaggedFields = true
  ): OutputType[] | null {
    let length: number

    if (compact) {
      length = this.readUnsignedVarInt()

      if (length === 0) {
        return null
      }

      length--
    } else {
      length = this.readInt32()

      if (length === -1) {
        return null
      }
    }

    const value: OutputType[] = []

    for (let i = 0; i < length; i++) {
      value.push(reader(this, i))

      if (discardTrailingTaggedFields) {
        this.readTaggedFields()
      }
    }

    return value
  }

  readNullableMap<Key, Value> (
    reader: EntryReader<[Key, Value]>,
    compact: boolean = true,
    discardTrailingTaggedFields = true
  ): Map<Key, Value> | null {
    let length: number

    if (compact) {
      length = this.readUnsignedVarInt()

      if (length === 0) {
        return null
      }

      length--
    } else {
      length = this.readInt32()

      if (length === -1) {
        return null
      }
    }

    const map: Map<Key, Value> = new Map()

    for (let i = 0; i < length; i++) {
      const [key, value] = reader(this, i)
      map.set(key, value)

      if (discardTrailingTaggedFields) {
        this.readTaggedFields()
      }
    }

    return map
  }

  readArray<OutputType> (
    reader: EntryReader<OutputType>,
    compact: boolean = true,
    discardTrailingTaggedFields = true
  ): OutputType[] {
    return this.readNullableArray(reader, compact, discardTrailingTaggedFields) || []
  }

  readMap<Key, Value> (
    reader: EntryReader<[Key, Value]>,
    compact: boolean = true,
    discardTrailingTaggedFields = true
  ): Map<Key, Value> {
    return this.readNullableMap(reader, compact, discardTrailingTaggedFields) ?? new Map()
  }

  readVarIntArray<OutputType> (reader: EntryReader<OutputType>): OutputType[] {
    const length = this.readVarInt()
    const value: OutputType[] = []

    for (let i = 0; i < length; i++) {
      value.push(reader(this, i))
    }

    return value
  }

  readVarIntMap<Key, Value> (reader: EntryReader<[Key, Value]>): Map<Key, Value> {
    const length = this.readVarInt()
    const map = new Map<Key, Value>()

    for (let i = 0; i < length; i++) {
      const [key, value] = reader(this, i)
      map.set(key, value)
    }

    return map
  }

  readNullableStruct<V> (reader: () => V): V | null {
    if (this.readInt8() === -1) {
      return null
    }
    return reader()
  }

  // TODO(ShogunPanda): Tagged fields are not supported yet
  readTaggedFields (): void {
    const length = this.readVarInt()
    if (length > 0) {
      this.skip(length)
    }
  }
}
