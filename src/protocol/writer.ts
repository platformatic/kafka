import BufferList from 'bl'
import { type BufferListAcceptedTypes } from 'bl/BufferList.js'
import { humanize } from '../utils.ts'
import { EMPTY_UUID, INT16_SIZE, INT32_SIZE, INT64_SIZE, INT8_SIZE, type NullableString } from './definitions.ts'
import { writeUnsignedVarInt, writeVarInt } from './varint32.ts'
import { writeUnsignedVarInt64, writeVarInt64 } from './varint64.ts'

// Note that in this class "== null" is purposely used instead of "===" to check for both null and undefined

export type ChildrenWriter = (w: Writer) => void
export type EntryWriter<InputType> = (writer: Writer, entry: InputType, index: number) => void

export class Writer {
  context: Record<string, any>
  #bl: BufferList

  static create (): Writer {
    return new Writer(new BufferList())
  }

  constructor (bl?: BufferList) {
    this.#bl = bl!
    this.context = {}
  }

  get bufferList (): BufferList {
    return this.#bl
  }

  get buffer (): Buffer {
    return this.#bl.slice()
  }

  get buffers (): Buffer[] {
    return this.#bl.getBuffers()
  }

  get length (): number {
    return this.#bl.length
  }

  inspect (): string {
    return this.buffers.map((buffer, i) => humanize(`Buffer ${i}`, buffer)).join('\n')
  }

  append (buffer: BufferListAcceptedTypes): this {
    this.#bl.append(buffer)

    return this
  }

  prepend (buffer: BufferListAcceptedTypes): this {
    this.#bl.prepend(buffer)

    return this
  }

  appendUnsignedInt8 (value: number, append: boolean = true): this {
    const buffer = Buffer.allocUnsafe(INT8_SIZE)
    buffer.writeUInt8(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendUnsignedInt16 (value: number, append: boolean = true): this {
    const buffer = Buffer.allocUnsafe(INT16_SIZE)
    buffer.writeUInt16BE(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendUnsignedInt32 (value: number, append: boolean = true): this {
    const buffer = Buffer.allocUnsafe(INT32_SIZE)
    buffer.writeUInt32BE(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendUnsignedInt64 (value: bigint, append: boolean = true): this {
    const buffer = Buffer.allocUnsafe(INT64_SIZE)
    buffer.writeBigUInt64BE(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendUnsignedVarInt (value: number, append: boolean = true): this {
    const buffer = writeUnsignedVarInt(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendUnsignedVarInt64 (value: bigint, append: boolean = true): this {
    const buffer = writeUnsignedVarInt64(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendInt8 (value: number, append: boolean = true): this {
    const buffer = Buffer.allocUnsafe(INT8_SIZE)
    buffer.writeInt8(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendInt16 (value: number, append: boolean = true): this {
    const buffer = Buffer.allocUnsafe(INT16_SIZE)
    buffer.writeInt16BE(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendInt32 (value: number, append: boolean = true): this {
    const buffer = Buffer.allocUnsafe(INT32_SIZE)
    buffer.writeInt32BE(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendInt64 (value: bigint, append: boolean = true): this {
    const buffer = Buffer.allocUnsafe(INT64_SIZE)
    buffer.writeBigInt64BE(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendFloat64 (value: number, append: boolean = true): this {
    const buffer = Buffer.allocUnsafe(INT64_SIZE)
    buffer.writeDoubleBE(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendVarInt (value: number, append: boolean = true): this {
    const buffer = writeVarInt(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendVarInt64 (value: bigint, append: boolean = true): this {
    const buffer = writeVarInt64(value)

    if (append) {
      this.#bl.append(buffer)
    } else {
      this.#bl.prepend(buffer)
    }

    return this
  }

  appendBoolean (value: boolean): this {
    return this.appendUnsignedInt8(value ? 1 : 0)
  }

  appendString (value: NullableString, compact: boolean = true, encoding: BufferEncoding = 'utf-8'): this {
    if (value == null) {
      return compact ? this.appendUnsignedVarInt(0) : this.appendInt16(-1)
    }

    const buffer = Buffer.from(value, encoding)

    if (compact) {
      this.appendUnsignedVarInt(buffer.length + 1)
    } else {
      this.appendInt16(buffer.length)
    }

    if (buffer.length) {
      this.#bl.append(buffer)
    }

    return this
  }

  appendUUID (value: NullableString): this {
    if (value == null) {
      return this.append(EMPTY_UUID)
    }

    const buffer = Buffer.from(value.replaceAll('-', ''), 'hex')
    this.#bl.append(buffer)

    return this
  }

  appendBytes (value: Buffer | undefined | null, compact: boolean = true): this {
    if (value == null) {
      return compact ? this.appendUnsignedVarInt(0) : this.appendInt32(-1)
    }

    if (compact) {
      this.appendUnsignedVarInt(value.length + 1)
    } else {
      this.appendInt32(value.length)
    }

    this.#bl.append(value)

    return this
  }

  // Note that this does not follow the wire protocol specification and thus the length is not +1ed
  appendVarIntBytes (value: Buffer | null | undefined): this {
    if (value == null) {
      return this.appendVarInt(0)
    }

    this.appendVarInt(value.length)
    this.#bl.append(value)

    return this
  }

  appendArray<InputType>(
    value: InputType[] | null | undefined,
    entryWriter: EntryWriter<InputType>,
    compact: boolean = true,
    appendTrailingTaggedFields = true
  ): this {
    if (value == null) {
      return compact ? this.appendUnsignedVarInt(0) : this.appendInt32(0)
    }

    const length = value.length

    if (compact) {
      this.appendUnsignedVarInt(length + 1)
    } else {
      this.appendInt32(length)
    }

    for (let i = 0; i < length; i++) {
      entryWriter(this, value![i], i)

      if (appendTrailingTaggedFields) {
        this.appendTaggedFields()
      }
    }

    return this
  }

  appendMap<Key, Value>(
    value: Map<Key, Value> | null | undefined,
    entryWriter: EntryWriter<[Key, Value]>,
    compact: boolean = true,
    appendTrailingTaggedFields = true
  ): this {
    if (value == null) {
      return compact ? this.appendUnsignedVarInt(0) : this.appendInt32(0)
    }

    const length = value.size

    if (compact) {
      this.appendUnsignedVarInt(length + 1)
    } else {
      this.appendInt32(length)
    }

    let i = 0
    for (const entry of value) {
      entryWriter(this, entry, i++)

      if (appendTrailingTaggedFields) {
        this.appendTaggedFields()
      }
    }

    return this
  }

  appendVarIntArray<InputType>(value: InputType[] | null | undefined, entryWriter: EntryWriter<InputType>): this {
    if (value == null) {
      return this.appendVarInt(0)
    }

    this.appendVarInt(value.length)

    for (let i = 0; i < value.length; i++) {
      entryWriter(this, value![i], i)
    }

    return this
  }

  appendVarIntMap<Key, Value>(value: Map<Key, Value> | null | undefined, entryWriter: EntryWriter<[Key, Value]>): this {
    if (value == null) {
      return this.appendVarInt(0)
    }

    this.appendVarInt(value.size)

    let i = 0
    for (const entry of value) {
      entryWriter(this, entry, i++)
    }

    return this
  }

  // TODO(ShogunPanda): Tagged fields are not supported yet
  appendTaggedFields (_: any[] = []): this {
    return this.appendInt8(0)
  }

  prependLength (): this {
    return this.appendInt32(this.length, false)
  }

  prependVarIntLength (): this {
    return this.appendVarInt(this.length, false)
  }
}
