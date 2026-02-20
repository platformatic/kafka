import { DynamicBuffer } from '@platformatic/dynamic-buffer'
import { humanize } from '../utils.ts'
import { EMPTY_TAGGED_FIELDS_BUFFER, EMPTY_UUID, type NullableString } from './definitions.ts'

// Note that in this class "== null" is purposely used instead of "===" to check for both null and undefined

export type ChildrenWriter = (w: Writer) => void
export type EntryWriter<InputType> = (writer: Writer, entry: InputType, index: number) => void

const instanceIdentifier = Symbol('plt.kafka.writer.instanceIdentifier')

export class Writer {
  context: Record<string, any>
  #buffer: DynamicBuffer;
  [instanceIdentifier]: boolean

  static isWriter (target: any): boolean {
    return target?.[instanceIdentifier] === true
  }

  static create (): Writer {
    return new Writer(new DynamicBuffer())
  }

  constructor (bl?: DynamicBuffer) {
    this.#buffer = bl!
    this.context = {}
    this[instanceIdentifier] = true
  }

  get buffer (): Buffer {
    return this.#buffer.buffer
  }

  get buffers (): Buffer[] {
    return this.#buffer.buffers
  }

  get dynamicBuffer (): DynamicBuffer {
    return this.#buffer
  }

  get length (): number {
    return this.#buffer.length
  }

  inspect (): string {
    return this.buffers.map((buffer, i) => humanize(`Buffer ${i}`, buffer)).join('\n')
  }

  append (buffer: Buffer): this {
    this.#buffer.append(buffer)

    return this
  }

  prepend (buffer: Buffer): this {
    this.#buffer.prepend(buffer)

    return this
  }

  appendFrom (buffer: Writer | DynamicBuffer): this {
    this.#buffer.appendFrom((buffer as Writer)?.dynamicBuffer ?? buffer)

    return this
  }

  prependFrom (buffer: Writer | DynamicBuffer): this {
    this.#buffer.prependFrom((buffer as Writer)?.dynamicBuffer ?? buffer)

    return this
  }

  appendUnsignedInt8 (value: number, append: boolean = true): this {
    this.#buffer.writeUInt8(value, append)
    return this
  }

  appendUnsignedInt16 (value: number, append: boolean = true): this {
    this.#buffer.writeUInt16BE(value, append)
    return this
  }

  appendUnsignedInt32 (value: number, append: boolean = true): this {
    this.#buffer.writeUInt32BE(value, append)
    return this
  }

  appendUnsignedInt64 (value: bigint, append: boolean = true): this {
    this.#buffer.writeBigUInt64BE(value, append)
    return this
  }

  appendUnsignedVarInt (value: number, append: boolean = true): this {
    this.#buffer.writeUnsignedVarInt(value, append)
    return this
  }

  appendUnsignedVarInt64 (value: bigint, append: boolean = true): this {
    this.#buffer.writeUnsignedVarInt64(value, append)
    return this
  }

  appendInt8 (value: number, append: boolean = true): this {
    this.#buffer.writeInt8(value, append)
    return this
  }

  appendInt16 (value: number, append: boolean = true): this {
    this.#buffer.writeInt16BE(value, append)
    return this
  }

  appendInt32 (value: number, append: boolean = true): this {
    this.#buffer.writeInt32BE(value, append)
    return this
  }

  appendInt64 (value: bigint, append: boolean = true): this {
    this.#buffer.writeBigInt64BE(value, append)
    return this
  }

  // In Kafka float is actually a double
  appendFloat64 (value: number, append: boolean = true): this {
    this.#buffer.writeDoubleBE(value, append)
    return this
  }

  appendVarInt (value: number, append: boolean = true): this {
    this.#buffer.writeVarInt(value, append)
    return this
  }

  appendVarInt64 (value: bigint, append: boolean = true): this {
    this.#buffer.writeVarInt64(value, append)
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
      this.#buffer.append(buffer)
    }

    return this
  }

  appendUUID (value: NullableString): this {
    if (value == null) {
      return this.append(EMPTY_UUID)
    }

    const buffer = Buffer.from(value.replaceAll('-', ''), 'hex')
    this.#buffer.append(buffer)

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

    this.#buffer.append(value)

    return this
  }

  // Note that this does not follow the wire protocol specification and thus the length is not +1ed
  appendVarIntBytes (value: Buffer | null | undefined): this {
    if (value == null) {
      return this.appendVarInt(-1)
    }

    this.appendVarInt(value.length)
    this.#buffer.append(value)

    return this
  }

  appendArray<InputType> (
    value: InputType[] | null | undefined,
    entryWriter: EntryWriter<InputType>,
    compact: boolean = true,
    appendTrailingTaggedFields = true
  ): this {
    if (value == null) {
      return compact ? this.appendUnsignedVarInt(0) : this.appendInt32(-1)
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

  appendMap<Key, Value> (
    value: Map<Key, Value> | null | undefined,
    entryWriter: EntryWriter<[Key, Value]>,
    compact: boolean = true,
    appendTrailingTaggedFields = true
  ): this {
    if (value == null) {
      return compact ? this.appendUnsignedVarInt(0) : this.appendInt32(-1)
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

  appendVarIntArray<InputType> (value: InputType[] | null | undefined, entryWriter: EntryWriter<InputType>): this {
    if (value == null) {
      return this.appendVarInt(0)
    }

    this.appendVarInt(value.length)

    for (let i = 0; i < value.length; i++) {
      entryWriter(this, value![i], i)
    }

    return this
  }

  appendVarIntMap<Key, Value> (
    value: Map<Key, Value> | null | undefined,
    entryWriter: EntryWriter<[Key, Value]>
  ): this {
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

  // TODO: Tagged fields are not supported yet
  appendTaggedFields (_: any[] = []): this {
    return this.append(EMPTY_TAGGED_FIELDS_BUFFER)
  }

  prependLength (): this {
    return this.appendInt32(this.length, false)
  }

  prependVarIntLength (): this {
    return this.appendVarInt(this.length, false)
  }
}
