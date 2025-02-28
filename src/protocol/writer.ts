import BufferList from 'bl'
import { ERROR_UNFINISHED_WRITE_BUFFER, KafkaError } from '../error.ts'
import { UUID_SIZE, type Collection } from './definitions.ts'
import { writeUnsignedVarInt, writeVarInt } from './varint32.ts'
import { writeUnsignedVarInt64, writeVarInt64 } from './varint64.ts'

// Note that in this class "== null" is purposely used instead of "===" to check for both null and undefined

export class Writer {
  buffer: Buffer
  position: number
  #markers: Map<string, number>

  static create (size: number): Writer {
    return new Writer(Buffer.allocUnsafe(size))
  }

  constructor (buffer?: Buffer) {
    this.buffer = buffer!
    this.position = 0
    this.#markers = new Map()
  }

  asBufferList (finalize: boolean = true): BufferList {
    return new BufferList(finalize ? this.finalize() : this.buffer)
  }

  appendTo (bl: BufferList, finalize: boolean = true): this {
    bl.append(finalize ? this.finalize() : this.buffer)
    return this
  }

  prependTo (bl: BufferList, finalize: boolean = true): this {
    if (finalize) {
      this.finalize()
    }

    // @ts-expect-error _bufs is not exposed
    bl._bufs.unshift(this.buffer)
    bl.length += this.buffer.length

    return this
  }

  finalize (): Buffer {
    if (this.position !== this.buffer.length) {
      throw new KafkaError(`Buffer was written up to position ${this.position} of ${this.buffer.length}.`, {
        code: ERROR_UNFINISHED_WRITE_BUFFER,
        buffer: this.buffer
      })
    }

    return this.buffer
  }

  reset (size: number): this {
    this.buffer = Buffer.allocUnsafe(size)
    this.position = 0

    return this
  }

  getMarker (marker: string): number {
    const position = this.#markers.get(marker)

    if (typeof position === 'undefined') {
      throw new Error(`Marker ${marker} not found`)
    }

    return position
  }

  setMarker (marker: string, length: number = 0): this {
    this.#markers.set(marker, this.position)

    if (length > 0) {
      this.position += length
    }

    return this
  }

  useMarker (marker: string, fn: (w: Writer) => void): this {
    const markerPosition = this.getMarker(marker)
    const originalPosition = this.position

    this.position = markerPosition
    fn(this)
    this.setMarker(marker)
    this.position = originalPosition

    return this
  }

  // Since we mostly use allocUnsafe, we need to fill the buffers with zeros
  skip (length: number): this {
    this.buffer.fill(0, this.position, this.position + length)
    this.position += length

    return this
  }

  writeInt8 (value: number): this {
    this.position = this.buffer.writeInt8(value, this.position)

    return this
  }

  writeInt16 (value: number): this {
    this.position = this.buffer.writeInt16BE(value, this.position)

    return this
  }

  writeInt32 (value: number): this {
    this.position = this.buffer.writeInt32BE(value, this.position)

    return this
  }

  writeInt64 (value: bigint): this {
    this.position = this.buffer.writeBigInt64BE(value, this.position)

    return this
  }

  writeUnsignedInt8 (value: number): this {
    this.position = this.buffer.writeUInt8(value, this.position)

    return this
  }

  writeUnsignedInt16 (value: number): this {
    this.position = this.buffer.writeUInt16BE(value, this.position)

    return this
  }

  writeUnsignedInt32 (value: number): this {
    this.position = this.buffer.writeUInt32BE(value, this.position)

    return this
  }

  writeUnsignedInt64 (value: bigint): this {
    this.position = this.buffer.writeBigUInt64BE(value, this.position)

    return this
  }

  writeUnsignedVarInt (value: number): this {
    this.position += writeUnsignedVarInt(this.buffer, this.position, value)

    return this
  }

  writeVarInt (value: number): this {
    this.position += writeVarInt(this.buffer, this.position, value)

    return this
  }

  writeUnsignedVarInt64 (value: bigint): this {
    this.position += writeUnsignedVarInt64(this.buffer, this.position, value)

    return this
  }

  writeVarInt64 (value: bigint): this {
    this.position += writeVarInt64(this.buffer, this.position, value)

    return this
  }

  writeBoolean (value: boolean): this {
    return this.writeUnsignedInt8(value ? 1 : 0)
  }

  writeString (value: string | undefined | null): this {
    if (value == null) {
      return this.writeInt16(-1)
    }

    if (value.length > 0) {
      this.writeInt16(value.length)
      this.position += this.buffer.write(value, this.position, 'utf-8')
    }

    return this
  }

  writeBytes (value: Buffer | BufferList | undefined | null): this {
    if (!value) {
      return this.writeInt32(-1)
    }

    if (value.length > 0) {
      this.writeInt32(value.length)
      value.copy(this.buffer, this.position)
      this.position += value.length
    }

    return this
  }

  writeUUID (value: string | undefined | null): this {
    if (value == null) {
      this.buffer.fill(0, this.position, this.position + UUID_SIZE)
    } else {
      this.buffer.write(value.replaceAll('-', ''), this.position, 'hex')
    }

    this.position += UUID_SIZE

    return this
  }

  writeCompactable (value: string | Buffer | BufferList | undefined | null): this {
    if (value == null) {
      return this.writeVarInt(0)
    }

    this.writeUnsignedVarInt(value.length + 1)

    if (value.length > 0) {
      if (typeof value === 'string') {
        this.position += this.buffer.write(value, this.position, 'utf-8')
      } else {
        value.copy(this.buffer, this.position)
        this.position += value.length
      }
    }

    return this
  }

  writeCompactableLength (value: Collection | number | undefined | null): this {
    if (value == null) {
      return this.writeVarInt(0)
    } else if (typeof value !== 'number') {
      value = (value as string).length ?? (value as Set<unknown>).size
    }

    return this.writeUnsignedVarInt(value + 1)
  }

  // Note that this does not follow the wire protocol specification and thus the length is not +1ed
  writeVarIntBytes (value: Buffer | BufferList | string | undefined | null): this {
    if (value == null) {
      return this.writeVarInt(0)
    }

    this.writeVarInt(value.length)

    if (value.length > 0) {
      if (typeof value === 'string') {
        this.position += this.buffer.write(value, this.position, 'utf-8')
      } else {
        value.copy(this.buffer, this.position)
        this.position += value.length
      }
    }

    return this
  }

  // TODO(ShogunPanda): Tagged fields are not supported yet
  writeTaggedFields (_: any[] = []): this {
    return this.writeInt8(0)
  }
}
