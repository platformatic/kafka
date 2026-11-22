import { int64ZigZagDecode, intZigZagDecode, OutOfBoundsError } from '@platformatic/dynamic-buffer'
import { UnsupportedCompressionError } from '../errors.ts'
import type { NumericMap } from '../utils.ts'
import {
  type CompressionAlgorithmSpecification,
  type CompressionAlgorithmValue,
  compressionsAlgorithms,
  compressionsAlgorithmsByBitmask
} from './compression.ts'
import { crc32c } from './crc32c.ts'
import { INT32_SIZE, INT64_SIZE, type NullableString } from './definitions.ts'
import { type Reader } from './reader.ts'
import { Writer } from './writer.ts'

export const CURRENT_RECORD_VERSION = 2
export const IS_COMPRESSED = 0b111 // Bits 0, 1 and/or 2 set
// Byte 3 is timestamp type, currently unused by this package
export const IS_TRANSACTIONAL = 1 << 4 // Bit 4 set
export const IS_CONTROL = 1 << 5 // Bits 5 set
export const BATCH_HEAD = INT64_SIZE + INT32_SIZE // FirstOffset + Length

export interface MessageBase<Key = Buffer, Value = Buffer> {
  key?: Key
  value?: Value
  topic: string
  partition?: number
  timestamp?: bigint
}

// This is used in produce
export interface MessageToProduce<
  Key = Buffer,
  Value = Buffer,
  HeaderKey = Buffer,
  HeaderValue = Buffer
> extends MessageBase<Key, Value> {
  headers?: Map<HeaderKey, HeaderValue> | Record<string, HeaderValue>
  metadata?: unknown // This is used by schema registry
}

// This is used by producer for consume-transform-produce flows
export interface MessageConsumerMetadata {
  coordinatorId: number
  groupId: string
  generationId: number
  memberId: string
}

export interface MessageJSON<Key = unknown, Value = unknown, HeaderKey = unknown, HeaderValue = unknown> {
  key: Key
  value: Value
  headers: Array<[HeaderKey, HeaderValue]> // We can't use Map or objects here since HeaderKey might not be a string
  topic: string
  partition: number
  timestamp: string
  offset: string
  leaderEpoch: number
  metadata: Record<string, unknown>
}

// This is used in consume
export interface Message<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> extends Required<
  MessageBase<Key, Value>
> {
  headers: Map<HeaderKey, HeaderValue>
  headerEntries: Array<[HeaderKey, HeaderValue]>
  offset: bigint
  leaderEpoch: number
  metadata: Record<string, unknown>
  commit (callback?: (error?: Error) => void): void | Promise<void>
  toJSON (): MessageJSON<Key, Value, HeaderKey, HeaderValue>
}

export interface MessageRecord {
  key?: Buffer
  value: Buffer
  headers?: Map<Buffer, Buffer>
  topic: string
  partition?: number
  timestamp?: bigint
}

export interface CreateRecordsBatchOptions {
  transactionalId?: NullableString
  compression: CompressionAlgorithmValue

  // Idempotency support
  firstSequence?: number
  producerId: bigint
  producerEpoch: number
  sequences: NumericMap

  // Unused at the moment
  partitionLeaderEpoch: number
}

/*
  Record (v0) =>
    Length => VARINT
    Attributes => INT8
    TimestampDelta => VARLONG
    OffsetDelta => VARINT
    Key => COMPACT_BYTES
    Value => COMPACT_BYTES
    Headers => [HeaderKey HeaderValue]
      HeaderKey => COMPACT_BYTES
      HeaderValue => COMPACT_BYTES

  Note: COMPACT_BYTES here do not follow the standard procedure where the length is increased by one
*/
export interface KafkaRecord {
  length: number
  attributes: number
  timestampDelta: bigint
  offsetDelta: number
  key: Buffer | null
  value: Buffer | null
  headers: [Buffer | null, Buffer | null][]
}

export interface MessageToConsume extends KafkaRecord {
  topic: string
  partition: number
  /*
    Deserializers and before-deserialization hooks can populate this to enrich the metadata of the
    message which is eventually pushed to the consumer stream.
  */
  metadata?: Record<string, unknown>
}

/*
  v0 - MessageFormat v2
    RecordBatch =>
      FirstOffset => INT64
      Length => INT32
      PartitionLeaderEpoch => INT32
      Magic => INT8
      CRC => UINT32
      Attributes => INT16
      LastOffsetDelta => INT32
      FirstTimestamp => INT64
      MaxTimestamp => INT64
      ProducerId => INT64
      ProducerEpoch => INT16
      FirstSequence => INT32
      Records => [Record]
*/
export interface RecordsBatch {
  firstOffset: bigint
  length: number
  partitionLeaderEpoch: number
  magic: number
  crc: number
  attributes: number
  lastOffsetDelta: number
  firstTimestamp: bigint
  maxTimestamp: bigint
  producerId: bigint
  producerEpoch: number
  firstSequence: number
  records: KafkaRecord[]
}

export const messageSchema = {
  type: 'object',
  properties: {
    key: true,
    value: true,
    headers: {
      // Note: we can't use oneOf here since a Map is also a 'object'. Thanks JS.
      anyOf: [
        {
          map: true
        },
        {
          type: 'object',
          additionalProperties: true
        }
      ]
    },
    topic: { type: 'string' },
    partition: { type: 'integer' },
    timestamp: { bigint: true }
  },
  required: ['value', 'topic'],
  additionalProperties: true
}

export function createRecord (message: MessageRecord, offsetDelta: number, firstTimestamp: bigint): Writer {
  return Writer.create()
    .appendInt8(0) // Attributes are unused for now
    .appendVarInt64((message.timestamp ?? BigInt(Date.now())) - firstTimestamp)
    .appendVarInt(offsetDelta)
    .appendVarIntBytes(message.key)
    .appendVarIntBytes(message.value)
    .appendVarIntMap(message.headers, (w, [key, value]) => {
      w.appendVarIntBytes(key).appendVarIntBytes(value)
    })
    .prependVarIntLength()
}

export function readRecord (reader: Reader): KafkaRecord {
  return {
    length: reader.readVarInt(),
    attributes: reader.readInt8(),
    timestampDelta: reader.readVarInt64(),
    offsetDelta: reader.readVarInt(),
    key: reader.readVarIntBytes(),
    value: reader.readVarIntBytes(),
    headers: reader.readVarIntArray(r => [r.readVarIntBytes(), r.readVarIntBytes()])
  }
}

/*
  Records are decoded from a contiguous Buffer with the helpers below rather than through a Reader.

  A fetch response can contain hundreds of thousands of records and each of them needs at least six varints.
  Going through the Reader costs a chunk lookup and a tuple allocation for each of those, while here the
  position is tracked in a module variable and nothing is allocated but the record itself.

  The helpers are synchronous and never re-entered, so sharing the position is safe.
*/
let recordsPosition = 0

function readRecordVarInt (buffer: Buffer): number {
  let value = 0
  let shift = 0
  let byte: number

  do {
    if (recordsPosition >= buffer.length) {
      throw new OutOfBoundsError('Out of bounds.')
    }

    byte = buffer[recordsPosition++]
    value += (byte & 0x7f) << shift
    shift += 7
  } while (byte >= 0x80)

  return intZigZagDecode(value)
}

function readRecordVarInt64 (buffer: Buffer): bigint {
  const start = recordsPosition
  let value = 0
  let shift = 0
  let byte: number

  // Most values, like timestamp deltas, fit in 28 bits and can be decoded without using BigInt
  do {
    if (recordsPosition >= buffer.length) {
      throw new OutOfBoundsError('Out of bounds.')
    }

    if (shift === 28) {
      recordsPosition = start
      return readRecordLongVarInt64(buffer)
    }

    byte = buffer[recordsPosition++]
    value |= (byte & 0x7f) << shift
    shift += 7
  } while (byte >= 0x80)

  return BigInt(intZigZagDecode(value))
}

function readRecordLongVarInt64 (buffer: Buffer): bigint {
  let value = 0n
  let shift = 0n
  let byte: number

  do {
    if (recordsPosition >= buffer.length) {
      throw new OutOfBoundsError('Out of bounds.')
    }

    byte = buffer[recordsPosition++]
    value += BigInt(byte & 0x7f) << shift
    shift += 7n
  } while (byte >= 0x80)

  return int64ZigZagDecode(value)
}

function readRecordVarIntBytes (buffer: Buffer): Buffer | null {
  const length = readRecordVarInt(buffer)

  if (length === -1) {
    return null
  }

  const end = recordsPosition + length

  if (length < 0 || end > buffer.length) {
    throw new OutOfBoundsError('Out of bounds.')
  }

  const value = buffer.subarray(recordsPosition, end)
  recordsPosition = end

  return value
}

// This must decode exactly what readRecord decodes
function readRecords (buffer: Buffer, count: number, records: KafkaRecord[]): void {
  recordsPosition = 0

  for (let i = 0; i < count; i++) {
    const length = readRecordVarInt(buffer)

    if (recordsPosition >= buffer.length) {
      throw new OutOfBoundsError('Out of bounds.')
    }

    const attributes = buffer.readInt8(recordsPosition++)
    const timestampDelta = readRecordVarInt64(buffer)
    const offsetDelta = readRecordVarInt(buffer)
    const key = readRecordVarIntBytes(buffer)
    const value = readRecordVarIntBytes(buffer)

    const headersCount = readRecordVarInt(buffer)

    // Each header takes at least two bytes, so a count beyond that is corrupted and must not be allocated
    if (headersCount > (buffer.length - recordsPosition) >> 1) {
      throw new OutOfBoundsError('Out of bounds.')
    }

    /*
      The array is allocated at its final length: the first push on an empty array reserves room for 17 elements,
      while records usually have a few headers.
    */
    const headers: [Buffer | null, Buffer | null][] = headersCount > 0 ? new Array(headersCount) : []

    for (let j = 0; j < headersCount; j++) {
      const headerKey = readRecordVarIntBytes(buffer)
      headers[j] = [headerKey, readRecordVarIntBytes(buffer)]
    }

    records.push({ length, attributes, timestampDelta, offsetDelta, key, value, headers })
  }
}

// Offset of the records in a batch: BATCH_HEAD, PartitionLeaderEpoch, Magic, CRC and the fields from Attributes on
const RECORDS_OFFSET = 61
// Offset of the Attributes, which is where the data covered by the CRC starts
const CRC_DATA_OFFSET = 21

const INT32_MIN = -0x80000000
const INT32_MAX = 0x7fffffff

/*
  Records are written straight into the buffer of their batch with the helpers below.

  Using a Writer for each record means several small buffers for each of them, which then must be merged
  to compute the CRC of the batch. Here the exact size of every record is computed first, so that a single
  buffer of the right size can be allocated and nothing has to be merged, copied twice or grown.
*/
function sizeOfVarInt (value: number): number {
  const encoded = ((value << 1) ^ (value >> 31)) >>> 0

  if (encoded < 0x80) {
    return 1
  } else if (encoded < 0x4000) {
    return 2
  } else if (encoded < 0x200000) {
    return 3
  } else if (encoded < 0x10000000) {
    return 4
  }

  return 5
}

function sizeOfVarInt64 (value: bigint): number {
  let encoded = BigInt.asUintN(64, (value << 1n) ^ (value >> 63n))
  let size = 1

  while (encoded > 0x7fn) {
    size++
    encoded >>= 7n
  }

  return size
}

function sizeOfVarIntBytes (value: Buffer | null | undefined): number {
  // A missing value is encoded as a length of -1, which takes a single byte
  return value == null ? 1 : sizeOfVarInt(value.length) + value.length
}

function writeVarInt (buffer: Buffer, value: number, position: number): number {
  let encoded = ((value << 1) ^ (value >> 31)) >>> 0

  while (encoded > 0x7f) {
    buffer[position++] = (encoded & 0x7f) | 0x80
    encoded >>>= 7
  }

  buffer[position++] = encoded
  return position
}

function writeVarInt64 (buffer: Buffer, value: bigint, position: number): number {
  let encoded = BigInt.asUintN(64, (value << 1n) ^ (value >> 63n))

  while (encoded > 0x7fn) {
    buffer[position++] = Number(encoded & 0x7fn) | 0x80
    encoded >>= 7n
  }

  buffer[position++] = Number(encoded)
  return position
}

function writeVarIntBytes (buffer: Buffer, value: Buffer | null | undefined, position: number): number {
  if (value == null) {
    return writeVarInt(buffer, -1, position)
  }

  position = writeVarInt(buffer, value.length, position)
  buffer.set(value, position)

  return position + value.length
}

interface PreparedRecordsBatch {
  attributes: number
  // The encoded records, preceded by as many unused bytes as requested
  buffer: Buffer
  firstSequence: number
  firstTimestamp: bigint
  maxTimestamp: bigint
}

/*
  The headers of a record are a map, and iterating one with for...of allocates an array for each entry.
  forEach() does not, and these functions are defined once so that no closure is created for each record.
  Encoding is synchronous, which is what makes sharing this state possible.
*/
let headersSize = 0
let headersBuffer: Buffer
let headersPosition = 0

function addHeaderSize (value: Buffer, key: Buffer): void {
  headersSize += sizeOfVarIntBytes(key) + sizeOfVarIntBytes(value)
}

function writeHeader (value: Buffer, key: Buffer): void {
  headersPosition = writeVarIntBytes(headersBuffer, key, headersPosition)
  headersPosition = writeVarIntBytes(headersBuffer, value, headersPosition)
}

function prepareRecordsBatch (
  messages: MessageRecord[],
  options: Partial<CreateRecordsBatchOptions>,
  reserved: number
): PreparedRecordsBatch {
  const count = messages.length
  const now = BigInt(Date.now())
  const firstTimestamp = messages[0].timestamp ?? now
  let maxTimestamp = firstTimestamp

  /*
    First pass: compute the size of each record.

    Timestamp deltas are kept as numbers when they fit in 32 bits, which is true unless a batch spans more than
    24 days, so that BigInt is only used to subtract. NaN marks the deltas which need the full 64 bits.
  */
  const sizes = new Int32Array(count)
  const deltas = new Float64Array(count)
  let size = reserved

  for (let i = 0; i < count; i++) {
    const message = messages[i]
    let ts = message.timestamp ?? now

    /* c8 ignore next 3 - Hard to test */
    if (typeof ts === 'number') {
      ts = BigInt(ts)
    }

    message.timestamp = ts

    if (ts > maxTimestamp) maxTimestamp = ts

    const delta = Number(ts - firstTimestamp)
    let recordSize = 1 // Attributes

    if (delta >= INT32_MIN && delta <= INT32_MAX) {
      deltas[i] = delta
      recordSize += sizeOfVarInt(delta)
    } else {
      deltas[i] = NaN
      recordSize += sizeOfVarInt64(ts - firstTimestamp)
    }

    recordSize += sizeOfVarInt(i) + sizeOfVarIntBytes(message.key) + sizeOfVarIntBytes(message.value)

    const headers = message.headers

    if (headers == null) {
      recordSize += 1
    } else {
      recordSize += sizeOfVarInt(headers.size)

      headersSize = 0
      headers.forEach(addHeaderSize)
      recordSize += headersSize
    }

    sizes[i] = recordSize
    size += sizeOfVarInt(recordSize) + recordSize
  }

  // Second pass: write the records
  const buffer = Buffer.allocUnsafe(size)
  let position = reserved

  for (let i = 0; i < count; i++) {
    const message = messages[i]

    position = writeVarInt(buffer, sizes[i], position)
    buffer[position++] = 0 // Attributes are unused for now

    const delta = deltas[i]

    // NaN is the only value which is different from itself
    /* eslint-disable-next-line no-self-compare */
    if (delta === delta) {
      position = writeVarInt(buffer, delta, position)
    } else {
      position = writeVarInt64(buffer, (message.timestamp as bigint) - firstTimestamp, position)
    }

    position = writeVarInt(buffer, i, position)
    position = writeVarIntBytes(buffer, message.key, position)
    position = writeVarIntBytes(buffer, message.value, position)

    const headers = message.headers

    if (headers == null) {
      position = writeVarInt(buffer, 0, position)
    } else {
      position = writeVarInt(buffer, headers.size, position)

      headersBuffer = buffer
      headersPosition = position
      headers.forEach(writeHeader)
      position = headersPosition
    }
  }

  let attributes = 0
  let firstSequence = 0

  if (options.sequences) {
    const firstMessage = messages[0]
    firstSequence = options.sequences.getWithDefault(`${firstMessage.topic}:${firstMessage.partition}`, 0)
  }

  if (options.transactionalId) {
    attributes |= IS_TRANSACTIONAL
  }

  return { attributes, buffer, firstSequence, firstTimestamp, maxTimestamp }
}

function getCompressionAlgorithm (
  compression: CompressionAlgorithmValue | undefined
): CompressionAlgorithmSpecification | undefined {
  if ((compression ?? 'none') === 'none') {
    return undefined
  }

  const algorithm = compressionsAlgorithms[
    compression as keyof typeof compressionsAlgorithms
  ] as CompressionAlgorithmSpecification

  if (!algorithm) {
    throw new UnsupportedCompressionError(`Unsupported compression algorithm ${compression}`)
  }

  return algorithm
}

/*
  Fills in everything which precedes the records and returns the batch.

  When the records are not compressed, prepared.buffer already has room for those fields in front of the records.
  When they are, the compressed records are copied after them in a new buffer.
*/
function finalizeRecordsBatch (
  messages: MessageRecord[],
  options: Partial<CreateRecordsBatchOptions>,
  prepared: PreparedRecordsBatch,
  compressed?: Buffer
): Writer {
  let batch = prepared.buffer

  if (compressed) {
    batch = Buffer.allocUnsafe(RECORDS_OFFSET + compressed.length)
    batch.set(compressed, RECORDS_OFFSET)
  }

  batch.writeBigInt64BE(0n, 0) // FirstOffset is 0
  batch.writeInt32BE(batch.length - BATCH_HEAD, 8) // Length
  batch.writeInt32BE(options.partitionLeaderEpoch ?? 0, 12)
  batch.writeInt8(CURRENT_RECORD_VERSION, 16)
  batch.writeInt16BE(prepared.attributes, 21)
  // LastOffsetDelta, FirstTimestamp and MaxTimestamp are extracted from the messages
  batch.writeInt32BE(messages.length - 1, 23)
  batch.writeBigInt64BE(BigInt(prepared.firstTimestamp), 27)
  batch.writeBigInt64BE(BigInt(prepared.maxTimestamp), 35)
  batch.writeBigInt64BE(options.producerId ?? -1n, 43)
  batch.writeInt16BE(options.producerEpoch ?? 0, 51)
  batch.writeInt32BE(prepared.firstSequence, 53)
  batch.writeInt32BE(messages.length, 57) // Number of records

  // The CRC covers everything from the Attributes to the end of the batch
  batch.writeUInt32BE(crc32c(batch.subarray(CRC_DATA_OFFSET)), 17)

  return Writer.create().append(batch)
}

export function createRecordsBatch (
  messages: MessageRecord[],
  options: Partial<CreateRecordsBatchOptions> = {}
): Writer {
  const algorithm = getCompressionAlgorithm(options.compression)

  if (!algorithm) {
    return finalizeRecordsBatch(messages, options, prepareRecordsBatch(messages, options, RECORDS_OFFSET))
  }

  const prepared = prepareRecordsBatch(messages, options, 0)
  prepared.attributes |= algorithm.bitmask

  return finalizeRecordsBatch(messages, options, prepared, algorithm.compressSync(prepared.buffer))
}

export async function createRecordsBatchAsync (
  messages: MessageRecord[],
  options: Partial<CreateRecordsBatchOptions> = {}
): Promise<Writer> {
  const algorithm = getCompressionAlgorithm(options.compression)

  if (!algorithm) {
    return finalizeRecordsBatch(messages, options, prepareRecordsBatch(messages, options, RECORDS_OFFSET))
  }

  const prepared = prepareRecordsBatch(messages, options, 0)
  prepared.attributes |= algorithm.bitmask

  const compressed = algorithm.compress
    ? await algorithm.compress(prepared.buffer)
    : algorithm.compressSync(prepared.buffer)

  return finalizeRecordsBatch(messages, options, prepared, compressed)
}

// TODO: Early bail out if there are not enough bytes to read all the records as it might be truncated
export function readRecordsBatch (reader: Reader): RecordsBatch {
  const initialPosition = reader.position
  const batch = {
    firstOffset: reader.readInt64(),
    length: reader.readInt32(),
    partitionLeaderEpoch: reader.readInt32(),
    magic: reader.readInt8(),
    crc: reader.readUnsignedInt32(),
    attributes: reader.readInt16(),
    lastOffsetDelta: reader.readInt32(),
    firstTimestamp: reader.readInt64(),
    maxTimestamp: reader.readInt64(),
    producerId: reader.readInt64(),
    producerEpoch: reader.readInt16(),
    firstSequence: reader.readInt32(),
    records: [] as KafkaRecord[]
  }

  const recordsLength = reader.readInt32()
  const compression = batch.attributes & IS_COMPRESSED

  const algorithm = compression !== 0 ? compressionsAlgorithmsByBitmask[compression] : undefined

  if (compression !== 0 && !algorithm) {
    throw new UnsupportedCompressionError(`Unsupported compression algorithm with bitmask ${compression}`)
  }

  // Everything from here to the end of the batch is the records, which might be compressed
  const recordsEnd = initialPosition + BATCH_HEAD + batch.length
  let records = reader.buffer.slice(reader.position, recordsEnd)
  reader.position = recordsEnd

  if (algorithm) {
    records = algorithm.decompressSync(records)
  }

  readRecords(records, recordsLength, batch.records)

  return batch
}

export function readRecordsBatches (reader: Reader): RecordsBatch[] {
  const batches: RecordsBatch[] = []

  while (
    reader.remaining >= BATCH_HEAD &&
    reader.remaining - BATCH_HEAD >= reader.peekInt32(reader.position + INT64_SIZE)
  ) {
    batches.push(readRecordsBatch(reader))
  }

  return batches
}
