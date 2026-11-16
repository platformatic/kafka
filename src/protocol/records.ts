import { DynamicBuffer, int64ZigZagDecode, intZigZagDecode, OutOfBoundsError } from '@platformatic/dynamic-buffer'
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
    const headers: [Buffer | null, Buffer | null][] = []

    for (let j = 0; j < headersCount; j++) {
      const headerKey = readRecordVarIntBytes(buffer)
      headers.push([headerKey, readRecordVarIntBytes(buffer)])
    }

    records.push({ length, attributes, timestampDelta, offsetDelta, key, value, headers })
  }
}

interface PreparedRecordsBatch {
  attributes: number
  buffer: DynamicBuffer
  firstSequence: number
  firstTimestamp: bigint
  maxTimestamp: bigint
}

function prepareRecordsBatch (
  messages: MessageRecord[],
  options: Partial<CreateRecordsBatchOptions>
): PreparedRecordsBatch {
  const now = BigInt(Date.now())
  const firstTimestamp = messages[0].timestamp ?? now
  let maxTimestamp = firstTimestamp

  const buffer = new DynamicBuffer()
  for (let i = 0; i < messages.length; i++) {
    let ts = messages[i].timestamp ?? now

    /* c8 ignore next 3 - Hard to test */
    if (typeof ts === 'number') {
      ts = BigInt(ts)
    }

    messages[i].timestamp = ts

    if (ts > maxTimestamp) maxTimestamp = ts

    const record = createRecord(messages[i], i, firstTimestamp)
    buffer.appendFrom(record.dynamicBuffer)
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

function finalizeRecordsBatch (
  messages: MessageRecord[],
  options: Partial<CreateRecordsBatchOptions>,
  prepared: PreparedRecordsBatch
): Writer {
  const writer = Writer.create()
    // Phase 1: Prepare the message from Attributes (included) to the end
    .appendInt16(prepared.attributes)
    // LastOffsetDelta, FirstTimestamp and MaxTimestamp are extracted from the messages
    .appendInt32(messages.length - 1)
    .appendInt64(BigInt(prepared.firstTimestamp))
    .appendInt64(BigInt(prepared.maxTimestamp))
    .appendInt64(options.producerId ?? -1n)
    .appendInt16(options.producerEpoch ?? 0)
    .appendInt32(prepared.firstSequence)
    .appendInt32(messages.length) // Number of records
    .appendFrom(prepared.buffer)

  // Phase 2: Prepend the PartitionLeaderEpoch, Magic and CRC, then the Length and firstOffset, in reverse order
  return (
    writer
      .appendUnsignedInt32(crc32c(writer.dynamicBuffer), false)
      .appendInt8(CURRENT_RECORD_VERSION, false)
      .appendInt32(options.partitionLeaderEpoch ?? 0, false)
      .prependLength()
      // FirstOffset is 0
      .appendInt64(0n, false)
  )
}

export function createRecordsBatch (
  messages: MessageRecord[],
  options: Partial<CreateRecordsBatchOptions> = {}
): Writer {
  const prepared = prepareRecordsBatch(messages, options)
  const algorithm = getCompressionAlgorithm(options.compression)

  if (algorithm) {
    prepared.attributes |= algorithm.bitmask
    prepared.buffer = new DynamicBuffer(algorithm.compressSync(prepared.buffer.buffer))
  }

  return finalizeRecordsBatch(messages, options, prepared)
}

export async function createRecordsBatchAsync (
  messages: MessageRecord[],
  options: Partial<CreateRecordsBatchOptions> = {}
): Promise<Writer> {
  const prepared = prepareRecordsBatch(messages, options)
  const algorithm = getCompressionAlgorithm(options.compression)

  if (algorithm) {
    prepared.attributes |= algorithm.bitmask
    const compressed = algorithm.compress
      ? await algorithm.compress(prepared.buffer.buffer)
      : algorithm.compressSync(prepared.buffer.buffer)
    prepared.buffer = new DynamicBuffer(compressed)
  }

  return finalizeRecordsBatch(messages, options, prepared)
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
