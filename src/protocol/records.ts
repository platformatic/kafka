import { UnsupportedCompressionError } from '../errors.ts'
import { type NumericMap } from '../utils.ts'
import {
  type CompressionAlgorithm,
  type CompressionAlgorithms,
  compressionsAlgorithms,
  compressionsAlgorithmsByBitmask
} from './compression.ts'
import { crc32c } from './crc32c.ts'
import { type NullableString } from './definitions.ts'
import { DynamicBuffer } from './dynamic-buffer.ts'
import { Reader } from './reader.ts'
import { Writer } from './writer.ts'

const CURRENT_RECORD_VERSION = 2
const IS_TRANSACTIONAL = 0b10000 // Bit 4 set
const IS_COMPRESSED = 0b111 // Bits 0, 1 and/or 2 set

export interface MessageBase<Key = Buffer, Value = Buffer> {
  key?: Key
  value?: Value
  topic: string
  partition?: number
  timestamp?: bigint
}

// This is used in produce
export interface MessageToProduce<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer>
  extends MessageBase<Key, Value> {
  headers?: Map<HeaderKey, HeaderValue> | Record<string, HeaderValue>
}

// This is used in consume
export interface Message<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer>
  extends Required<MessageBase<Key, Value>> {
  headers: Map<HeaderKey, HeaderValue>
  offset: bigint
  commit(callback?: (error?: Error) => void): void | Promise<void>
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
  compression: CompressionAlgorithms

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
  key: Buffer
  value: Buffer
  headers: [Buffer, Buffer][]
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
    key: {
      oneOf: [{ type: 'string' }, { buffer: true }]
    },
    value: {
      oneOf: [{ type: 'string' }, { buffer: true }]
    },
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

export function createRecordsBatch (
  messages: MessageRecord[],
  options: Partial<CreateRecordsBatchOptions> = {}
): Writer {
  const now = BigInt(Date.now())
  const timestamps = []

  for (let i = 0; i < messages.length; i++) {
    timestamps.push(messages[i].timestamp ?? now)
  }

  messages.sort()

  const firstTimestamp = timestamps[0]
  const maxTimestamp = timestamps[timestamps.length - 1]

  let buffer = new DynamicBuffer()
  for (let i = 0; i < messages.length; i++) {
    const record = createRecord(messages[i], i, firstTimestamp)
    buffer.appendFrom(record.dynamicBuffer)
  }

  let attributes = 0

  let firstSequence = 0

  if (options.sequences) {
    const firstMessage = messages[0]
    firstSequence = options.sequences.getWithDefault(`${firstMessage.topic}:${firstMessage.partition}`, 0)
  }

  // Set the transaction
  if (options.transactionalId) {
    attributes |= IS_TRANSACTIONAL
  }

  // Set the compression, if any
  /* c8 ignore next */
  if ((options.compression ?? 'none') !== 'none') {
    const algorithm = compressionsAlgorithms[
      options.compression as keyof typeof compressionsAlgorithms
    ] as CompressionAlgorithm

    if (!algorithm) {
      throw new UnsupportedCompressionError(`Unsupported compression algorithm ${options.compression}`)
    }

    attributes |= algorithm.bitmask

    const compressed = algorithm.compressSync(buffer.buffer)
    buffer = new DynamicBuffer(compressed)
  }

  const writer = Writer.create()
    // Phase 1: Prepare the message from Attributes (included) to the end
    .appendInt16(attributes)
    // LastOffsetDelta, FirstTimestamp and MaxTimestamp are extracted from the messages
    .appendInt32(messages.length - 1)
    .appendInt64(BigInt(firstTimestamp))
    .appendInt64(BigInt(maxTimestamp))
    /* c8 ignore next */
    .appendInt64(options.producerId ?? -1n)
    /* c8 ignore next */
    .appendInt16(options.producerEpoch ?? 0)
    .appendInt32(firstSequence)
    .appendInt32(messages.length) // Number of records
    .appendFrom(buffer)

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

export function readRecordsBatch (reader: Reader): RecordsBatch {
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

  if (compression !== 0) {
    const algorithm = compressionsAlgorithmsByBitmask[compression]

    if (!algorithm) {
      throw new UnsupportedCompressionError(`Unsupported compression algorithm with bitmask ${compression}`)
    }

    const buffer = algorithm.decompressSync(reader.buffer.slice(reader.position, reader.buffer.length))

    reader = Reader.from(buffer)
  }

  for (let i = 0; i < recordsLength; i++) {
    batch.records.push(readRecord(reader))
  }

  return batch
}
