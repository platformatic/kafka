import BufferList from 'bl'
import { ERROR_UNSUPPORTED_COMPRESSION, KafkaError } from '../error.ts'
import {
  type CompressionAlgorithm,
  type CompressionAlgorithms,
  compressionsAlgorithms,
  compressionsAlgorithmsByBitmask
} from './compression.ts'
import { crc32c } from './crc32c.ts'
import { INT16_SIZE, INT32_SIZE, INT64_SIZE, INT8_SIZE, sizeOfCompactable } from './definitions.ts'
import { Reader } from './reader.ts'
import { sizeOfVarInt } from './varint32.ts'
import { sizeOfVarInt64 } from './varint64.ts'
import { Writer } from './writer.ts'

const CURRENT_RECORD_VERSION = 2

const RECORD_BATCH_HEAD_SIZE =
  INT64_SIZE + // FirstOffset
  INT32_SIZE + // Length
  INT32_SIZE + // PartitionLeaderEpoch
  INT8_SIZE + // Magic
  INT32_SIZE // CRC

const RECORD_BATCH_TAIL_SIZE =
  INT16_SIZE + // Attributes
  INT32_SIZE + // LastOffsetDelta
  INT64_SIZE + // FirstTimestamp
  INT64_SIZE + // MaxTimestamp
  INT64_SIZE + // ProducerId
  INT16_SIZE + // ProducerEpoch
  INT32_SIZE + // FirstSequence
  INT32_SIZE // Records

const IS_TRANSACTIONAL = 0b10000 // Bit 4 set
const IS_COMPRESSED = 0b111 // Bits 0, 1 and/or 2 set

export interface Message {
  key: string | Buffer
  value: string | Buffer
  headers?: Record<string, string | Buffer>
  topic: string
  partition?: number
  timestamp?: bigint
}

export interface CreateRecordsBatchOptions {
  transactional: boolean
  compression: CompressionAlgorithms

  // Idempotency support
  producerId: bigint
  producerEpoch: number
  firstSequence: number

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

  // Note: COMPACT_BYTES here do not follow the standard procedure where the length is increased by one
  Buffer allocations: Headers + 1
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

  Buffer allocations: Record + 1
*/
export interface KafkaRecordsBatch {
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

export function readRecord (reader: Reader): KafkaRecord {
  const length = reader.readVarInt()
  const attributes = reader.readInt8()
  const timestampDelta = reader.readVarInt64()
  const offsetDelta = reader.readVarInt()
  const key = reader.readVarIntBytes()
  const value = reader.readVarIntBytes()

  const headersLength = reader.readVarInt()
  const headers: [Buffer, Buffer][] = []
  for (let i = 0; i < headersLength; i++) {
    headers.push([reader.readVarIntBytes(), reader.readVarIntBytes()])
  }

  return {
    length,
    attributes,
    timestampDelta,
    offsetDelta,
    key,
    value,
    headers
  }
}
export function createRecord (message: Message, offsetDelta: number, firstTimestamp: bigint): BufferList {
  const timestampDelta = (message.timestamp ?? BigInt(Date.now())) - firstTimestamp

  const buffer = new BufferList()

  const writer = new Writer()

  // Serialize the headers first
  for (const [key, value] of Object.entries(message.headers ?? {})) {
    writer
      .reset(sizeOfCompactable(key) + sizeOfCompactable(value))
      .writeVarIntBytes(key)
      .writeVarIntBytes(value)
      .appendTo(buffer)
  }

  // @ts-expect-error _bufs is not exposed
  const headerPairs = buffer._bufs.length
  const headerLength = buffer.length

  const length =
    INT8_SIZE + // Attributes
    sizeOfVarInt64(timestampDelta) +
    sizeOfVarInt(offsetDelta) +
    sizeOfCompactable(message.key) +
    sizeOfCompactable(message.value) +
    sizeOfVarInt(headerPairs)

  writer
    .reset(sizeOfVarInt(length) + length)
    .writeVarInt(length + headerLength)
    .writeInt8(0) // Attributes are unused for now
    .writeVarInt64(timestampDelta)
    .writeVarInt(offsetDelta)
    .writeVarIntBytes(message.key)
    .writeVarIntBytes(message.value)
    .writeVarInt(headerPairs)
    .prependTo(buffer)

  return buffer
}

export async function readRecordsBatch (reader: Reader): Promise<KafkaRecordsBatch> {
  const firstOffset = reader.readInt64()
  const length = reader.readInt32()
  let fieldsAfterLengthSize = reader.position

  const partitionLeaderEpoch = reader.readInt32()
  const magic = reader.readInt8()
  const crc = reader.readUnsignedInt32()
  const attributes = reader.readInt16()
  const lastOffsetDelta = reader.readInt32()
  const firstTimestamp = reader.readInt64()
  const maxTimestamp = reader.readInt64()
  const producerId = reader.readInt64()
  const producerEpoch = reader.readInt16()
  const firstSequence = reader.readInt32()
  const recordsLength = reader.readInt32()
  fieldsAfterLengthSize = reader.position - fieldsAfterLengthSize

  const compression = attributes & IS_COMPRESSED
  if (compression !== 0) {
    const algorithm = compressionsAlgorithmsByBitmask[compression]

    if (!algorithm) {
      throw new KafkaError(`Unsupported compression algorithm with bitmask ${compression}`, {
        code: ERROR_UNSUPPORTED_COMPRESSION
      })
    }

    const buffer = await algorithm.decode(
      reader.buffer.slice(reader.position, reader.position + length - fieldsAfterLengthSize)
    )

    reader = new Reader(new BufferList(buffer))
  }

  const records: KafkaRecord[] = []

  for (let i = 0; i < recordsLength; i++) {
    records.push(readRecord(reader))
  }

  return {
    firstOffset,
    length,
    partitionLeaderEpoch,
    magic,
    crc,
    attributes,
    lastOffsetDelta,
    firstTimestamp,
    maxTimestamp,
    producerId,
    producerEpoch,
    firstSequence,
    records
  }
}

export async function createRecordsBatch (
  messages: Message[],
  options: Partial<CreateRecordsBatchOptions> = {}
): Promise<BufferList> {
  const now = BigInt(Date.now())
  const timestamps = messages.map(m => m.timestamp ?? now).sort()
  const firstTimestamp = timestamps[0]
  const maxTimestamp = timestamps[timestamps.length - 1]

  let buffer = new BufferList()
  for (let i = 0; i < messages.length; i++) {
    const record = createRecord(messages[i], i, firstTimestamp)
    buffer.append(record)
  }

  let attributes = 0

  // Set the transaction
  if (options.transactional) {
    attributes |= IS_TRANSACTIONAL
  }

  // Set the compression, if any
  if ((options.compression ?? 'none') !== 'none') {
    const algorithm = compressionsAlgorithms[
      options.compression as keyof typeof compressionsAlgorithms
    ] as CompressionAlgorithm

    if (!algorithm) {
      throw new KafkaError(`Unsupported compression algorithm ${options.compression}`, {
        code: ERROR_UNSUPPORTED_COMPRESSION
      })
    }

    attributes |= algorithm.bitmask

    const compressed = await algorithm.encode(buffer)
    buffer = new BufferList(compressed)
  }

  // Prepare the tail of the header
  const writer = Writer.create(RECORD_BATCH_TAIL_SIZE)
    .writeInt16(attributes)
    // LastOffsetDelta, FirstTimestamp and MaxTimestamp are extracted from the messages
    .writeInt32(messages.length - 1)
    .writeInt64(BigInt(firstTimestamp))
    .writeInt64(BigInt(maxTimestamp))
    .writeInt64(options.producerId ?? -1n)
    .writeInt16(options.producerEpoch ?? 0)
    .writeInt32(options.firstSequence ?? 0)
    .writeInt32(messages.length) // Number of records
    .prependTo(buffer)

  const crc = crc32c(buffer)

  writer
    .reset(RECORD_BATCH_HEAD_SIZE)
    .writeInt64(0n) // FirstOffset is 0
    // Length of the remainder of the message - Note that since appending of this buffer to the list happens
    // after this, we need to account for the size the head itself
    .writeInt32(buffer.length + RECORD_BATCH_HEAD_SIZE - INT32_SIZE - INT64_SIZE)
    .writeInt32(options.partitionLeaderEpoch ?? 0)
    .writeInt8(CURRENT_RECORD_VERSION)
    .writeUnsignedInt32(crc)
    .prependTo(buffer)

  return buffer
}
