import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import test from 'node:test'
import {
  compressionsAlgorithms,
  createRecord,
  createRecordsBatch,
  NumericMap,
  Reader,
  readRecord,
  readRecordsBatch,
  Writer,
  type CompressionAlgorithm,
  type MessageRecord
} from '../../src/index.ts'

test('createRecord should create a buffer with the correct format', () => {
  const timestamp = BigInt(1720000000000) // Fixed timestamp for testing
  const firstTimestamp = BigInt(1720000000000) // Same timestamp for simplicity
  const message: MessageRecord = {
    key: Buffer.from('key'),
    value: Buffer.from('value'),
    headers: new Map([
      [Buffer.from('header1'), Buffer.from('value1')],
      [Buffer.from('header2'), Buffer.from('value2')]
    ]),
    topic: 'test-topic',
    timestamp
  }

  const offsetDelta = 5
  const recordBuffer = createRecord(message, offsetDelta, firstTimestamp)

  // Verify the record has the expected format
  const reader = Reader.from(recordBuffer)

  const length = reader.readVarInt()
  strictEqual(length > 0, true, 'Length should be greater than 0')

  const attributes = reader.readInt8()
  strictEqual(attributes, 0, 'Attributes should be 0')

  const timestampDelta = reader.readVarInt64()
  strictEqual(timestampDelta, 0n, 'Timestamp delta should be 0 (same as firstTimestamp)')

  const readOffsetDelta = reader.readVarInt()
  strictEqual(readOffsetDelta, offsetDelta, 'Offset delta should match')

  const key = reader.readVarIntBytes()
  ok(Buffer.isBuffer(key), 'Key should be a buffer')
  strictEqual(key.toString(), 'key', 'Key should match')

  const value = reader.readVarIntBytes()
  ok(Buffer.isBuffer(value), 'Value should be a buffer')
  strictEqual(value.toString(), 'value', 'Value should match')

  const headers = reader.readVarIntArray(r => [r.readVarIntBytes(), r.readVarIntBytes()])
  strictEqual(headers.length, 2, 'Should have two headers')
  strictEqual(headers[0][0].toString(), 'header1', 'First header key should match')
  strictEqual(headers[0][1].toString(), 'value1', 'First header value should match')
  strictEqual(headers[1][0].toString(), 'header2', 'Second header key should match')
  strictEqual(headers[1][1].toString(), 'value2', 'Second header value should match')
})

test('createRecord should handle null/undefined values', () => {
  const firstTimestamp = BigInt(1720000000000)
  const message: MessageRecord = {
    value: Buffer.from('value'),
    topic: 'test-topic'
    // No key, headers, or timestamp specified
  }

  const offsetDelta = 0
  const recordBuffer = createRecord(message, offsetDelta, firstTimestamp)

  const reader = Reader.from(recordBuffer)

  const length = reader.readVarInt()
  strictEqual(length > 0, true, 'Length should be greater than 0')

  reader.readInt8() // Skip attributes

  // When timestamp is not provided, it should use Date.now()
  const timestampDelta = reader.readVarInt64()
  strictEqual(typeof timestampDelta, 'bigint', 'Timestamp delta should be a bigint')

  reader.readVarInt() // Skip offset delta

  const key = reader.readVarIntBytes()
  // The implementation uses an empty buffer for null keys, not null
  deepStrictEqual(key, Buffer.alloc(0), 'Key should be an empty buffer')

  const value = reader.readVarIntBytes()
  ok(Buffer.isBuffer(value), 'Value should be a buffer')
  strictEqual(value.toString(), 'value', 'Value should match')

  const headers = reader.readVarIntArray(r => [r.readVarIntBytes(), r.readVarIntBytes()])
  strictEqual(headers.length, 0, 'Should have no headers')
})

test('readRecord should correctly parse a record buffer', () => {
  // Create a mock record buffer
  const writer = Writer.create()
    .appendVarInt(20) // Length
    .appendInt8(0) // Attributes
    .appendVarInt64(10n) // Timestamp delta
    .appendVarInt(5) // Offset delta
    .appendVarIntBytes(Buffer.from('key')) // Key
    .appendVarIntBytes(Buffer.from('value')) // Value
    .appendVarInt(2) // 2 headers
    .appendVarIntBytes(Buffer.from('header1'))
    .appendVarIntBytes(Buffer.from('value1'))
    .appendVarIntBytes(Buffer.from('header2'))
    .appendVarIntBytes(Buffer.from('value2'))

  const reader = Reader.from(writer)
  const record = readRecord(reader)

  strictEqual(record.length, 20, 'Length should match')
  strictEqual(record.attributes, 0, 'Attributes should match')
  strictEqual(record.timestampDelta, 10n, 'Timestamp delta should match')
  strictEqual(record.offsetDelta, 5, 'Offset delta should match')
  strictEqual(record.key.toString(), 'key', 'Key should match')
  strictEqual(record.value.toString(), 'value', 'Value should match')
  strictEqual(record.headers.length, 2, 'Should have two headers')
  strictEqual(record.headers[0][0].toString(), 'header1', 'First header key should match')
  strictEqual(record.headers[0][1].toString(), 'value1', 'First header value should match')
  strictEqual(record.headers[1][0].toString(), 'header2', 'Second header key should match')
  strictEqual(record.headers[1][1].toString(), 'value2', 'Second header value should match')
})

test('createRecordsBatch should create a batch with the correct format', () => {
  const messages: MessageRecord[] = [
    {
      key: Buffer.from('key1'),
      value: Buffer.from('value1'),
      topic: 'test-topic',
      partition: 0,
      timestamp: BigInt(1720000000000)
    },
    {
      key: Buffer.from('key2'),
      value: Buffer.from('value2'),
      topic: 'test-topic',
      partition: 0,
      timestamp: BigInt(1720000000100)
    }
  ]

  const options = {
    producerId: 123456789n,
    producerEpoch: 42,
    partitionLeaderEpoch: 5,
    compression: 'none' as const
  }

  const batchBuffer = createRecordsBatch(messages, options)

  // Verify the batch has the expected format
  const reader = Reader.from(batchBuffer)

  const firstOffset = reader.readInt64()
  strictEqual(firstOffset, 0n, 'First offset should be 0')

  const length = reader.readInt32()
  strictEqual(length > 0, true, 'Length should be greater than 0')

  const partitionLeaderEpoch = reader.readInt32()
  strictEqual(partitionLeaderEpoch, 5, 'Partition leader epoch should match')

  const magic = reader.readInt8()
  strictEqual(magic, 2, 'Magic should be 2')

  const crc = reader.readUnsignedInt32()
  strictEqual(typeof crc, 'number', 'CRC should be a number')

  const attributes = reader.readInt16()
  strictEqual(attributes, 0, 'Attributes should be 0 for no compression')

  const lastOffsetDelta = reader.readInt32()
  strictEqual(lastOffsetDelta, 1, 'Last offset delta should be 1')

  const firstTimestamp = reader.readInt64()
  strictEqual(firstTimestamp, BigInt(1720000000000), 'First timestamp should match')

  const maxTimestamp = reader.readInt64()
  strictEqual(maxTimestamp, BigInt(1720000000100), 'Max timestamp should match')

  const producerId = reader.readInt64()
  strictEqual(producerId, 123456789n, 'Producer ID should match')

  const producerEpoch = reader.readInt16()
  strictEqual(producerEpoch, 42, 'Producer epoch should match')

  const firstSequence = reader.readInt32()
  strictEqual(firstSequence, 0, 'First sequence should be 0')

  const recordsLength = reader.readInt32()
  strictEqual(recordsLength, 2, 'Records length should be 2')

  // Verify each record
  const record1 = readRecord(reader)
  strictEqual(record1.offsetDelta, 0, 'First record offset delta should be 0')
  strictEqual(record1.key.toString(), 'key1', 'First record key should match')
  strictEqual(record1.value.toString(), 'value1', 'First record value should match')

  const record2 = readRecord(reader)
  strictEqual(record2.offsetDelta, 1, 'Second record offset delta should be 1')
  strictEqual(record2.key.toString(), 'key2', 'Second record key should match')
  strictEqual(record2.value.toString(), 'value2', 'Second record value should match')
})

test('createRecordsBatch should handle minimal options', () => {
  const messages: MessageRecord[] = [
    {
      value: Buffer.from('value'),
      topic: 'test-topic'
    }
  ]

  // Minimal options
  const options = {
    producerId: 0n,
    compression: 'none' as const
  }

  const batchBuffer = createRecordsBatch(messages, options)

  // Just verify we can read it without errors
  const reader = Reader.from(batchBuffer)
  const batch = readRecordsBatch(reader)

  strictEqual(batch.records.length, 1, 'Should have one record')
  strictEqual(batch.records[0].value.toString(), 'value', 'Record value should match')
})

test('createRecordsBatch with compression should compress the records', () => {
  const messages: MessageRecord[] = [
    {
      value: Buffer.from('value1'.repeat(100)), // Make it bigger to see compression effect
      topic: 'test-topic'
    },
    {
      value: Buffer.from('value2'.repeat(100)),
      topic: 'test-topic'
    }
  ]

  // Use gzip compression
  const options = {
    producerId: 0n,
    compression: 'gzip' as const
  }

  const batchBuffer = createRecordsBatch(messages, options)

  // Verify it's compressed
  const reader = Reader.from(batchBuffer)
  const batch = readRecordsBatch(reader)

  strictEqual(batch.attributes & 0b111, 1, 'Attributes should have gzip compression bit set')
  strictEqual(batch.records.length, 2, 'Should have two records after decompression')
  strictEqual(batch.records[0].value.toString(), 'value1'.repeat(100), 'First record value should match')
  strictEqual(batch.records[1].value.toString(), 'value2'.repeat(100), 'Second record value should match')
})

test('createRecordsBatch should throw on unsupported compression', () => {
  const messages: MessageRecord[] = [
    {
      value: Buffer.from('value'),
      topic: 'test-topic'
    }
  ]

  // Invalid compression
  const options = {
    producerId: 0n,
    compression: 'invalid-algorithm' as any
  }

  let error: Error | null = null
  try {
    createRecordsBatch(messages, options)
  } catch (err: any) {
    error = err
  }

  ok(error instanceof Error, 'Should throw an error')
  strictEqual(error!.message, 'Unsupported compression algorithm invalid-algorithm')
})

test('createRecordsBatch should handle transactionalId', () => {
  const messages: MessageRecord[] = [
    {
      value: Buffer.from('value'),
      topic: 'test-topic'
    }
  ]

  const options = {
    producerId: 0n,
    compression: 'none' as const,
    transactionalId: 'transaction-1'
  }

  const batchBuffer = createRecordsBatch(messages, options)

  // Verify transaction bit is set
  const reader = Reader.from(batchBuffer)
  reader.readInt64() // firstOffset
  reader.readInt32() // length
  reader.readInt32() // partitionLeaderEpoch
  reader.readInt8() // magic
  reader.readUnsignedInt32() // crc

  const attributes = reader.readInt16()
  strictEqual((attributes & 0b10000) !== 0, true, 'Transaction bit should be set')
})

test('createRecordsBatch should handle sequence numbers', () => {
  const messages: MessageRecord[] = [
    {
      value: Buffer.from('value'),
      topic: 'test-topic',
      partition: 0
    }
  ]

  // Need to use NumericMap class for sequences, not a regular Map
  const sequencesMap = new NumericMap()
  sequencesMap.set('test-topic:0', 42)

  const options = {
    producerId: 0n,
    compression: 'none' as const,
    sequences: sequencesMap
  }

  const batchBuffer = createRecordsBatch(messages, options)

  // Verify first sequence is read from the map
  const reader = Reader.from(batchBuffer)
  reader.readInt64() // firstOffset
  reader.readInt32() // length
  reader.readInt32() // partitionLeaderEpoch
  reader.readInt8() // magic
  reader.readUnsignedInt32() // crc
  reader.readInt16() // attributes
  reader.readInt32() // lastOffsetDelta
  reader.readInt64() // firstTimestamp
  reader.readInt64() // maxTimestamp
  reader.readInt64() // producerId
  reader.readInt16() // producerEpoch

  const firstSequence = reader.readInt32()
  strictEqual(firstSequence, 42, 'First sequence should match the one in the map')
})

test('readRecordsBatch should handle records with compression', () => {
  // First, create a compressed batch
  const messages: MessageRecord[] = [
    {
      value: Buffer.from('compressed1'),
      topic: 'test-topic'
    },
    {
      value: Buffer.from('compressed2'),
      topic: 'test-topic'
    }
  ]

  const options = {
    producerId: 0n,
    compression: 'gzip' as const
  }

  const batchBuffer = createRecordsBatch(messages, options)

  // Now read and verify it
  const reader = Reader.from(batchBuffer)
  const batch = readRecordsBatch(reader)

  strictEqual(batch.attributes & 0b111, 1, 'Attributes should have gzip compression bit set')
  strictEqual(batch.records.length, 2, 'Should have two records')
  strictEqual(batch.records[0].value.toString(), 'compressed1', 'First record value should match')
  strictEqual(batch.records[1].value.toString(), 'compressed2', 'Second record value should match')
})

test('readRecordsBatch should throw on unsupported compression bitmask', () => {
  // Create a mock batch with an invalid compression bitmask
  const writer = Writer.create()
    .appendInt64(0n) // firstOffset
    .appendInt32(100) // length
    .appendInt32(0) // partitionLeaderEpoch
    .appendInt8(2) // magic
    .appendUnsignedInt32(0) // crc
    .appendInt16(0b111111) // attributes with invalid compression bits
    .appendInt32(0) // lastOffsetDelta
    .appendInt64(0n) // firstTimestamp
    .appendInt64(0n) // maxTimestamp
    .appendInt64(0n) // producerId
    .appendInt16(0) // producerEpoch
    .appendInt32(0) // firstSequence
    .appendInt32(0) // recordsLength

  const reader = Reader.from(writer)

  let error: Error | null = null
  try {
    readRecordsBatch(reader)
  } catch (err: any) {
    error = err
  }

  ok(error instanceof Error, 'Should throw an error')
  ok(
    error!.message.includes('Unsupported compression algorithm with bitmask'),
    'Error message should mention unsupported compression'
  )
})

// Test integration of createRecordsBatch and readRecordsBatch
test('createRecordsBatch and readRecordsBatch should be symmetrical', () => {
  const timestamp1 = BigInt(1720000000000)
  const timestamp2 = BigInt(1720000000100)

  const messages: MessageRecord[] = [
    {
      key: Buffer.from('roundtrip-key-1'),
      value: Buffer.from('roundtrip-value-1'),
      headers: new Map([[Buffer.from('header1'), Buffer.from('header-value-1')]]),
      topic: 'roundtrip-topic',
      partition: 2,
      timestamp: timestamp1
    },
    {
      key: Buffer.from('roundtrip-key-2'),
      value: Buffer.from('roundtrip-value-2'),
      headers: new Map([[Buffer.from('header2'), Buffer.from('header-value-2')]]),
      topic: 'roundtrip-topic',
      partition: 2,
      timestamp: timestamp2
    }
  ]

  const options = {
    producerId: 9876543210n,
    producerEpoch: 21,
    compression: 'none' as const
  }

  // Create a batch
  const batchBuffer = createRecordsBatch(messages, options)

  // Read it back
  const reader = Reader.from(batchBuffer)
  const batch = readRecordsBatch(reader)

  // Verify all the fields match
  strictEqual(batch.firstOffset, 0n, 'First offset should be 0')
  strictEqual(batch.partitionLeaderEpoch, 0, 'Partition leader epoch should be default 0')
  strictEqual(batch.magic, 2, 'Magic should be 2')
  strictEqual(batch.attributes, 0, 'Attributes should be 0')
  strictEqual(batch.lastOffsetDelta, 1, 'Last offset delta should be 1')
  strictEqual(batch.firstTimestamp, timestamp1, 'First timestamp should match')
  strictEqual(batch.maxTimestamp, timestamp2, 'Max timestamp should match')
  strictEqual(batch.producerId, 9876543210n, 'Producer ID should match')
  strictEqual(batch.producerEpoch, 21, 'Producer epoch should match')

  // Verify records
  strictEqual(batch.records.length, 2, 'Should have two records')

  // First record
  strictEqual(batch.records[0].offsetDelta, 0, 'First record offset delta should be 0')
  strictEqual(batch.records[0].key.toString(), 'roundtrip-key-1', 'First record key should match')
  strictEqual(batch.records[0].value.toString(), 'roundtrip-value-1', 'First record value should match')
  strictEqual(batch.records[0].headers.length, 1, 'First record should have one header')
  strictEqual(batch.records[0].headers[0][0].toString(), 'header1', 'First record header key should match')
  strictEqual(batch.records[0].headers[0][1].toString(), 'header-value-1', 'First record header value should match')

  // Second record
  strictEqual(batch.records[1].offsetDelta, 1, 'Second record offset delta should be 1')
  strictEqual(batch.records[1].key.toString(), 'roundtrip-key-2', 'Second record key should match')
  strictEqual(batch.records[1].value.toString(), 'roundtrip-value-2', 'Second record value should match')
  strictEqual(batch.records[1].headers.length, 1, 'Second record should have one header')
  strictEqual(batch.records[1].headers[0][0].toString(), 'header2', 'Second record header key should match')
  strictEqual(batch.records[1].headers[0][1].toString(), 'header-value-2', 'Second record header value should match')
})

// Tests with various compression algorithms
// Note: Skip these if optional compression libraries aren't available
for (const [compression, algorithm] of Object.entries<CompressionAlgorithm>(compressionsAlgorithms)) {
  test(
    `createRecordsBatch and readRecordsBatch with ${compression} compression`,
    { skip: !algorithm.available },
    () => {
      const messages: MessageRecord[] = [
        {
          key: Buffer.from('compress-key'),
          value: Buffer.from('compress-value'.repeat(10)),
          topic: 'compress-topic'
        }
      ]

      const options = {
        producerId: 0n,
        compression: compression as any
      }

      // Create a compressed batch
      const batchBuffer = createRecordsBatch(messages, options)

      // Read it back
      const reader = Reader.from(batchBuffer)
      const batch = readRecordsBatch(reader)

      // Verify compression was used and data is correct
      strictEqual(
        batch.attributes & 0b111,
        algorithm.bitmask,
        `Attributes should have ${compression} compression bit set`
      )
      strictEqual(batch.records.length, 1, 'Should have one record')
      strictEqual(batch.records[0].key.toString(), 'compress-key', 'Record key should match')
      strictEqual(batch.records[0].value.toString(), 'compress-value'.repeat(10), 'Record value should match')
    }
  )
}
