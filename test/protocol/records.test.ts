import BufferList from 'bl'
import { deepStrictEqual, throws } from 'node:assert'
import test from 'node:test'
import { UnsupportedCompressionError } from '../../src/errors.ts'
import { Reader } from '../../src/protocol/reader.ts'
import {
  createRecord,
  createRecordsBatch,
  readRecord,
  readRecordsBatch,
  type KafkaRecord,
  type KafkaRecordsBatch,
  type Message
} from '../../src/protocol/records.ts'

test('createRecord creates a valid record', () => {
  const testMessage: Message = {
    key: 'test-key',
    value: 'test-value',
    headers: {
      'header1': 'value1',
      'header2': 'value2'
    },
    topic: 'test-topic',
    partition: 0,
    timestamp: BigInt(1000)
  }

  const offsetDelta = 5
  const firstTimestamp = BigInt(500)

  const recordBuffer = createRecord(testMessage, offsetDelta, firstTimestamp)
  const reader = new Reader(recordBuffer)

  const length = reader.readVarInt()
  deepStrictEqual(typeof length, 'number')

  const attributes = reader.readInt8()
  deepStrictEqual(attributes, 0)

  const timestampDelta = reader.readVarInt64()
  deepStrictEqual(timestampDelta, BigInt(500)) // 1000 - 500

  const readOffsetDelta = reader.readVarInt()
  deepStrictEqual(readOffsetDelta, offsetDelta)

  const key = reader.readVarIntBytes()
  deepStrictEqual(key.toString(), 'test-key')

  const value = reader.readVarIntBytes()
  deepStrictEqual(value.toString(), 'test-value')

  const headers = reader.readVarIntArray(r => [r.readVarIntBytes(), r.readVarIntBytes()])
  deepStrictEqual(headers.length, 2)
  deepStrictEqual(headers[0][0].toString(), 'header1')
  deepStrictEqual(headers[0][1].toString(), 'value1')
  deepStrictEqual(headers[1][0].toString(), 'header2')
  deepStrictEqual(headers[1][1].toString(), 'value2')
})

test('createRecord handles default timestamp', () => {
  const testMessage: Message = {
    key: 'test-key',
    value: 'test-value',
    headers: {},
    topic: 'test-topic'
  }

  const offsetDelta = 0
  const firstTimestamp = BigInt(Date.now())

  const recordBuffer = createRecord(testMessage, offsetDelta, firstTimestamp)
  const reader = new Reader(recordBuffer)

  reader.readVarInt() // length
  reader.readInt8() // attributes
  
  const timestampDelta = reader.readVarInt64()
  // Timestamp delta should be a small value since we just created the timestamp
  deepStrictEqual(timestampDelta < BigInt(1000), true)
})

test('createRecord handles binary data in key and value', () => {
  const testKey = Buffer.from([1, 2, 3, 4])
  const testValue = Buffer.from([5, 6, 7, 8])
  
  const testMessage: Message = {
    key: testKey,
    value: testValue,
    topic: 'test-topic'
  }

  const offsetDelta = 0
  const firstTimestamp = BigInt(1000)

  const recordBuffer = createRecord(testMessage, offsetDelta, firstTimestamp)
  const reader = new Reader(recordBuffer)

  reader.readVarInt() // length
  reader.readInt8() // attributes
  reader.readVarInt64() // timestampDelta
  reader.readVarInt() // offsetDelta

  const key = reader.readVarIntBytes()
  deepStrictEqual(key, testKey)

  const value = reader.readVarIntBytes()
  deepStrictEqual(value, testValue)
})

test('readRecord correctly parses a record', () => {
  // Create a record and then read it back
  const testMessage: Message = {
    key: 'test-key',
    value: 'test-value',
    headers: {
      'header1': 'value1',
      'header2': 'value2'
    },
    topic: 'test-topic',
    timestamp: BigInt(1000)
  }

  const offsetDelta = 5
  const firstTimestamp = BigInt(500)

  const recordBuffer = createRecord(testMessage, offsetDelta, firstTimestamp)
  const reader = new Reader(recordBuffer)

  const record = readRecord(reader)

  deepStrictEqual(record.attributes, 0)
  deepStrictEqual(record.timestampDelta, BigInt(500))
  deepStrictEqual(record.offsetDelta, offsetDelta)
  deepStrictEqual(record.key.toString(), 'test-key')
  deepStrictEqual(record.value.toString(), 'test-value')
  
  // Headers are returned as an object, not an array
  deepStrictEqual(Object.keys(record.headers).length, 2)
  deepStrictEqual(record.headers['header1'].toString(), 'value1')
  deepStrictEqual(record.headers['header2'].toString(), 'value2')
})

test('createRecordsBatch creates a valid batch with multiple records', () => {
  const messages: Message[] = [
    {
      key: 'key1',
      value: 'value1',
      topic: 'test-topic',
      partition: 0,
      timestamp: BigInt(1000)
    },
    {
      key: 'key2',
      value: 'value2',
      topic: 'test-topic',
      partition: 0,
      timestamp: BigInt(1500)
    }
  ]

  const batchBuffer = createRecordsBatch(messages, {
    compression: 'none',
    producerId: 42n,
    producerEpoch: 1,
    sequences: { 'test-topic:0': 100 }
  })

  deepStrictEqual(batchBuffer instanceof BufferList, true)
})

test('createRecordsBatch throws error on unsupported compression', () => {
  const messages: Message[] = [
    {
      key: 'key1',
      value: 'value1',
      topic: 'test-topic',
      partition: 0
    }
  ]

  throws(() => {
    createRecordsBatch(messages, {
      compression: 'unsupported' as any
    })
  }, (err: any) => {
    return err instanceof UnsupportedCompressionError &&
      err.message.includes('Unsupported compression algorithm unsupported')
  })
})

test('createRecordsBatch handles transactional messages', () => {
  const messages: Message[] = [
    {
      key: 'key1',
      value: 'value1',
      topic: 'test-topic',
      partition: 0
    }
  ]

  const batchBuffer = createRecordsBatch(messages, {
    transactionalId: 'transaction-1',
    compression: 'none',
    producerId: 42n,
    producerEpoch: 1
  })

  // Read the batch to verify transactional flag is set
  const reader = new Reader(batchBuffer)
  reader.readInt64() // firstOffset
  reader.readInt32() // length
  reader.readInt32() // partitionLeaderEpoch
  reader.readInt8() // magic
  reader.readUnsignedInt32() // crc
  
  const attributes = reader.readInt16()
  // Check if bit 4 (IS_TRANSACTIONAL) is set
  deepStrictEqual(Boolean(attributes & 0b10000), true)
})

test('readRecordsBatch parses a record batch correctly', () => {
  const messages: Message[] = [
    {
      key: 'key1',
      value: 'value1',
      topic: 'test-topic',
      partition: 0,
      timestamp: BigInt(1000)
    },
    {
      key: 'key2',
      value: 'value2',
      topic: 'test-topic',
      partition: 0,
      timestamp: BigInt(1500)
    }
  ]

  const batchBuffer = createRecordsBatch(messages, {
    compression: 'none',
    producerId: 42n,
    producerEpoch: 1,
    partitionLeaderEpoch: 0
  })

  const reader = new Reader(batchBuffer)
  const batch = readRecordsBatch(reader)

  deepStrictEqual(batch.firstOffset, 0n)
  deepStrictEqual(batch.producerId, 42n)
  deepStrictEqual(batch.producerEpoch, 1)
  deepStrictEqual(batch.records.length, 2)
  
  deepStrictEqual(batch.records[0].key.toString(), 'key1')
  deepStrictEqual(batch.records[0].value.toString(), 'value1')
  
  deepStrictEqual(batch.records[1].key.toString(), 'key2')
  deepStrictEqual(batch.records[1].value.toString(), 'value2')
})