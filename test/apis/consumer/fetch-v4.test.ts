import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { createRecordsBatch, fetchV4, Reader, Writer } from '../../../src/index.ts'

test('Fetch v4 preserves the Consumer request arguments and normalizes its response', () => {
  const request = fetchV4.createRequest(100, 1, 1024, 0, 9, 2, [{ topicId: 'topic', partitions: [{ partition: 0, currentLeaderEpoch: -1, lastFetchedEpoch: -1, fetchOffset: 4n, partitionMaxBytes: 512 }] }], [{ topicId: 'forgotten-topic', partitions: [0] }], '')
  const requestReader = Reader.from(request)
  deepStrictEqual([requestReader.readInt32(), requestReader.readInt32(), requestReader.readInt32(), requestReader.readInt32(), requestReader.readInt8()], [-1, 100, 1, 1024, 0])
  deepStrictEqual(requestReader.readArray(r => ({ topic: r.readString(false), partitions: r.readArray(r => [r.readInt32(), r.readInt64(), r.readInt32()], false, false) }), false, false), [{ topic: 'topic', partitions: [[0, 4n, 512]] }])
  strictEqual(requestReader.remaining, 0)

  const response = fetchV4.parseResponse(1, 1, 4, Reader.from(Writer.create().appendInt32(3).appendArray([{ topic: 'topic' }], (w, topic) => w.appendString(topic.topic, false).appendArray([{ partition: 0 }], (w, partition) => w.appendInt32(partition.partition).appendInt16(0).appendInt64(10n).appendInt64(9n).appendArray([], () => {}, false, false).appendBytes(Buffer.alloc(0), false), false, false), false, false)))
  deepStrictEqual(response, { throttleTimeMs: 3, errorCode: 0, sessionId: 0, responses: [{ topicId: 'topic', partitions: [{ partitionIndex: 0, errorCode: 0, highWatermark: 10n, lastStableOffset: 9n, logStartOffset: -1n, abortedTransactions: [], preferredReadReplica: -1, records: [] }] }] })
})

test('Fetch v4 preserves nullable aborted transactions', () => {
  const response = fetchV4.parseResponse(1, 1, 4, Reader.from(Writer.create()
    .appendInt32(0)
    .appendArray([{ topic: 'topic' }], w => w.appendString('topic', false).appendArray([null, [], [{ producerId: 1n, firstOffset: 2n }]], (w, abortedTransactions, partition) => w
      .appendInt32(partition)
      .appendInt16(0)
      .appendInt64(10n)
      .appendInt64(9n)
      .appendArray(abortedTransactions, (w, transaction) => w.appendInt64(transaction.producerId).appendInt64(transaction.firstOffset), false, false)
      .appendBytes(Buffer.alloc(0), false), false, false), false, false)))

  deepStrictEqual(response.responses[0]!.partitions.map(partition => partition.abortedTransactions), [null, [], [{ producerId: 1n, firstOffset: 2n }]])
})

test('Fetch v4 preserves nullable records', () => {
  const batch = createRecordsBatch([{ topic: 'topic', value: Buffer.from('value') }])
  const records = [null, Buffer.alloc(0), batch.buffer]
  const recordsWire = Writer.create().appendBytes(records[0], false).appendBytes(records[1], false).appendBytes(records[2], false)
  const recordsReader = Reader.from(recordsWire)
  strictEqual(recordsReader.readInt32(), -1)
  strictEqual(recordsReader.readInt32(), 0)
  strictEqual(recordsReader.readInt32(), batch.length)

  const response = fetchV4.parseResponse(1, 1, 4, Reader.from(Writer.create().appendInt32(0).appendArray([{ topic: 'topic' }], w => w.appendString('topic', false).appendArray(records, (w, records, partition) => w
    .appendInt32(partition).appendInt16(0).appendInt64(10n).appendInt64(9n).appendArray([], () => {}, false, false).appendBytes(records, false), false, false), false, false)))

  deepStrictEqual(response.responses[0]!.partitions.map(partition => partition.records?.length === 1 ? partition.records[0].records[0].value?.toString() : partition.records), [null, [], 'value'])
})
