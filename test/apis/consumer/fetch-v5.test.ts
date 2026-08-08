import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { fetchV5, Reader, Writer } from '../../../src/index.ts'

test('Fetch v5 preserves the Consumer request arguments and parses record batches', () => {
  const request = fetchV5.createRequest(100, 1, 1024, 0, 9, 2, [{ topicId: 'topic', partitions: [{ partition: 0, currentLeaderEpoch: -1, lastFetchedEpoch: -1, fetchOffset: 4n, partitionMaxBytes: 512 }] }], [{ topicId: 'forgotten-topic', partitions: [0] }], '')
  const requestReader = Reader.from(request)
  deepStrictEqual([requestReader.readInt32(), requestReader.readInt32(), requestReader.readInt32(), requestReader.readInt32(), requestReader.readInt8()], [-1, 100, 1, 1024, 0])
  deepStrictEqual(requestReader.readArray(r => ({ topic: r.readString(false), partitions: r.readArray(r => [r.readInt32(), r.readInt64(), r.readInt64(), r.readInt32()], false, false) }), false, false), [{ topic: 'topic', partitions: [[0, 4n, -1n, 512]] }])
  strictEqual(requestReader.remaining, 0)

  const response = fetchV5.parseResponse(1, 1, 5, Reader.from(Writer.create().appendInt32(3).appendArray([{ topic: 'topic' }], (w, topic) => w.appendString(topic.topic, false).appendArray([{ partition: 0 }], (w, partition) => w.appendInt32(partition.partition).appendInt16(0).appendInt64(10n).appendInt64(9n).appendInt64(2n).appendArray([], () => {}, false, false).appendBytes(Buffer.alloc(0), false), false, false), false, false)))
  deepStrictEqual(response, { throttleTimeMs: 3, errorCode: 0, sessionId: 0, responses: [{ topicId: 'topic', partitions: [{ partitionIndex: 0, errorCode: 0, highWatermark: 10n, lastStableOffset: 9n, logStartOffset: 2n, abortedTransactions: [], preferredReadReplica: -1, records: [] }] }] })
})
