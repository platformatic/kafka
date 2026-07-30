import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { ListOffsetTimestamps } from '../../../src/apis/enumerations.ts'
import { listOffsetsV0, Reader, Writer } from '../../../src/index.ts'

test('ListOffsets exposes Kafka special timestamps', () => {
  deepStrictEqual(ListOffsetTimestamps, {
    LATEST: -1n,
    EARLIEST: -2n,
    MAX: -3n,
    EARLIEST_LOCAL: -4n,
    LATEST_TIERED: -5n
  })
})

test('ListOffsets v0 keeps the Consumer call shape and normalizes legacy offsets', () => {
  const request = listOffsetsV0.createRequest(-1, 1, [{ name: 'topic', partitions: [{ partitionIndex: 2, currentLeaderEpoch: 4, timestamp: ListOffsetTimestamps.LATEST }] }])
  const requestReader = Reader.from(request)
  deepStrictEqual(requestReader.readInt32(), -1)
  deepStrictEqual(requestReader.readArray(r => ({ name: r.readString(false), partitions: r.readArray(r => [r.readInt32(), r.readInt64(), r.readInt32()], false, false) }), false, false), [{ name: 'topic', partitions: [[2, ListOffsetTimestamps.LATEST, 1]] }])

  const response = listOffsetsV0.parseResponse(1, 2, 0, Reader.from(Writer.create().appendArray([{ name: 'topic' }], (w, topic) => w.appendString(topic.name, false).appendArray([{ partition: 2 }], (w, partition) => w.appendInt32(partition.partition).appendInt16(0).appendArray([7n, 8n], (w, offset) => w.appendInt64(offset), false, false), false, false), false, false)))
  deepStrictEqual(response, { throttleTimeMs: 0, topics: [{ name: 'topic', partitions: [{ partitionIndex: 2, errorCode: 0, timestamp: -1n, offset: 7n, leaderEpoch: -1 }] }] })
})
