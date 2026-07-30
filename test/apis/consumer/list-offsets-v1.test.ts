import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { ListOffsetTimestamps } from '../../../src/apis/enumerations.ts'
import { listOffsetsV1, Reader, Writer } from '../../../src/index.ts'

test('ListOffsets v1 omits legacy max_num_offsets and returns one offset', () => {
  const request = listOffsetsV1.createRequest(-1, 1, [{ name: 'topic', partitions: [{ partitionIndex: 2, currentLeaderEpoch: 4, timestamp: ListOffsetTimestamps.LATEST }] }])
  const requestReader = Reader.from(request)
  deepStrictEqual(requestReader.readInt32(), -1)
  deepStrictEqual(requestReader.readArray(r => ({ name: r.readString(false), partitions: r.readArray(r => [r.readInt32(), r.readInt64()], false, false) }), false, false), [{ name: 'topic', partitions: [[2, ListOffsetTimestamps.LATEST]] }])

  const response = listOffsetsV1.parseResponse(1, 2, 1, Reader.from(Writer.create().appendArray([{ name: 'topic' }], (w, topic) => w.appendString(topic.name, false).appendArray([{ partition: 2 }], (w, partition) => w.appendInt32(partition.partition).appendInt16(0).appendInt64(6n).appendInt64(7n), false, false), false, false)))
  deepStrictEqual(response, { throttleTimeMs: 0, topics: [{ name: 'topic', partitions: [{ partitionIndex: 2, errorCode: 0, timestamp: 6n, offset: 7n, leaderEpoch: -1 }] }] })
})
