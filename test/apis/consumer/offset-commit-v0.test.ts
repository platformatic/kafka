import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import * as codec from '../../../src/apis/consumer/offset-commit-v0.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'
test('OffsetCommit v0 ignores generation and member arguments from the normalized client shape', () => {
  const request = Reader.from(
    codec.createRequest('group', 1, 'member', null, [
      {
        name: 'topic',
        partitions: [{ partitionIndex: 0, committedOffset: 2n, committedLeaderEpoch: -1, committedMetadata: null }]
      }
    ])
  )
  deepStrictEqual(request.readString(false), 'group')
  deepStrictEqual(request.readArray(r => ({
    name: r.readString(false),
    partitions: r.readArray(r => ({
      partitionIndex: r.readInt32(),
      committedOffset: r.readInt64(),
      committedMetadata: r.readString(false)
    }), false, false)
  }), false, false), [{
    name: 'topic',
    partitions: [{ partitionIndex: 0, committedOffset: 2n, committedMetadata: '' }]
  }])
  deepStrictEqual(
    codec.parseResponse(
      1,
      8,
      0,
      Reader.from(
        Writer.create().appendArray(
          [{ name: 'topic', partitions: [0] }],
          (w, topic) =>
            w
              .appendString(topic.name, false)
              .appendArray(topic.partitions, (w, partition) => w.appendInt32(partition).appendInt16(0), false),
          false
        )
      )
    ),
    { throttleTimeMs: 0, topics: [{ name: 'topic', partitions: [{ partitionIndex: 0, errorCode: 0 }] }] }
  )
})
