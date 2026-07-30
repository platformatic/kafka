import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import * as txnOffsetCommitV1 from '../../../src/apis/producer/txn-offset-commit-v1.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('TxnOffsetCommit v1 serializes the pre-leader-epoch request and parses its throttled response', () => {
  strictEqual(txnOffsetCommitV1.api.version, 1)
  const reader = Reader.from(
    txnOffsetCommitV1.createRequest('tx', 'group', 1n, 2, 3, 'member', null, [
      {
        name: 'topic',
        partitions: [{ partitionIndex: 0, committedOffset: 4n, committedLeaderEpoch: 5, committedMetadata: null }]
      }
    ])
  )

  deepStrictEqual(
    [
      reader.readString(false),
      reader.readString(false),
      reader.readInt64(),
      reader.readInt16(),
      reader.readArray(
        r => ({
          name: r.readString(false),
          partitions: r.readArray(
            r => ({
              partitionIndex: r.readInt32(),
              committedOffset: r.readInt64(),
              committedMetadata: r.readNullableString(false)
            }),
            false,
            false
          )
        }),
        false,
        false
      )
    ],
    [
      'tx',
      'group',
      1n,
      2,
      [{ name: 'topic', partitions: [{ partitionIndex: 0, committedOffset: 4n, committedMetadata: null }] }]
    ]
  )
  strictEqual(reader.remaining, 0)

  const response = Writer.create()
    .appendInt32(12)
    .appendArray(
      [{ name: 'topic', partitions: [0] }],
      (w, topic) =>
        w
          .appendString(topic.name, false)
          .appendArray(topic.partitions, (w, partition) => w.appendInt32(partition).appendInt16(0), false, false),
      false,
      false
    )
  deepStrictEqual(txnOffsetCommitV1.parseResponse(1, 28, 1, Reader.from(response)), {
    throttleTimeMs: 12,
    topics: [{ name: 'topic', partitions: [{ partitionIndex: 0, errorCode: 0 }] }]
  })
})

test('TxnOffsetCommit v1 exposes partition protocol errors', () => {
  const response = Writer.create()
    .appendInt32(0)
    .appendArray(
      [{ name: 'topic', partitions: [0] }],
      (w, topic) =>
        w
          .appendString(topic.name, false)
          .appendArray(topic.partitions, (w, partition) => w.appendInt32(partition).appendInt16(25), false, false),
      false,
      false
    )
  throws(() => txnOffsetCommitV1.parseResponse(1, 28, 1, Reader.from(response)), ResponseError)
})
