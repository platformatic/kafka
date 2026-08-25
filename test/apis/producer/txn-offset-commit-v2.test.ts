import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import * as txnOffsetCommitV2 from '../../../src/apis/producer/txn-offset-commit-v2.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('TxnOffsetCommit v2 serializes the committed leader epoch and parses its throttled response', () => {
  strictEqual(txnOffsetCommitV2.api.version, 2)
  const reader = Reader.from(
    txnOffsetCommitV2.createRequest('tx', 'group', 1n, 2, 3, 'member', null, [
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
              committedLeaderEpoch: r.readInt32(),
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
      [
        {
          name: 'topic',
          partitions: [{ partitionIndex: 0, committedOffset: 4n, committedLeaderEpoch: 5, committedMetadata: null }]
        }
      ]
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
  deepStrictEqual(txnOffsetCommitV2.parseResponse(1, 28, 2, Reader.from(response)), {
    throttleTimeMs: 12,
    topics: [{ name: 'topic', partitions: [{ partitionIndex: 0, errorCode: 0 }] }]
  })
})

test('TxnOffsetCommit v2 exposes partition protocol errors', () => {
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
  throws(() => txnOffsetCommitV2.parseResponse(1, 28, 2, Reader.from(response)), ResponseError)
})
