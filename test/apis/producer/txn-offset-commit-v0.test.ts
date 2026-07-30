import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import * as txnOffsetCommitV0 from '../../../src/apis/producer/txn-offset-commit-v0.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('TxnOffsetCommit v0 serializes the complete legacy request and parses its response', () => {
  strictEqual(txnOffsetCommitV0.api.version, 0)
  const reader = Reader.from(
    txnOffsetCommitV0.createRequest('tx', 'group', 1n, 2, 3, 'member', null, [
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

  deepStrictEqual(
    txnOffsetCommitV0.parseResponse(
      1,
      28,
      0,
      Reader.from(
        Writer.create().appendInt32(12).appendArray(
          [{ name: 'topic', partitions: [0] }],
          (w, topic) =>
            w
              .appendString(topic.name, false)
              .appendArray(topic.partitions, (w, partition) => w.appendInt32(partition).appendInt16(0), false, false),
          false,
          false
        )
      )
    ),
    {
      throttleTimeMs: 12,
      topics: [{ name: 'topic', partitions: [{ partitionIndex: 0, errorCode: 0 }] }]
    }
  )
})

test('TxnOffsetCommit v0 exposes partition protocol errors', () => {
  const response = Writer.create().appendInt32(0).appendArray(
    [{ name: 'topic', partitions: [0] }],
    (w, topic) =>
      w
        .appendString(topic.name, false)
        .appendArray(topic.partitions, (w, partition) => w.appendInt32(partition).appendInt16(25), false, false),
    false,
    false
  )
  throws(() => txnOffsetCommitV0.parseResponse(1, 28, 0, Reader.from(response)), ResponseError)
})
