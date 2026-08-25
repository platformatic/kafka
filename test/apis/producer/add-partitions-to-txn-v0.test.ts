import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import * as addPartitionsToTxnV0 from '../../../src/apis/producer/add-partitions-to-txn-v0.ts'
import { ResponseError } from '../../../src/errors.ts'
import { Reader } from '../../../src/protocol/reader.ts'
import { Writer } from '../../../src/protocol/writer.ts'

test('AddPartitionsToTxn v0 serializes and normalizes the legacy schema', () => {
  strictEqual(addPartitionsToTxnV0.api.version, 0)
  const reader = Reader.from(
    addPartitionsToTxnV0.createRequest([
      {
        transactionalId: 'tx',
        producerId: 1n,
        producerEpoch: 2,
        verifyOnly: true,
        topics: [{ name: 'topic', partitions: [0, 1] }]
      },
      {
        transactionalId: 'ignored',
        producerId: 3n,
        producerEpoch: 4,
        verifyOnly: false,
        topics: []
      }
    ])
  )
  deepStrictEqual(
    [
      reader.readString(false),
      reader.readInt64(),
      reader.readInt16(),
      reader.readArray(
        r => ({ name: r.readString(false), partitions: r.readArray(r => r.readInt32(), false, false) }),
        false,
        false
      )
    ],
    ['tx', 1n, 2, [{ name: 'topic', partitions: [0, 1] }]]
  )
  deepStrictEqual(
    addPartitionsToTxnV0.parseResponse(
      1,
      24,
      0,
      Reader.from(
        Writer.create()
          .appendInt32(0)
          .appendArray(
            [{ name: 'topic', partitions: [0] }],
            (w, topic) =>
              w
                .appendString(topic.name, false)
                .appendArray(topic.partitions, (w, partition) => w.appendInt32(partition).appendInt16(0), false),
            false
          )
      )
    ),
    {
      throttleTimeMs: 0,
      errorCode: 0,
      resultsByTransaction: [
        {
          transactionalId: '',
          topicResults: [{ name: 'topic', resultsByPartition: [{ partitionIndex: 0, partitionErrorCode: 0 }] }]
        }
      ]
    }
  )
})

test('AddPartitionsToTxn v0 exposes partition protocol errors', () => {
  const response = Writer.create()
    .appendInt32(0)
    .appendArray(
      [{ name: 'topic', partitions: [0] }],
      (w, topic) =>
        w
          .appendString(topic.name, false)
          .appendArray(topic.partitions, (w, partition) => w.appendInt32(partition).appendInt16(6), false),
      false
    )
  throws(() => addPartitionsToTxnV0.parseResponse(1, 24, 0, Reader.from(response)), ResponseError)
})
