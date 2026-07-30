import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import {
  addPartitionsToTxnV3,
  addPartitionsToTxnV4,
  addPartitionsToTxnV5,
  addOffsetsToTxnV3,
  addOffsetsToTxnV4,
  endTxnV3,
  endTxnV4,
  initProducerIdV3,
  initProducerIdV4,
  initProducerIdV5,
  produceV9,
  produceV10,
  produceV11,
  Reader,
  ResponseError,
  txnOffsetCommitV3,
  txnOffsetCommitV4,
  Writer
} from '../../../src/index.ts'

function appendUnknownRootTag (writer: Writer): Writer {
  return writer.appendUnsignedVarInt(1).appendUnsignedVarInt(42).appendUnsignedVarInt(2).append(Buffer.from([1, 2]))
}

function appendUnknownTag (writer: Writer): Writer {
  return appendUnknownRootTag(writer)
}

function appendTag (writer: Writer, tag: number, payload: Writer): Writer {
  return writer.appendUnsignedVarInt(1).appendUnsignedVarInt(tag).appendUnsignedVarInt(payload.length).appendFrom(payload)
}

test('Produce v10-v11 parse known current leader and node endpoints tags', async t => {
  for (const [version, api] of [
    [10, produceV10],
    [11, produceV11]
  ] as const) {
    await t.test(`Produce v${version}`, () => {
      const currentLeader = Writer.create().appendInt32(1).appendInt32(7)
      const nodeEndpoints = Writer.create().appendArray([0], w => {
        w.appendInt32(1).appendString('broker').appendInt32(9092).appendString(null).appendTaggedFields()
      }, true, false)
      const writer = Writer.create().appendArray([0], w => {
        w.appendString('topic').appendArray([0], w => {
          w.appendInt32(0)
            .appendInt16(0)
            .appendInt64(0n)
            .appendInt64(0n)
            .appendInt64(0n)
            .appendArray([], () => {}, true, false)
            .appendString(null)
          appendTag(w, 0, currentLeader)
        }, true, false)
        w.appendTaggedFields()
      }, true, false).appendInt32(0)
      appendTag(writer, 0, nodeEndpoints)

      const response = api.parseResponse(1, 0, version, Reader.from(writer))
      deepStrictEqual(response.responses[0].partitionResponses[0].currentLeader, { leaderId: 1, leaderEpoch: 7 })
      deepStrictEqual(response.nodeEndpoints, [{ nodeId: 1, host: 'broker', port: 9092, rack: null }])
    })
  }
})

test('flexible producer responses consume unknown root tags before errors', async t => {
  for (const [name, version, api] of [
    ['EndTxn', 3, endTxnV3],
    ['EndTxn', 4, endTxnV4],
    ['AddOffsetsToTxn', 3, addOffsetsToTxnV3],
    ['AddOffsetsToTxn', 4, addOffsetsToTxnV4]
  ] as const) {
    await t.test(`${name} v${version}`, () => {
      const reader = Reader.from(appendUnknownRootTag(Writer.create().appendInt32(0).appendInt16(1)))
      throws(() => api.parseResponse(1, api.api.key, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [
    [3, initProducerIdV3],
    [4, initProducerIdV4],
    [5, initProducerIdV5]
  ] as const) {
    await t.test(`InitProducerId v${version}`, () => {
      const reader = Reader.from(appendUnknownRootTag(Writer.create().appendInt32(0).appendInt16(1).appendInt64(-1n).appendInt16(-1)))
      throws(() => api.parseResponse(1, 22, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [
    [3, addPartitionsToTxnV3],
    [4, addPartitionsToTxnV4],
    [5, addPartitionsToTxnV5]
  ] as const) {
    await t.test(`AddPartitionsToTxn v${version}`, () => {
      const writer = Writer.create().appendInt32(0)
      if (version >= 4) {
        writer.appendInt16(1).appendArray([], () => {})
      } else {
        writer.appendArray([{ name: 'topic', partitions: [0] }], (w, topic) => {
          w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => w.appendInt32(partition).appendInt16(1))
        })
      }
      const reader = Reader.from(appendUnknownRootTag(writer))
      throws(() => api.parseResponse(1, 24, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [
    [3, txnOffsetCommitV3],
    [4, txnOffsetCommitV4]
  ] as const) {
    await t.test(`TxnOffsetCommit v${version}`, () => {
      const writer = Writer.create().appendInt32(0).appendArray([{ name: 'topic', partitions: [0] }], (w, topic) => {
        w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => w.appendInt32(partition).appendInt16(1))
      })
      const reader = Reader.from(appendUnknownRootTag(writer))
      throws(() => api.parseResponse(1, 28, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }
})

test('flexible producer responses consume unknown nested tags before errors', async t => {
  for (const [version, api] of [
    [9, produceV9],
    [10, produceV10],
    [11, produceV11]
  ] as const) {
    await t.test(`Produce v${version}`, () => {
      const writer = Writer.create().appendArray([{ name: 'topic' }], w => {
        w.appendString('topic').appendArray([0], w => {
          w.appendInt32(0)
            .appendInt16(0)
            .appendInt64(0n)
            .appendInt64(0n)
            .appendInt64(0n)
            .appendArray([0], w => {
              w.appendInt32(0).appendString('record error')
              appendUnknownTag(w)
            }, true, false)
          w.appendString(null)
          appendUnknownTag(w)
        }, true, false)
        appendUnknownTag(w)
      }, true, false).appendInt32(0)
      appendUnknownTag(writer)

      const reader = Reader.from(writer)
      throws(() => api.parseResponse(1, 0, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [
    [3, addPartitionsToTxnV3],
    [4, addPartitionsToTxnV4],
    [5, addPartitionsToTxnV5]
  ] as const) {
    await t.test(`AddPartitionsToTxn v${version}`, () => {
      const writer = Writer.create().appendInt32(0)
      if (version === 3) {
        writer.appendArray([0], w => {
          w.appendString('topic').appendArray([0], w => {
            w.appendInt32(0).appendInt16(1)
            appendUnknownTag(w)
          }, true, false)
          appendUnknownTag(w)
        }, true, false)
      } else {
        writer.appendInt16(0).appendArray([0], w => {
          w.appendString('transaction').appendArray([0], w => {
            w.appendString('topic').appendArray([0], w => {
              w.appendInt32(0).appendInt16(1)
              appendUnknownTag(w)
            }, true, false)
            appendUnknownTag(w)
          }, true, false)
          appendUnknownTag(w)
        }, true, false)
      }
      appendUnknownTag(writer)

      const reader = Reader.from(writer)
      throws(() => api.parseResponse(1, 24, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [
    [3, txnOffsetCommitV3],
    [4, txnOffsetCommitV4]
  ] as const) {
    await t.test(`TxnOffsetCommit v${version}`, () => {
      const writer = Writer.create().appendInt32(0).appendArray([0], w => {
        w.appendString('topic').appendArray([0], w => {
          w.appendInt32(0).appendInt16(1)
          appendUnknownTag(w)
        }, true, false)
        appendUnknownTag(w)
      }, true, false)
      appendUnknownTag(writer)

      const reader = Reader.from(writer)
      throws(() => api.parseResponse(1, 28, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }
})
