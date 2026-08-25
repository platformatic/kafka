import { strictEqual, throws } from 'node:assert'
import test from 'node:test'
import {
  consumerGroupHeartbeatV0,
  consumerGroupHeartbeatV1,
  fetchV12,
  fetchV13,
  fetchV14,
  fetchV15,
  fetchV16,
  fetchV17,
  heartbeatV4,
  joinGroupV6,
  joinGroupV7,
  joinGroupV8,
  joinGroupV9,
  leaveGroupV4,
  leaveGroupV5,
  listOffsetsV6,
  listOffsetsV7,
  listOffsetsV8,
  listOffsetsV9,
  offsetCommitV8,
  offsetCommitV9,
  offsetFetchV6,
  offsetFetchV7,
  offsetFetchV8,
  offsetFetchV9,
  offsetForLeaderEpochV4,
  Reader,
  ResponseError,
  syncGroupV4,
  syncGroupV5,
  Writer
} from '../../../src/index.ts'

function appendUnknownTag (writer: Writer): Writer {
  return writer.appendUnsignedVarInt(1).appendUnsignedVarInt(42).appendUnsignedVarInt(2).append(Buffer.from([1, 2]))
}

test('flexible consumer responses consume root tags before errors', async t => {
  for (const [version, api] of [
    [12, fetchV12], [13, fetchV13], [14, fetchV14], [15, fetchV15], [16, fetchV16], [17, fetchV17]
  ] as const) {
    await t.test(`Fetch v${version}`, () => {
      const reader = Reader.from(appendUnknownTag(Writer.create().appendInt32(0).appendInt16(1).appendInt32(0).appendArray([], () => {})))
      throws(() => api.parseResponse(1, 1, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [[6, joinGroupV6], [7, joinGroupV7], [8, joinGroupV8], [9, joinGroupV9]] as const) {
    await t.test(`JoinGroup v${version}`, () => {
      const writer = Writer.create().appendInt32(0).appendInt16(1).appendInt32(0)
      if (version === 6) {
        writer.appendString(null)
      } else {
        writer.appendString(null).appendString(null)
      }
      writer.appendString('')
      if (version === 9) {
        writer.appendBoolean(false)
      }
      writer.appendString('').appendArray([], () => {})
      const reader = Reader.from(appendUnknownTag(writer))
      throws(() => api.parseResponse(1, 11, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [[4, leaveGroupV4], [5, leaveGroupV5]] as const) {
    await t.test(`LeaveGroup v${version}`, () => {
      const reader = Reader.from(appendUnknownTag(Writer.create().appendInt32(0).appendInt16(1).appendArray([], () => {})))
      throws(() => api.parseResponse(1, 13, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [[4, syncGroupV4], [5, syncGroupV5]] as const) {
    await t.test(`SyncGroup v${version}`, () => {
      const writer = Writer.create().appendInt32(0).appendInt16(1)
      if (version === 5) {
        writer.appendString(null).appendString(null)
      }
      const reader = Reader.from(appendUnknownTag(writer.appendBytes(Buffer.alloc(0))))
      throws(() => api.parseResponse(1, 14, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const api of [heartbeatV4]) {
    const reader = Reader.from(appendUnknownTag(Writer.create().appendInt32(0).appendInt16(1)))
    throws(() => api.parseResponse(1, 12, 4, reader), ResponseError)
    strictEqual(reader.remaining, 0)
  }

  for (const [version, api] of [[0, consumerGroupHeartbeatV0], [1, consumerGroupHeartbeatV1]] as const) {
    await t.test(`ConsumerGroupHeartbeat v${version}`, () => {
      const reader = Reader.from(
        appendUnknownTag(Writer.create().appendInt32(0).appendInt16(1).appendString(null).appendString(null).appendInt32(0).appendInt32(0).appendInt8(-1))
      )
      throws(() => api.parseResponse(1, 68, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }
})

test('flexible consumer responses consume nested tags exactly once', () => {
  for (const [version, api] of [[6, listOffsetsV6], [7, listOffsetsV7], [8, listOffsetsV8], [9, listOffsetsV9]] as const) {
    const reader = Reader.from(
      appendUnknownTag(
        Writer.create().appendInt32(0).appendArray([{ name: 'topic' }], (writer, topic) => {
          writer.appendString(topic.name).appendArray([{ index: 0 }], writer => {
            writer.appendInt32(0).appendInt16(0).appendInt64(0n).appendInt64(0n).appendInt32(0)
            appendUnknownTag(writer)
          }, true, false)
          appendUnknownTag(writer)
        }, true, false)
      )
    )
    api.parseResponse(1, 2, version, reader)
    strictEqual(reader.remaining, 0)
  }
})

test('flexible offset responses consume root tags', async t => {
  for (const [version, api] of [[6, offsetFetchV6], [7, offsetFetchV7]] as const) {
    await t.test(`OffsetFetch v${version}`, () => {
      const reader = Reader.from(appendUnknownTag(Writer.create().appendInt32(0).appendArray([], () => {}).appendInt16(1)))
      throws(() => api.parseResponse(1, 9, version, reader), ResponseError)
      strictEqual(reader.remaining, 0)
    })
  }

  for (const [version, api] of [
    [8, offsetFetchV8], [9, offsetFetchV9], [8, offsetCommitV8], [9, offsetCommitV9], [6, listOffsetsV6],
    [7, listOffsetsV7], [8, listOffsetsV8], [9, listOffsetsV9], [4, offsetForLeaderEpochV4]
  ] as const) {
    await t.test(`API ${api.api.key} v${version}`, () => {
      const reader = Reader.from(appendUnknownTag(Writer.create().appendInt32(0).appendArray([], () => {})))
      api.parseResponse(1, api.api.key, version, reader)
      strictEqual(reader.remaining, 0)
    })
  }
})
