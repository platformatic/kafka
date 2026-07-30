import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { alterPartitionV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

test('AlterPartition v0 ignores leader recovery state and normalizes its response', () => {
  const { api, createRequest, parseResponse } = alterPartitionV0
  const requestReader = Reader.from(createRequest(1, 2n, [{ topicName: 'topic', partitions: [{ partitionIndex: 0, leaderEpoch: 3, newIsrWithEpochs: [{ brokerId: 1, brokerEpoch: 5n }, { brokerId: 2, brokerEpoch: 6n }], leaderRecoveryState: 1, partitionEpoch: 4 }] }]))
  strictEqual(requestReader.readInt32(), 1)
  strictEqual(requestReader.readInt64(), 2n)
  deepStrictEqual(requestReader.readArray(reader => ({ topicName: reader.readString(), partitions: reader.readArray(reader => ({ partitionIndex: reader.readInt32(), leaderEpoch: reader.readInt32(), newIsr: reader.readArray(reader => reader.readInt32(), true, false), partitionEpoch: reader.readInt32() })) })), [{ topicName: 'topic', partitions: [{ partitionIndex: 0, leaderEpoch: 3, newIsr: [1, 2], partitionEpoch: 4 }] }])
  requestReader.readTaggedFields()
  strictEqual(requestReader.remaining, 0)
  deepStrictEqual({ key: api.key, version: api.version }, { key: 56, version: 0 })
  const responseReader = Reader.from(Writer.create().appendInt32(1).appendInt16(0).appendArray([{}], writer => writer.appendString('topic').appendArray([{}], writer => writer.appendInt32(0).appendInt16(0).appendInt32(1).appendInt32(3).appendArray([1, 2], (writer, id) => writer.appendInt32(id), true, false).appendInt32(4))).appendUnsignedVarInt(1).appendUnsignedVarInt(42).appendUnsignedVarInt(1).appendUnsignedInt8(0))
  deepStrictEqual(parseResponse(1, 56, 0, responseReader), { throttleTimeMs: 1, errorCode: 0, topics: [{ topicName: 'topic', topicId: '00000000-0000-0000-0000-000000000000', partitions: [{ partitionIndex: 0, errorCode: 0, leaderId: 1, leaderEpoch: 3, isr: [1, 2], leaderRecoveryState: 0, partitionEpoch: 4 }] }] })
  strictEqual(responseReader.remaining, 0)
  throws(() => parseResponse(1, 56, 0, Reader.from(Writer.create().appendInt32(0).appendInt16(15).appendArray([], () => {}).appendTaggedFields())), ResponseError)
})
