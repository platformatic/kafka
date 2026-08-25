import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { describeQuorumV0, Reader, ResponseError, Writer } from '../../../src/index.ts'
import type { DescribeQuorumRequestTopic, DescribeQuorumResponse } from '../../../src/apis/admin/describe-quorum-v2.ts'
import type {
  DescribeQuorumRequestTopic as DescribeQuorumRequestTopicV0,
  DescribeQuorumResponse as DescribeQuorumResponseV0
} from '../../../src/apis/admin/describe-quorum-v0.ts'

const request: DescribeQuorumRequestTopic = {} as DescribeQuorumRequestTopicV0
const response: DescribeQuorumResponse = {} as DescribeQuorumResponseV0
strictEqual(typeof request, 'object')
strictEqual(typeof response, 'object')

test('DescribeQuorum v0 serializes its legacy request and parses legacy voters', () => {
  const { api, createRequest, parseResponse } = describeQuorumV0
  const requestReader = Reader.from(createRequest([{ topicName: 'topic', partitions: [{ partitionIndex: 2 }] }]))
  deepStrictEqual(requestReader.readArray(reader => ({
    topicName: reader.readString(),
    partitions: reader.readArray(reader => ({ partitionIndex: reader.readInt32() }))
  })), [{ topicName: 'topic', partitions: [{ partitionIndex: 2 }] }])
  requestReader.readTaggedFields()
  strictEqual(requestReader.remaining, 0)
  deepStrictEqual({ key: api.key, version: api.version }, { key: 55, version: 0 })

  const responseReader = Reader.from(
    Writer.create().appendInt16(0).appendArray([{ topicName: 'topic' }], writer => {
      writer.appendString('topic').appendArray([{ partitionIndex: 2 }], writer => {
        writer.appendInt32(2).appendInt16(0).appendInt32(1).appendInt32(4).appendInt64(10n)
          .appendArray([{ replicaId: 1, logEndOffset: 10n }], (writer, voter) => writer.appendInt32(voter.replicaId).appendInt64(voter.logEndOffset))
          .appendArray([], () => {})
      })
    }).appendUnsignedVarInt(1).appendUnsignedVarInt(42).appendUnsignedVarInt(1).appendUnsignedInt8(0)
  )
  deepStrictEqual(parseResponse(1, 55, 0, responseReader), {
    errorCode: 0,
    errorMessage: null,
    topics: [{ topicName: 'topic', partitions: [{ partitionIndex: 2, errorCode: 0, errorMessage: null, leaderId: 1, leaderEpoch: 4, highWatermark: 10n, currentVoters: [{ replicaId: 1, replicaDirectoryId: '00000000-0000-0000-0000-000000000000', logEndOffset: 10n, lastFetchTimestamp: -1n, lastCaughtUpTimestamp: -1n }], observers: [] }] }],
    nodes: []
  })
  strictEqual(responseReader.remaining, 0)

  const errorReader = Reader.from(Writer.create().appendInt16(15).appendArray([], () => {}).appendTaggedFields())
  throws(() => parseResponse(1, 55, 0, errorReader), ResponseError)
})
