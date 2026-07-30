import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { describeQuorumV1, Reader, ResponseError, Writer } from '../../../src/index.ts'
import type { DescribeQuorumRequestTopic, DescribeQuorumResponse } from '../../../src/apis/admin/describe-quorum-v2.ts'
import type {
  DescribeQuorumRequestTopic as DescribeQuorumRequestTopicV1,
  DescribeQuorumResponse as DescribeQuorumResponseV1
} from '../../../src/apis/admin/describe-quorum-v1.ts'

const request: DescribeQuorumRequestTopic = {} as DescribeQuorumRequestTopicV1
const response: DescribeQuorumResponse = {} as DescribeQuorumResponseV1
strictEqual(typeof request, 'object')
strictEqual(typeof response, 'object')

test('DescribeQuorum v1 serializes its request and parses voter timestamps', () => {
  const { api, createRequest, parseResponse } = describeQuorumV1
  const requestReader = Reader.from(createRequest([{ topicName: 'topic', partitions: [{ partitionIndex: 2 }] }]))
  deepStrictEqual(requestReader.readArray(reader => ({ topicName: reader.readString(), partitions: reader.readArray(reader => ({ partitionIndex: reader.readInt32() })) })), [{ topicName: 'topic', partitions: [{ partitionIndex: 2 }] }])
  requestReader.readTaggedFields()
  strictEqual(requestReader.remaining, 0)
  deepStrictEqual({ key: api.key, version: api.version }, { key: 55, version: 1 })

  const responseReader = Reader.from(
    Writer.create().appendInt16(0).appendArray([{ topicName: 'topic' }], writer => {
      writer.appendString('topic').appendArray([{}], writer => {
        writer.appendInt32(2).appendInt16(0).appendInt32(1).appendInt32(4).appendInt64(10n)
          .appendArray([{ replicaId: 1, logEndOffset: 10n, lastFetchTimestamp: 8n, lastCaughtUpTimestamp: 9n }], (writer, voter) => writer.appendInt32(voter.replicaId).appendInt64(voter.logEndOffset).appendInt64(voter.lastFetchTimestamp).appendInt64(voter.lastCaughtUpTimestamp))
          .appendArray([], () => {})
      })
    }).appendUnsignedVarInt(1).appendUnsignedVarInt(42).appendUnsignedVarInt(1).appendUnsignedInt8(0)
  )
  deepStrictEqual(parseResponse(1, 55, 1, responseReader), {
    errorCode: 0,
    errorMessage: null,
    topics: [{ topicName: 'topic', partitions: [{ partitionIndex: 2, errorCode: 0, errorMessage: null, leaderId: 1, leaderEpoch: 4, highWatermark: 10n, currentVoters: [{ replicaId: 1, replicaDirectoryId: '00000000-0000-0000-0000-000000000000', logEndOffset: 10n, lastFetchTimestamp: 8n, lastCaughtUpTimestamp: 9n }], observers: [] }] }],
    nodes: []
  })
  strictEqual(responseReader.remaining, 0)
  throws(() => parseResponse(1, 55, 1, Reader.from(Writer.create().appendInt16(15).appendArray([], () => {}).appendTaggedFields())), ResponseError)
})
