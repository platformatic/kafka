import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import type {
  DescribeQuorumRequestPartition as DescribeQuorumRequestPartitionV2,
  DescribeQuorumRequestTopic as DescribeQuorumRequestTopicV2,
  DescribeQuorumResponse as DescribeQuorumResponseV2,
  DescribeQuorumResponsePartition as DescribeQuorumResponsePartitionV2,
  DescribeQuorumResponseTopic as DescribeQuorumResponseTopicV2,
  DescribeQuorumResponseVoter as DescribeQuorumResponseVoterV2
} from './describe-quorum-v2.ts'

const zeroUuid = '00000000-0000-0000-0000-000000000000'

export type DescribeQuorumRequestPartition = DescribeQuorumRequestPartitionV2
export type DescribeQuorumRequestTopic = DescribeQuorumRequestTopicV2

export type DescribeQuorumRequest = Parameters<typeof createRequest>

export type DescribeQuorumResponseVoter = DescribeQuorumResponseVoterV2
export type DescribeQuorumResponsePartition = DescribeQuorumResponsePartitionV2
export type DescribeQuorumResponseTopic = DescribeQuorumResponseTopicV2
export type DescribeQuorumResponse = DescribeQuorumResponseV2

/*
  DescribeQuorum Request (Version: 0) => [topics] TAG_BUFFER
    topics => topic_name [partitions] TAG_BUFFER
      topic_name => COMPACT_STRING
      partitions => partition_index TAG_BUFFER
        partition_index => INT32
*/
export function createRequest (topics: DescribeQuorumRequestTopic[]): Writer {
  return Writer.create().appendArray(topics, (writer, topic) => {
    writer.appendString(topic.topicName).appendArray(topic.partitions, (writer, partition) => {
      writer.appendInt32(partition.partitionIndex)
    })
  }).appendTaggedFields()
}

/*
  DescribeQuorum Response (Version: 0) => error_code [topics] TAG_BUFFER
    error_code => INT16
    topics => topic_name [partitions] TAG_BUFFER
      topic_name => COMPACT_STRING
      partitions => partition_index error_code leader_id leader_epoch high_watermark [current_voters] [observers] TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        leader_id => INT32
        leader_epoch => INT32
        high_watermark => INT64
        current_voters => replica_id log_end_offset TAG_BUFFER
          replica_id => INT32
          log_end_offset => INT64
        observers => replica_id log_end_offset TAG_BUFFER
          replica_id => INT32
          log_end_offset => INT64
*/
export function parseResponse (_correlationId: number, apiKey: number, apiVersion: number, reader: Reader): DescribeQuorumResponse {
  const errors: ResponseErrorWithLocation[] = []
  const errorCode = reader.readInt16()

  if (errorCode !== 0) {
    errors.push(['', [errorCode, null]])
  }

  const response: DescribeQuorumResponse = {
    errorCode,
    errorMessage: null,
    topics: reader.readArray((reader, topicIndex) => ({
      topicName: reader.readString(),
      partitions: reader.readArray((reader, partitionIndex) => {
        const partition = {
          partitionIndex: reader.readInt32(),
          errorCode: reader.readInt16(),
          errorMessage: null,
          leaderId: reader.readInt32(),
          leaderEpoch: reader.readInt32(),
          highWatermark: reader.readInt64(),
          currentVoters: reader.readArray(reader => ({
            replicaId: reader.readInt32(),
            replicaDirectoryId: zeroUuid,
            logEndOffset: reader.readInt64(),
            lastFetchTimestamp: -1n,
            lastCaughtUpTimestamp: -1n
          })),
          observers: reader.readArray(reader => ({
            replicaId: reader.readInt32(),
            replicaDirectoryId: zeroUuid,
            logEndOffset: reader.readInt64(),
            lastFetchTimestamp: -1n,
            lastCaughtUpTimestamp: -1n
          }))
        }

        if (partition.errorCode !== 0) {
          errors.push([`/topics/${topicIndex}/partitions/${partitionIndex}`, [partition.errorCode, null]])
        }

        return partition
      })
    })),
    nodes: []
  }

  reader.readTaggedFields()

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<DescribeQuorumRequest, DescribeQuorumResponse>(55, 0, createRequest, parseResponse)
