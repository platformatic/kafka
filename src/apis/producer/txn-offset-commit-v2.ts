import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface TxnOffsetCommitRequestPartition {
  partitionIndex: number
  committedOffset: bigint
  committedLeaderEpoch: number
  committedMetadata?: NullableString
}

export interface TxnOffsetCommitRequestTopic {
  name: string
  partitions: TxnOffsetCommitRequestPartition[]
}

export type TxnOffsetCommitRequest = Parameters<typeof createRequest>

export interface TxnOffsetCommitResponsePartition {
  partitionIndex: number
  errorCode: number
}

export interface TxnOffsetCommitResponseTopic {
  name: string
  partitions: TxnOffsetCommitResponsePartition[]
}

export interface TxnOffsetCommitResponse {
  throttleTimeMs: number
  topics: TxnOffsetCommitResponseTopic[]
}

/*
  TxnOffsetCommit Request (Version: 2) => transactional_id group_id producer_id producer_epoch [topics]
    transactional_id => STRING
    group_id => STRING
    producer_id => INT64
    producer_epoch => INT16
    topics => name [partitions]
      name => STRING
      partitions => partition_index committed_offset committed_leader_epoch committed_metadata
        partition_index => INT32
        committed_offset => INT64
        committed_leader_epoch => INT32
        committed_metadata => NULLABLE_STRING
*/
export function createRequest (
  transactionalId: string,
  groupId: string,
  producerId: bigint,
  producerEpoch: number,
  _generationId: number,
  _memberId: string,
  _groupInstanceId: NullableString,
  topics: TxnOffsetCommitRequestTopic[]
): Writer {
  return Writer.create()
    .appendString(transactionalId, false)
    .appendString(groupId, false)
    .appendInt64(producerId)
    .appendInt16(producerEpoch)
    .appendArray(
      topics,
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt64(partition.committedOffset)
              .appendInt32(partition.committedLeaderEpoch)
              .appendString(partition.committedMetadata, false)
          },
          false,
          false
        )
      },
      false,
      false
    )
}

/*
  TxnOffsetCommit Response (Version: 2) => throttle_time_ms [topics]
    throttle_time_ms => INT32
    topics => name [partitions]
      name => STRING
      partitions => partition_index error_code
        partition_index => INT32
        error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): TxnOffsetCommitResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: TxnOffsetCommitResponse = {
    throttleTimeMs: reader.readInt32(),
    topics: reader.readArray(
      (r, topicIndex) => ({
        name: r.readString(false),
        partitions: r.readArray((r, partitionIndex) => {
          const partition = { partitionIndex: r.readInt32(), errorCode: r.readInt16() }
          if (partition.errorCode !== 0) {
            errors.push([`/topics/${topicIndex}/partitions/${partitionIndex}`, [partition.errorCode, null]])
          }
          return partition
        }, false, false)
      }),
      false,
      false
    )
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}

export const api = createAPI<TxnOffsetCommitRequest, TxnOffsetCommitResponse>(
  28,
  2,
  createRequest,
  parseResponse,
  false,
  false
)
