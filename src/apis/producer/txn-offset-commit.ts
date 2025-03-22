import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
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

export interface TxnOffsetCommitRequest {
  transactionalId: string
  groupId: string
  producerId: bigint
  producerEpoch: number
  generationId: number
  memberId: string
  groupInstanceId: NullableString
  topics: TxnOffsetCommitRequestTopic[]
}

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
  TxnOffsetCommit Request (Version: 4) => transactional_id group_id producer_id producer_epoch generation_id member_id group_instance_id [topics] TAG_BUFFER
    transactional_id => COMPACT_STRING
    group_id => COMPACT_STRING
    producer_id => INT64
    producer_epoch => INT16
    generation_id => INT32
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index committed_offset committed_leader_epoch committed_metadata TAG_BUFFER
        partition_index => INT32
        committed_offset => INT64
        committed_leader_epoch => INT32
        committed_metadata => COMPACT_NULLABLE_STRING
*/
function createRequest (request: TxnOffsetCommitRequest): Writer {
  return Writer.create()
    .appendString(request.transactionalId, true)
    .appendString(request.groupId, true)
    .appendInt64(request.producerId)
    .appendInt16(request.producerEpoch)
    .appendInt32(request.generationId)
    .appendString(request.memberId, true)
    .appendString(request.groupInstanceId, true)
    .appendArray(request.topics, (w, t) => {
      w.appendString(t.name, true).appendArray(t.partitions, (w, p) => {
        w.appendInt32(p.partitionIndex)
          .appendInt64(p.committedOffset)
          .appendInt32(p.committedLeaderEpoch)
          .appendString(p.committedMetadata, true)
          .appendTaggedFields() // Add tagged fields for partitions
      }, true, true)
      .appendTaggedFields() // Add tagged fields for topics
    }, true, true)
    .appendTaggedFields()
}

/*
  TxnOffsetCommit Response (Version: 4) => throttle_time_ms [topics] TAG_BUFFER
    throttle_time_ms => INT32
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index error_code TAG_BUFFER
        partition_index => INT32
        error_code => INT16
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): TxnOffsetCommitResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: TxnOffsetCommitResponse = {
    throttleTimeMs: reader.readInt32(),
    topics: reader.readArray((r, i) => {
      return {
        name: r.readString()!,
        partitions: r.readArray((r, j) => {
          const partition = {
            partitionIndex: r.readInt32(),
            errorCode: r.readInt16()
          }

          if (partition.errorCode !== 0) {
            errors.push([`/topics/${i}/partitions/${j}`, partition.errorCode])
          }

          return partition
        })!
      }
    })!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const txnOffsetCommitV4 = createAPI<TxnOffsetCommitRequest, TxnOffsetCommitResponse>(
  28,
  4,
  createRequest,
  parseResponse
)
