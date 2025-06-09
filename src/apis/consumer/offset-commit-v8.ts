import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface OffsetCommitRequestPartition {
  partitionIndex: number
  committedOffset: bigint
  committedLeaderEpoch: number
  committedMetadata?: NullableString
}

export interface OffsetCommitRequestTopic {
  name: string
  partitions: OffsetCommitRequestPartition[]
}

export type OffsetCommitRequest = Parameters<typeof createRequest>

export interface OffsetCommitResponsePartition {
  partitionIndex: number
  errorCode: number
}

export interface OffsetCommitResponseTopic {
  name: string
  partitions: OffsetCommitResponsePartition[]
}

export interface OffsetCommitResponse {
  throttleTimeMs: number
  topics: OffsetCommitResponseTopic[]
}

/*
  OffsetCommit Request (Version: 8) => group_id generation_id_or_member_epoch member_id group_instance_id [topics] TAG_BUFFER
    group_id => COMPACT_STRING
    generation_id_or_member_epoch => INT32
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
export function createRequest (
  groupId: string,
  generationIdOrMemberEpoch: number,
  memberId: string,
  groupInstanceId: NullableString,
  topics: OffsetCommitRequestTopic[]
): Writer {
  return Writer.create()
    .appendString(groupId)
    .appendInt32(generationIdOrMemberEpoch)
    .appendString(memberId)
    .appendString(groupInstanceId)
    .appendArray(topics, (w, t) => {
      w.appendString(t.name).appendArray(t.partitions, (w, p) => {
        w.appendInt32(p.partitionIndex)
          .appendInt64(p.committedOffset)
          .appendInt32(p.committedLeaderEpoch)
          .appendString(p.committedMetadata)
      })
    })
    .appendTaggedFields()
}

/*
  OffsetCommit Response (Version: 8) => throttle_time_ms [topics] TAG_BUFFER
    throttle_time_ms => INT32
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index error_code TAG_BUFFER
        partition_index => INT32
        error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): OffsetCommitResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: OffsetCommitResponse = {
    throttleTimeMs: reader.readInt32(),
    topics: reader.readArray((r, i) => {
      return {
        name: r.readString(),
        partitions: r.readArray((r, j) => {
          const partition = {
            partitionIndex: r.readInt32(),
            errorCode: r.readInt16()
          }

          if (partition.errorCode !== 0) {
            errors.push([`/topics/${i}/partitions/${j}`, partition.errorCode])
          }

          return partition
        })
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<OffsetCommitRequest, OffsetCommitResponse>(8, 8, createRequest, parseResponse)
