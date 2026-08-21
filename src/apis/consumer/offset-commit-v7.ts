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
  OffsetCommit Request (Version: 7) => group_id generation_id_or_member_epoch member_id group_instance_id [topics]
    group_id => STRING
    generation_id_or_member_epoch => INT32
    member_id => STRING
    group_instance_id => NULLABLE_STRING
    topics => name [partitions]
      name => STRING
      partitions => partition_index committed_offset committed_leader_epoch committed_metadata
        partition_index => INT32
        committed_offset => INT64
        committed_leader_epoch => INT32
        committed_metadata => NULLABLE_STRING
*/
export function createRequest (
  groupId: string,
  generationIdOrMemberEpoch: number,
  memberId: string,
  groupInstanceId: NullableString,
  topics: OffsetCommitRequestTopic[]
): Writer {
  return Writer.create()
    .appendString(groupId, false)
    .appendInt32(generationIdOrMemberEpoch)
    .appendString(memberId, false)
    .appendString(groupInstanceId, false)
    .appendArray(
      topics,
      (w, t) => {
        w.appendString(t.name, false).appendArray(
          t.partitions,
          (w, p) => {
            w.appendInt32(p.partitionIndex)
              .appendInt64(p.committedOffset)
              .appendInt32(p.committedLeaderEpoch)
              .appendString(p.committedMetadata, false)
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
  OffsetCommit Response (Version: 7) => throttle_time_ms [topics]
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
): OffsetCommitResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: OffsetCommitResponse = {
    throttleTimeMs: reader.readInt32(),
    topics: reader.readArray(
      (r, i) => ({
        name: r.readString(false),
        partitions: r.readArray(
          (r, j) => {
            const partition = { partitionIndex: r.readInt32(), errorCode: r.readInt16() }
            if (partition.errorCode !== 0) {
              errors.push([`/topics/${i}/partitions/${j}`, [partition.errorCode, null]])
            }
            return partition
          },
          false,
          false
        )
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

export const api = createAPI<OffsetCommitRequest, OffsetCommitResponse>(
  8,
  7,
  createRequest,
  parseResponse,
  false,
  false
)
