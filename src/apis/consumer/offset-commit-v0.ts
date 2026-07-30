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
  OffsetCommit Request (Version: 0) => group_id [topics]
    group_id => STRING
    topics => name [partitions]
      name => STRING
      partitions => partition_index committed_offset metadata
        partition_index => INT32
        committed_offset => INT64
        metadata => NULLABLE_STRING
*/
export function createRequest (
  groupId: string,
  _generationId: number,
  _memberId: string,
  _groupInstanceId: NullableString,
  topics: OffsetCommitRequestTopic[]
): Writer {
  return Writer.create()
    .appendString(groupId, false)
    .appendArray(
      topics,
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => {
            w.appendInt32(partition.partitionIndex)
              .appendInt64(partition.committedOffset)
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
  OffsetCommit Response (Version: 0) => [topics]
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
  const response = {
    throttleTimeMs: 0,
    topics: reader.readArray(
      (r, topicIndex) => ({
        name: r.readString(false),
        partitions: r.readArray(
          (r, partitionIndex) => {
            const partition = { partitionIndex: r.readInt32(), errorCode: r.readInt16() }
            if (partition.errorCode !== 0) { errors.push([`/topics/${topicIndex}/partitions/${partitionIndex}`, [partition.errorCode, null]]) }
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
  if (errors.length) throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  return response
}

export const api = createAPI<OffsetCommitRequest, OffsetCommitResponse>(
  8,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
