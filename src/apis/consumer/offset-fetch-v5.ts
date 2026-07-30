import { ResponseError } from '../../errors.ts'
import { type Nullable, type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface OffsetFetchRequestTopic {
  name: string
  partitionIndexes: number[]
}
export interface OffsetFetchRequestGroup {
  groupId: string
  memberId?: NullableString
  memberEpoch?: number
  topics?: Nullable<OffsetFetchRequestTopic[]>
}
export type OffsetFetchRequest = Parameters<typeof createRequest>
export interface OffsetFetchResponsePartition {
  partitionIndex: number
  committedOffset: bigint
  committedLeaderEpoch: number
  metadata: NullableString
  errorCode: number
}
export interface OffsetFetchResponseTopic {
  name: string
  partitions: OffsetFetchResponsePartition[]
}
export interface OffsetFetchResponse {
  throttleTimeMs: number
  topics: OffsetFetchResponseTopic[]
  errorCode: number
  groups: OffsetFetchResponseGroup[]
}
export interface OffsetFetchResponseGroup {
  groupId: string
  topics: OffsetFetchResponseTopic[]
  errorCode: number
}

/*
  OffsetFetch Request (Version: 5) => group_id [topics]
    group_id => STRING
    topics => name [partition_indexes]
      name => STRING
      partition_indexes => INT32
*/
export function createRequest (groups: OffsetFetchRequestGroup[], _requireStable: boolean): Writer {
  const group = groups[0] ?? { groupId: '', topics: null }
  return Writer.create()
    .appendString(group.groupId, false)
    .appendArray(
      group.topics,
      (writer, topic) => {
        writer
          .appendString(topic.name, false)
          .appendArray(
            topic.partitionIndexes,
            (writer, partitionIndex) => writer.appendInt32(partitionIndex),
            false,
            false
          )
      },
      false,
      false
    )
}

/*
  OffsetFetch Response (Version: 5) => throttle_time_ms [topics] error_code
    throttle_time_ms => INT32
    topics => name [partitions]
      name => STRING
      partitions => partition_index committed_offset committed_leader_epoch metadata error_code
        partition_index => INT32
        committed_offset => INT64
        committed_leader_epoch => INT32
        metadata => NULLABLE_STRING
        error_code => INT16
    error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): OffsetFetchResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: OffsetFetchResponse = {
    throttleTimeMs: reader.readInt32(),
    topics: reader.readArray(
      (topicReader, topicIndex) => ({
        name: topicReader.readString(false),
        partitions: topicReader.readArray(
          (partitionReader, partitionIndex) => {
            const partition = {
              partitionIndex: partitionReader.readInt32(),
              committedOffset: partitionReader.readInt64(),
              committedLeaderEpoch: partitionReader.readInt32(),
              metadata: partitionReader.readNullableString(false),
              errorCode: partitionReader.readInt16()
            }
            if (partition.errorCode !== 0) {
              errors.push([`/topics/${topicIndex}/partitions/${partitionIndex}`, [partition.errorCode, null]])
            }
            return partition
          },
          false,
          false
        )
      }),
      false,
      false
    ),
    errorCode: reader.readInt16(),
    groups: []
  }
  if (response.errorCode !== 0) {
    errors.push(['/errorCode', [response.errorCode, null]])
  }
  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}

export const api = createAPI<OffsetFetchRequest, OffsetFetchResponse>(9, 5, createRequest, parseResponse, false, false)
