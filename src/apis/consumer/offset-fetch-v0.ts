import { ResponseError, UserError } from '../../errors.ts'
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
  topics: Nullable<OffsetFetchRequestTopic[]>
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
export interface OffsetFetchResponseGroup {
  groupId: string
  topics: OffsetFetchResponseTopic[]
  errorCode: number
}
export interface OffsetFetchResponse {
  throttleTimeMs: number
  topics: OffsetFetchResponseTopic[]
  errorCode: number
  groups: OffsetFetchResponseGroup[]
}
/*
  OffsetFetch Request (Version: 0) => group_id [topics]
    group_id => STRING
    topics => name [partition_indexes]
      name => STRING
      partition_indexes => INT32
*/
export function createRequest (groups: OffsetFetchRequestGroup[], requireStable: boolean): Writer {
  return createRequestForVersion(groups, requireStable, 0)
}

export function createRequestForVersion (
  groups: OffsetFetchRequestGroup[],
  _requireStable: boolean,
  version: number
): Writer {
  const group = groups[0] ?? { groupId: '', topics: [] }
  if (group.topics == null) {
    throw new UserError(`OffsetFetch v${version} does not support fetching all offsets.`)
  }

  return Writer.create()
    .appendString(group.groupId, false)
    .appendArray(
      group.topics,
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitionIndexes,
          (w, partition) => w.appendInt32(partition),
          false,
          false
        )
      },
      false,
      false
    )
}
/*
  OffsetFetch Response (Version: 0) => [topics]
    topics => name [partitions]
      name => STRING
      partitions => partition_index committed_offset metadata error_code
        partition_index => INT32
        committed_offset => INT64
        metadata => NULLABLE_STRING
        error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): OffsetFetchResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response = {
    throttleTimeMs: 0,
    topics: reader.readArray(
      (r, topicIndex) => ({
        name: r.readString(false),
        partitions: r.readArray(
          (r, partitionIndex) => {
            const partition = {
              partitionIndex: r.readInt32(),
              committedOffset: r.readInt64(),
              committedLeaderEpoch: -1,
              metadata: r.readNullableString(false),
              errorCode: r.readInt16()
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
    errorCode: 0,
    groups: []
  }
  if (errors.length) throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  return response
}
export const api = createAPI<OffsetFetchRequest, OffsetFetchResponse>(9, 0, createRequest, parseResponse, false, false)
