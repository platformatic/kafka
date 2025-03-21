import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface OffsetFetchRequestTopic {
  name: string
  partitionIndexes: number[]
}

export interface OffsetFetchRequestGroup {
  groupId: string
  memberId?: NullableString
  memberEpoch: number
  topics: OffsetFetchRequestTopic[]
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
  groups: OffsetFetchResponseGroup[]
}

/*
  OffsetFetch Request (Version: 9) => [groups] require_stable TAG_BUFFER
    groups => group_id member_id member_epoch [topics] TAG_BUFFER
      group_id => COMPACT_STRING
      member_id => COMPACT_NULLABLE_STRING
      member_epoch => INT32
      topics => name [partition_indexes] TAG_BUFFER
        name => COMPACT_STRING
        partition_indexes => INT32
    require_stable => BOOLEAN
*/
function createRequest (groups: OffsetFetchRequestGroup[], requireStable: boolean): Writer {
  return Writer.create()
    .appendArray(groups, (w, g) => {
      w.appendString(g.groupId)
        .appendString(g.memberId)
        .appendInt32(g.memberEpoch)
        .appendArray(g.topics, (w, t) => {
          w.appendString(t.name).appendArray(t.partitionIndexes, (w, i) => w.appendInt32(i), true, false)
        })
    })
    .appendBoolean(requireStable)
    .appendTaggedFields()
}

/*
  OffsetFetch Response (Version: 9) => throttle_time_ms [groups] TAG_BUFFER
    throttle_time_ms => INT32
    groups => group_id [topics] error_code TAG_BUFFER
      group_id => COMPACT_STRING
      topics => name [partitions] TAG_BUFFER
        name => COMPACT_STRING
        partitions => partition_index committed_offset committed_leader_epoch metadata error_code TAG_BUFFER
          partition_index => INT32
          committed_offset => INT64
          committed_leader_epoch => INT32
          metadata => COMPACT_NULLABLE_STRING
          error_code => INT16
      error_code => INT16
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): OffsetFetchResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: OffsetFetchResponse = {
    throttleTimeMs: reader.readInt32(),
    groups: reader.readArray((r, i) => {
      const group = {
        groupId: r.readString()!,
        topics: r.readArray((r, j) => {
          return {
            name: r.readString()!,
            partitions: r.readArray((r, k) => {
              const partition = {
                partitionIndex: r.readInt32(),
                committedOffset: r.readInt64(),
                committedLeaderEpoch: r.readInt32(),
                metadata: r.readString(),
                errorCode: r.readInt16()
              }

              if (partition.errorCode !== 0) {
                errors.push([`/groups/${i}/topics/${j}/partitions/${k}`, partition.errorCode])
              }

              return partition
            })!
          }
        })!,
        errorCode: r.readInt16()
      }

      if (group.errorCode !== 0) {
        errors.push([`/groups/${i}`, group.errorCode])
      }

      return group
    })!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const offsetFetchV9 = createAPI<OffsetFetchRequest, OffsetFetchResponse>(9, 9, createRequest, parseResponse)
