import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface ListOffsetsRequestPartition {
  partitionIndex: number
  currentLeaderEpoch: number
  timestamp: bigint
}

export interface ListOffsetsRequestTopic {
  name: string
  partitions: ListOffsetsRequestPartition[]
}

// The isolation level is unavailable before v2, but remains accepted for Consumer compatibility.
export type ListOffsetsRequest = Parameters<typeof createRequest>

export interface ListOffsetResponsePartition {
  partitionIndex: number
  errorCode: number
  timestamp: bigint
  offset: bigint
  leaderEpoch: number
}

export interface ListOffsetResponseTopic {
  name: string
  partitions: ListOffsetResponsePartition[]
}

export interface ListOffsetsResponse {
  throttleTimeMs: number
  topics: ListOffsetResponseTopic[]
}

/*
  ListOffsets Request (Version: 0) => replica_id [topics]
    replica_id => INT32
    topics => name [partitions]
      name => STRING
      partitions => partition_index timestamp max_num_offsets
        partition_index => INT32
        timestamp => INT64
        max_num_offsets => INT32
*/
export function createRequest (
  replica: number,
  _isolationLevel: number,
  topics: ListOffsetsRequestTopic[]
): Writer {
  return Writer.create().appendInt32(replica).appendArray(
    topics,
    (w, topic) => {
      w.appendString(topic.name, false).appendArray(
        topic.partitions,
        (w, partition) => w.appendInt32(partition.partitionIndex).appendInt64(partition.timestamp).appendInt32(1),
        false,
        false
      )
    },
    false,
    false
  )
}

/*
  ListOffsets Response (Version: 0) => [topics]
    topics => name [partitions]
      name => STRING
      partitions => partition_index error_code [offsets]
        partition_index => INT32
        error_code => INT16
        offsets => INT64
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ListOffsetsResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: ListOffsetsResponse = {
    throttleTimeMs: 0,
    topics: reader.readArray(
      (r, i) => ({
        name: r.readString(false),
        partitions: r.readArray(
          (r, j) => {
            const partitionIndex = r.readInt32()
            const errorCode = r.readInt16()
            const offsets = r.readArray(r => r.readInt64(), false, false)
            if (errorCode !== 0) {
              errors.push([`/topics/${i}/partitions/${j}`, [errorCode, null]])
            }
            return { partitionIndex, errorCode, timestamp: -1n, offset: offsets[0] ?? -1n, leaderEpoch: -1 }
          },
          false,
          false
        )
      }),
      false,
      false
    )
  }

  if (errors.length > 0) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}

export const api = createAPI<ListOffsetsRequest, ListOffsetsResponse>(2, 0, createRequest, parseResponse, false, false)
