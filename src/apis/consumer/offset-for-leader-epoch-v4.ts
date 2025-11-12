import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface OffsetForLeaderEpochRequestPartition {
  partitionIndex: number
  currentLeaderEpoch: number
  leaderEpoch: number
}

export interface OffsetForLeaderEpochRequestTopic {
  name: string
  partitions: OffsetForLeaderEpochRequestPartition[]
}

export type OffsetForLeaderEpochRequest = Parameters<typeof createRequest>

export interface OffsetForLeaderEpochResponsePartition {
  partition: number
  errorCode: number
  leaderEpoch: number
  endOffset: bigint
}

export interface OffsetForLeaderEpochResponseTopic {
  topic: string
  partitions: OffsetForLeaderEpochResponsePartition[]
}

export interface OffsetForLeaderEpochResponse {
  throttleTimeMs: number
  topics: OffsetForLeaderEpochResponseTopic[]
}

/*
  OffsetForLeaderEpoch Request (Version: 4) => replica_id [topic] TAG_BUFFER
    replica_id => INT32
    topic => topic [partitions] TAG_BUFFER
      topic => COMPACT_STRING
      partitions => partition current_leader_epoch leader_epoch TAG_BUFFER
        partition => INT32
        current_leader_epoch => INT32
        leader_epoch => INT32
*/
export function createRequest (replicaId: number, topics: OffsetForLeaderEpochRequestTopic[]): Writer {
  return Writer.create()
    .appendInt32(replicaId)
    .appendArray(topics, (w, topic) => {
      w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
        w.appendInt32(partition.partitionIndex)
          .appendInt32(partition.currentLeaderEpoch)
          .appendInt32(partition.leaderEpoch)
      })
    })
    .appendTaggedFields()
}

/*
  OffsetForLeaderEpoch Response (Version: 4) => throttle_time_ms [topics] TAG_BUFFER
    throttle_time_ms => INT32
    topics => topic [partitions] TAG_BUFFER
      topic => COMPACT_STRING
      partitions => partition error_code leader_epoch end_offset TAG_BUFFER
        partition => INT32
        error_code => INT16
        leader_epoch => INT32
        end_offset => INT64
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): OffsetForLeaderEpochResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: OffsetForLeaderEpochResponse = {
    throttleTimeMs: reader.readInt32(),
    topics: reader.readArray((r, i) => {
      return {
        topic: r.readString(),
        partitions: r.readArray((r, j) => {
          const partition = r.readInt32()
          const errorCode = r.readInt16()

          if (errorCode !== 0) {
            errors.push([`/topics/${i}/partitions/${j}`, errorCode])
          }

          return {
            partition,
            errorCode,
            leaderEpoch: r.readInt32(),
            endOffset: r.readInt64()
          }
        })
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse>(
  23,
  4,
  createRequest,
  parseResponse
)
