import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../index.ts'

interface ListOffsetsRequestPartition {
  partitionIndex: number
  currentLeaderEpoch: number
  timestamp: bigint
}

interface ListOffsetsRequestTopic {
  name: string
  partitions: ListOffsetsRequestPartition[]
}

export type ListOffsetsRequest = Parameters<typeof createRequest>

export interface ListOffsetResponsePartition {
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
  ListOffsets Request (Version: 9) => replica_id isolation_level [topics] TAG_BUFFER
    replica_id => INT32
    isolation_level => INT8
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index current_leader_epoch timestamp TAG_BUFFER
        partition_index => INT32
        current_leader_epoch => INT32
        timestamp => INT64
*/
function createRequest (replica: number, isolationLevel: number, topics: ListOffsetsRequestTopic[]): Writer {
  return Writer.create()
    .appendInt32(replica)
    .appendInt8(isolationLevel)
    .appendArray(topics, (w, topic) => {
      w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => {
        w.appendInt32(partition.partitionIndex)
          .appendInt32(partition.currentLeaderEpoch)
          .appendInt64(partition.timestamp)
      })
    })
    .appendTaggedFields()
}

/*
  ListOffsets Response (Version: 9) => throttle_time_ms [topics] TAG_BUFFER
    throttle_time_ms => INT32
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index error_code timestamp offset leader_epoch TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        timestamp => INT64
        offset => INT64
        leader_epoch => INT32
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): ListOffsetsResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: ListOffsetsResponse = {
    throttleTimeMs: reader.readInt32(),
    topics: reader.readArray((r, i) => {
      return {
        name: r.readString()!,
        partitions: r.readArray((r, j) => {
          const partition = {
            partitionIndex: r.readInt32(),
            errorCode: r.readInt16(),
            timestamp: r.readInt64(),
            offset: r.readInt64(),
            leaderEpoch: r.readInt32()
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
    throw new ResponseError(apiKey, apiVersion, { errors: Object.fromEntries(errors), response })
  }

  return response
}

export const listOffsetsV9 = createAPI<ListOffsetsRequest, ListOffsetsResponse>(2, 9, createRequest, parseResponse)
