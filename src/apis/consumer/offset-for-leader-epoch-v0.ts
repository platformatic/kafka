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
  errorCode: number
  partition: number
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
  OffsetForLeaderEpoch Request (Version: 0) => [topics]
    topics => topic [partitions]
      topic => STRING
      partitions => partition leader_epoch
        partition => INT32
        leader_epoch => INT32
*/
export function createRequest (_replicaId: number, topics: OffsetForLeaderEpochRequestTopic[]): Writer {
  return Writer.create().appendArray(
    topics,
    (w, topic) => {
      w.appendString(topic.name, false).appendArray(
        topic.partitions,
        (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt32(partition.leaderEpoch)
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
  OffsetForLeaderEpoch Response (Version: 0) => [topics]
    topics => topic [partitions]
      topic => STRING
      partitions => error_code partition end_offset
        error_code => INT16
        partition => INT32
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
    throttleTimeMs: 0,
    topics: reader.readArray(
      (r, i) => ({
        topic: r.readString(false),
        partitions: r.readArray(
          (r, j) => {
            const errorCode = r.readInt16()
            if (errorCode !== 0) {
              errors.push([`/topics/${i}/partitions/${j}`, [errorCode, null]])
            }
            return { errorCode, partition: r.readInt32(), leaderEpoch: -1, endOffset: r.readInt64() }
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

export const api = createAPI<OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse>(
  23,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
