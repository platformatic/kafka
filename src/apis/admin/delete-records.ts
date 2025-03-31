import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface DeleteRecordsRequestPartitions {
  partitionIndex: number
  offset: bigint
}

export interface DeleteRecordsRequestTopics {
  name: string
  partitions: DeleteRecordsRequestPartitions[]
}

export type DeleteRecordsRequest = Parameters<typeof createRequest>

export interface DeleteRecordsResponsePartition {
  partitionIndex: number
  lowWatermark: bigint
  errorCode: number
}

export interface DeleteRecordsResponseTopic {
  name: string
  partitions: DeleteRecordsResponsePartition[]
}

export interface DeleteRecordsResponse {
  throttleTimeMs: number
  topics: DeleteRecordsResponseTopic[]
}

/*
  DeleteRecords Request (Version: 2) => [topics] timeout_ms TAG_BUFFER
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index offset TAG_BUFFER
        partition_index => INT32
        offset => INT64
    timeout_ms => INT32
*/
export function createRequest (topics: DeleteRecordsRequestTopics[], timeoutMs: number): Writer {
  return Writer.create()
    .appendArray(topics, (w, t) => {
      w.appendString(t.name).appendArray(t.partitions, (w, p) => {
        w.appendInt32(p.partitionIndex).appendInt64(p.offset)
      })
    })
    .appendInt32(timeoutMs)
    .appendTaggedFields()
}

/*
  DeleteRecords Response (Version: 2) => throttle_time_ms [topics] TAG_BUFFER
    throttle_time_ms => INT32
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index low_watermark error_code TAG_BUFFER
        partition_index => INT32
        low_watermark => INT64
        error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): DeleteRecordsResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: DeleteRecordsResponse = {
    throttleTimeMs: reader.readInt32(),
    topics: reader.readArray((r, i) => {
      return {
        name: r.readString()!,
        partitions: r.readArray((r, j) => {
          const partition = {
            partitionIndex: r.readInt32(),
            lowWatermark: r.readInt64(),
            errorCode: r.readInt16()
          }

          if (partition.errorCode !== 0) {
            errors.push([`topics[${i}].partitions[${j}]`, partition.errorCode])
          }

          return partition
        })!
      }
    })!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const deleteRecordsV2 = createAPI<DeleteRecordsRequest, DeleteRecordsResponse>(
  21,
  2,
  createRequest,
  parseResponse
)
