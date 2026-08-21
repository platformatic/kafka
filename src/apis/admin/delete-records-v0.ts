import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
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
  DeleteRecords Request (Version: 0) => [topics] timeout_ms
    topics => name [partitions]
      name => STRING
      partitions => partition_index offset
        partition_index => INT32
        offset => INT64
    timeout_ms => INT32
*/
export function createRequest (topics: DeleteRecordsRequestTopics[], timeoutMs: number): Writer {
  return Writer.create()
    .appendArray(
      topics,
      (writer, topic) =>
        writer
          .appendString(topic.name, false)
          .appendArray(
            topic.partitions,
            (writer, partition) => writer.appendInt32(partition.partitionIndex).appendInt64(partition.offset),
            false,
            false
          ),
      false,
      false
    )
    .appendInt32(timeoutMs)
}

/*
  DeleteRecords Response (Version: 0) => throttle_time_ms [topics]
    throttle_time_ms => INT32
    topics => name [partitions]
      name => STRING
      partitions => partition_index low_watermark error_code
        partition_index => INT32
        low_watermark => INT64
        error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DeleteRecordsResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: DeleteRecordsResponse = {
    throttleTimeMs: reader.readInt32(),
    topics: reader.readArray(
      (r, topicIndex) => ({
        name: r.readString(false),
        partitions: r.readArray(
          (r, partitionIndex) => {
            const partition = { partitionIndex: r.readInt32(), lowWatermark: r.readInt64(), errorCode: r.readInt16() }
            if (partition.errorCode !== 0) {
              errors.push([`topics[${topicIndex}].partitions[${partitionIndex}]`, [partition.errorCode, null]])
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

export const api = createAPI<DeleteRecordsRequest, DeleteRecordsResponse>(
  21,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
