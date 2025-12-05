import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface AlterPartitionReassignmentsRequestPartition {
  partitionIndex: number
  replicas: number[]
}

export interface AlterPartitionReassignmentsRequestTopic {
  name: string
  partitions: AlterPartitionReassignmentsRequestPartition[]
}

export type AlterPartitionReassignmentsRequest = Parameters<typeof createRequest>

export interface AlterPartitionReassignmentsResponsePartition {
  partitionIndex: number
  errorCode: number
  errorMessage: NullableString
}

export interface AlterPartitionReassignmentsResponseResponse {
  name: string
  partitions: AlterPartitionReassignmentsResponsePartition[]
}

export interface AlterPartitionReassignmentsResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
  responses: AlterPartitionReassignmentsResponseResponse[]
}

/*
  AlterPartitionReassignments Request (Version: 0) => timeout_ms [topics] TAG_BUFFER
    timeout_ms => INT32
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index [replicas] TAG_BUFFER
        partition_index => INT32
        replicas => INT32
*/
export function createRequest (timeoutMs: number, topics: AlterPartitionReassignmentsRequestTopic[]): Writer {
  return Writer.create()
    .appendInt32(timeoutMs)
    .appendArray(topics, (w, t) => {
      w.appendString(t.name).appendArray(t.partitions, (w, p) => {
        w.appendInt32(p.partitionIndex).appendArray(p.replicas, (w, r) => w.appendInt32(r), true, false)
      })
    })
    .appendTaggedFields()
}

/*
  AlterPartitionReassignments Response (Version: 0) => throttle_time_ms error_code error_message [responses] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    responses => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index error_code error_message TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        error_message => COMPACT_NULLABLE_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): AlterPartitionReassignmentsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()
  const errorMessage = reader.readNullableString()

  if (errorCode !== 0) {
    errors.push(['', [errorCode, errorMessage ?? '']])
  }

  const response: AlterPartitionReassignmentsResponse = {
    throttleTimeMs,
    errorCode,
    errorMessage,
    responses: reader.readArray((r, i) => {
      return {
        name: r.readString(),
        partitions: r.readArray((r, j) => {
          const partition = {
            partitionIndex: r.readInt32(),
            errorCode: r.readInt16(),
            errorMessage: r.readNullableString()
          }

          if (partition.errorCode !== 0) {
            errors.push([`responses/${i}/partitions/${j}`, [partition.errorCode, partition.errorMessage]])
          }

          return partition
        })
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse>(
  45,
  0,
  createRequest,
  parseResponse
)
