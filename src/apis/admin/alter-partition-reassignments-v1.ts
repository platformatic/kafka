import { ResponseError } from '../../errors.ts'
import { type Nullable, type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface AlterPartitionReassignmentsRequestPartition {
  partitionIndex: number
  replicas: Nullable<number[]>
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

export interface AlterPartitionReassignmentsResponseTopic {
  name: string
  partitions: AlterPartitionReassignmentsResponsePartition[]
}

export type AlterPartitionReassignmentsResponseResponse = AlterPartitionReassignmentsResponseTopic

export interface AlterPartitionReassignmentsResponse {
  throttleTimeMs: number
  allowReplicationFactorChange: boolean
  errorCode: number
  errorMessage: NullableString
  responses: AlterPartitionReassignmentsResponseTopic[]
}

/*
  AlterPartitionReassignments Request (Version: 1) => timeout_ms allow_replication_factor_change [topics] TAG_BUFFER
    timeout_ms => INT32
    allow_replication_factor_change => BOOLEAN
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index [replicas] TAG_BUFFER
        partition_index => INT32
        replicas => INT32
*/
export function createRequest (
  timeoutMs: number,
  allowReplicationFactorChange: boolean,
  topics: AlterPartitionReassignmentsRequestTopic[]
): Writer {
  return Writer.create()
    .appendInt32(timeoutMs)
    .appendBoolean(allowReplicationFactorChange)
    .appendArray(topics, (w, t) => {
      w.appendString(t.name).appendArray(t.partitions, (w, p) => {
        w.appendInt32(p.partitionIndex).appendArray(p.replicas, (w, r) => w.appendInt32(r), true, false)
      })
    })
    .appendTaggedFields()
}

/*
  AlterPartitionReassignments Response (Version: 1) => throttle_time_ms allow_replication_factor_change error_code error_message [responses] TAG_BUFFER
  throttle_time_ms => INT32
  allow_replication_factor_change => BOOLEAN
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
  const allowReplicationFactorChange = reader.readBoolean()
  const errorCode = reader.readInt16()
  const errorMessage = reader.readNullableString()

  /* c8 ignore next 3 - Hard to test */
  if (errorCode !== 0) {
    errors.push(['', [errorCode, errorMessage]])
  }

  const response: AlterPartitionReassignmentsResponse = {
    throttleTimeMs,
    allowReplicationFactorChange,
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

  reader.readTaggedFields()

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse>(
  45,
  1,
  createRequest,
  parseResponse
)
