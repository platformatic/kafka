import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../index.ts'

export interface ListPartitionReassignmentsRequestTopic {
  name: string
  partitionIndexes: number[]
}

export type ListPartitionReassignmentsRequest = Parameters<typeof createRequest>

export interface ListPartitionReassignmentsResponsePartition {
  partitionIndex: number
  replicas: number[]
  addingReplicas: number[]
  removingReplicas: number[]
}

export interface ListPartitionReassignmentsResponseTopic {
  name: string
  partitions: ListPartitionReassignmentsResponsePartition[]
}

export interface ListPartitionReassignmentsResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
  topics: ListPartitionReassignmentsResponseTopic[]
}

/*
  ListPartitionReassignments Request (Version: 0) => timeout_ms [topics] TAG_BUFFER
    timeout_ms => INT32
    topics => name [partition_indexes] TAG_BUFFER
      name => COMPACT_STRING
      partition_indexes => INT32
*/
function createRequest (timeoutMs: number, topics: ListPartitionReassignmentsRequestTopic[]): Writer {
  return Writer.create()
    .appendInt32(timeoutMs)
    .appendArray(topics, (w, t) => {
      w.appendString(t.name).appendArray(t.partitionIndexes, (w, p) => w.appendInt32(p), true, false)
    })
    .appendTaggedFields()
}

/*
  ListPartitionReassignments Response (Version: 0) => throttle_time_ms error_code error_message [topics] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => partition_index [replicas] [adding_replicas] [removing_replicas] TAG_BUFFER
        partition_index => INT32
        replicas => INT32
        adding_replicas => INT32
        removing_replicas => INT32
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): ListPartitionReassignmentsResponse {
  const reader = Reader.from(raw)

  const response: ListPartitionReassignmentsResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    errorMessage: reader.readString(),
    topics: reader.readArray(r => {
      return {
        name: r.readString()!,
        partitions: r.readArray(r => {
          return {
            partitionIndex: r.readInt32(),
            replicas: r.readArray(r => r.readInt32(), true, false)!,
            addingReplicas: r.readArray(r => r.readInt32(), true, false)!,
            removingReplicas: r.readArray(r => r.readInt32(), true, false)!
          }
        })!
      }
    })!
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { errors: { '': response.errorCode }, response })
  }

  return response
}

export const listPartitionReassignmentsV0 = createAPI<
  ListPartitionReassignmentsRequest,
  ListPartitionReassignmentsResponse
>(46, 0, createRequest, parseResponse)
