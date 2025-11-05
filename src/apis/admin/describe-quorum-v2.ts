import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface DescribeQuorumRequestPartition {
  partitionIndex: number
}

export interface DescribeQuorumRequestTopic {
  topicName: string
  partitions: DescribeQuorumRequestPartition[]
}

export type DescribeQuorumRequest = Parameters<typeof createRequest>

export interface DescribeQuorumResponseVoter {
  replicaId: number
  replicaDirectoryId: string
  logEndOffset: bigint
  lastFetchTimestamp: bigint
  lastCaughtUpTimestamp: bigint
}

export interface DescribeQuorumResponsePartition {
  partitionIndex: number
  errorCode: number
  errorMessage: NullableString
  leaderId: number
  leaderEpoch: number
  highWatermark: bigint
  currentVoters: DescribeQuorumResponseVoter[]
  observers: DescribeQuorumResponseVoter[]
}

export interface DescribeQuorumResponseNodeListener {
  name: string
  host: string
  port: number
}

export interface DescribeQuorumResponseTopic {
  topicName: string
  partitions: DescribeQuorumResponsePartition[]
}

export interface DescribeQuorumResponseNode {
  nodeId: number
  listeners: DescribeQuorumResponseNodeListener[]
}

export interface DescribeQuorumResponse {
  errorCode: number
  errorMessage: NullableString
  topics: DescribeQuorumResponseTopic[]
  nodes: DescribeQuorumResponseNode[]
}

/*
  DescribeQuorum Request (Version: 2) => [topics] TAG_BUFFER
    topics => topic_name [partitions] TAG_BUFFER
      topic_name => COMPACT_STRING
      partitions => partition_index TAG_BUFFER
        partition_index => INT32
*/
export function createRequest (topics: DescribeQuorumRequestTopic[]): Writer {
  return Writer.create()
    .appendArray(topics, (w, t) => {
      w.appendString(t.topicName).appendArray(t.partitions, (w, p) => {
        w.appendInt32(p.partitionIndex)
      })
    })
    .appendTaggedFields()
}

/*
  DescribeQuorum Response (Version: 2) => error_code error_message [topics] [nodes] TAG_BUFFER
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    topics => topic_name [partitions] TAG_BUFFER
      topic_name => COMPACT_STRING
      partitions => partition_index error_code error_message leader_id leader_epoch high_watermark [current_voters] [observers] TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        error_message => COMPACT_NULLABLE_STRING
        leader_id => INT32
        leader_epoch => INT32
        high_watermark => INT64
        current_voters => replica_id replica_directory_id log_end_offset last_fetch_timestamp last_caught_up_timestamp TAG_BUFFER
          replica_id => INT32
          replica_directory_id => UUID
          log_end_offset => INT64
          last_fetch_timestamp => INT64
          last_caught_up_timestamp => INT64
        observers => replica_id replica_directory_id log_end_offset last_fetch_timestamp last_caught_up_timestamp TAG_BUFFER
          replica_id => INT32
          replica_directory_id => UUID
          log_end_offset => INT64
          last_fetch_timestamp => INT64
          last_caught_up_timestamp => INT64
    nodes => node_id [listeners] TAG_BUFFER
      node_id => INT32
      listeners => name host port TAG_BUFFER
        name => COMPACT_STRING
        host => COMPACT_STRING
        port => UINT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeQuorumResponse {
  const errors: ResponseErrorWithLocation[] = []

  const errorCode = reader.readInt16()
  const errorMessage = reader.readNullableString()

  if (errorCode !== 0) {
    errors.push(['', [errorCode, errorMessage]])
  }
  const response: DescribeQuorumResponse = {
    errorCode,
    errorMessage,
    topics: reader.readArray((r, i) => {
      return {
        topicName: r.readString(),
        partitions: r.readArray((r, j) => {
          const partition = {
            partitionIndex: r.readInt32(),
            errorCode: r.readInt16(),
            errorMessage: r.readNullableString(),
            leaderId: r.readInt32(),
            leaderEpoch: r.readInt32(),
            highWatermark: r.readInt64(),
            currentVoters: r.readArray(r => {
              return {
                replicaId: r.readInt32(),
                replicaDirectoryId: r.readUUID(),
                logEndOffset: r.readInt64(),
                lastFetchTimestamp: r.readInt64(),
                lastCaughtUpTimestamp: r.readInt64()
              }
            }),
            observers: r.readArray(r => {
              return {
                replicaId: r.readInt32(),
                replicaDirectoryId: r.readUUID(),
                logEndOffset: r.readInt64(),
                lastFetchTimestamp: r.readInt64(),
                lastCaughtUpTimestamp: r.readInt64()
              }
            })
          }

          if (partition.errorCode !== 0) {
            errors.push([`/topics/${i}/partitions/${j}`, [partition.errorCode, partition.errorMessage]])
          }

          return partition
        })
      }
    }),
    nodes: reader.readArray(r => {
      return {
        nodeId: r.readInt32(),
        listeners: r.readArray(r => {
          return {
            name: r.readString(),
            host: r.readString(),
            port: r.readUnsignedInt16()
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

export const api = createAPI<DescribeQuorumRequest, DescribeQuorumResponse>(55, 2, createRequest, parseResponse)
