import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface DescribeTopicPartitionsRequestTopic {
  name: string
}

export interface DescribeTopicPartitionsRequestCursor {
  topicName: string
  partitionIndex: number
}

export type DescribeTopicPartitionsRequest = Parameters<typeof createRequest>

export interface DescribeTopicPartitionsResponsePartition {
  errorCode: number
  partitionIndex: number
  leaderId: number
  leaderEpoch: number
  replicaNodes: number[]
  isrNodes: number[]
  eligibleLeaderReplicas: number[]
  lastKnownElr: number[]
  offlineReplicas: number[]
}

export interface DescribeTopicPartitionsResponseTopic {
  errorCode: number
  name: NullableString
  topicId: string
  isInternal: boolean
  partitions: DescribeTopicPartitionsResponsePartition[]
  topicAuthorizedOperations: number
}

export interface DescribeTopicPartitionsResponseCursor {
  topicName: string
  partitionIndex: number
}

export interface DescribeTopicPartitionsResponse {
  throttleTimeMs: number
  topics: DescribeTopicPartitionsResponseTopic[]
  nextCursor?: DescribeTopicPartitionsResponseCursor
}

/*
  DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor TAG_BUFFER
    topics => name TAG_BUFFER
      name => COMPACT_STRING
    response_partition_limit => INT32
    cursor => topic_name partition_index TAG_BUFFER
      topic_name => COMPACT_STRING
      partition_index => INT32
*/
export function createRequest (
  topics: DescribeTopicPartitionsRequestTopic[],
  responsePartitionLimit: number,
  cursor?: DescribeTopicPartitionsRequestCursor
): Writer {
  const writer = Writer.create()
    .appendArray(topics, (w, t) => w.appendString(t.name))
    .appendInt32(responsePartitionLimit)

  if (cursor) {
    writer.appendInt8(1).appendString(cursor.topicName).appendInt32(cursor.partitionIndex).appendTaggedFields()
  } else {
    writer.appendInt8(-1)
  }

  return writer.appendTaggedFields()
}

/*
  DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor TAG_BUFFER
    throttle_time_ms => INT32
    topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER
      error_code => INT16
      name => COMPACT_NULLABLE_STRING
      topic_id => UUID
      is_internal => BOOLEAN
      partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [eligible_leader_replicas] [last_known_elr] [offline_replicas] TAG_BUFFER
        error_code => INT16
        partition_index => INT32
        leader_id => INT32
        leader_epoch => INT32
        replica_nodes => INT32
        isr_nodes => INT32
        eligible_leader_replicas => INT32
        last_known_elr => INT32
        offline_replicas => INT32
      topic_authorized_operations => INT32
    next_cursor => topic_name partition_index TAG_BUFFER
      topic_name => COMPACT_STRING
      partition_index => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): DescribeTopicPartitionsResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: DescribeTopicPartitionsResponse = {
    throttleTimeMs: reader.readInt32(),
    topics: reader.readArray((r, i) => {
      const errorCode = r.readInt16()

      if (errorCode !== 0) {
        errors.push([`/topics/${i}`, errorCode])
      }

      return {
        errorCode,
        name: r.readString(),
        topicId: r.readUUID(),
        isInternal: r.readBoolean(),
        partitions: r.readArray((r, j) => {
          const errorCode = r.readInt16()

          if (errorCode !== 0) {
            errors.push([`/topics/${i}/partitions/${j}`, errorCode])
          }

          return {
            errorCode,
            partitionIndex: r.readInt32(),
            leaderId: r.readInt32(),
            leaderEpoch: r.readInt32(),
            replicaNodes: r.readArray(r => r.readInt32(), true, false)!,
            isrNodes: r.readArray(r => r.readInt32(), true, false)!,
            eligibleLeaderReplicas: r.readArray(r => r.readInt32(), true, false)!,
            lastKnownElr: r.readArray(r => r.readInt32(), true, false)!,
            offlineReplicas: r.readArray(r => r.readInt32(), true, false)!
          }
        })!,
        topicAuthorizedOperations: r.readInt32()
      }
    })!
  }

  if (reader.readInt8() === 1) {
    response.nextCursor = {
      topicName: reader.readString()!,
      partitionIndex: reader.readInt32()
    }
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const describeTopicPartitionsV0 = createAPI<DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse>(
  75,
  0,
  createRequest,
  parseResponse
)
