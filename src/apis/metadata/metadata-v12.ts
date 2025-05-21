import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export type MetadataRequest = Parameters<typeof createRequest>

export interface MetadataResponsePartition {
  errorCode: number
  partitionIndex: number
  leaderId: number
  leaderEpoch: number
  replicaNodes: number[]
  isrNodes: number[]
  offlineReplicas: number[]
}

export interface MetadataResponseTopic {
  errorCode: number
  name: NullableString
  topicId: string
  isInternal: boolean
  partitions: MetadataResponsePartition[]
  topicAuthorizedOperations: number
}

export interface MetadataResponseBroker {
  nodeId: number
  host: string
  port: number
  rack: NullableString
}

export interface MetadataResponse {
  throttleTimeMs: number
  brokers: MetadataResponseBroker[]
  clusterId: NullableString
  controllerId: number
  topics: MetadataResponseTopic[]
}

/*
  Metadata Request (Version: 12) => [topics] allow_auto_topic_creation include_topic_authorized_operations TAG_BUFFER
    topics => topic_id name TAG_BUFFER
      topic_id => UUID
      name => COMPACT_NULLABLE_STRING
    allow_auto_topic_creation => BOOLEAN
    include_topic_authorized_operations => BOOLEAN
*/
export function createRequest (
  topics: string[] | null,
  allowAutoTopicCreation: boolean = false,
  includeTopicAuthorizedOperations: boolean = false
): Writer {
  return Writer.create()
    .appendArray(topics, (w, topic) => w.appendUUID(null).appendString(topic))
    .appendBoolean(allowAutoTopicCreation)
    .appendBoolean(includeTopicAuthorizedOperations)
    .appendTaggedFields()
}

/*
  Metadata Response (Version: 12) => throttle_time_ms [brokers] cluster_id controller_id [topics] TAG_BUFFER
    throttle_time_ms => INT32
    brokers => node_id host port rack TAG_BUFFER
      node_id => INT32
      host => COMPACT_STRING
      port => INT32
      rack => COMPACT_NULLABLE_STRING
    cluster_id => COMPACT_NULLABLE_STRING
    controller_id => INT32
    topics => error_code name topic_id is_internal [partitions] topic_authorized_operations TAG_BUFFER
      error_code => INT16
      name => COMPACT_NULLABLE_STRING
      topic_id => UUID
      is_internal => BOOLEAN
      partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] TAG_BUFFER
        error_code => INT16
        partition_index => INT32
        leader_id => INT32
        leader_epoch => INT32
        replica_nodes => INT32
        isr_nodes => INT32
        offline_replicas => INT32
      topic_authorized_operations => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): MetadataResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: MetadataResponse = {
    throttleTimeMs: reader.readInt32(),
    brokers: reader.readArray(r => {
      return {
        nodeId: r.readInt32(),
        host: r.readString(),
        port: r.readInt32(),
        rack: r.readNullableString()
      }
    }),
    clusterId: reader.readNullableString(),
    controllerId: reader.readInt32(),
    topics: reader.readArray((r, i) => {
      const errorCode = r.readInt16()

      if (errorCode !== 0) {
        errors.push([`/topics/${i}`, errorCode])
      }

      return {
        errorCode,
        name: r.readNullableString(),
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
            replicaNodes: r.readArray(() => r.readInt32(), true, false)!,
            isrNodes: r.readArray(() => r.readInt32(), true, false)!,
            offlineReplicas: r.readArray(() => r.readInt32(), true, false)!
          }
        }),
        topicAuthorizedOperations: reader.readInt32()
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<MetadataRequest, MetadataResponse>(3, 12, createRequest, parseResponse)
