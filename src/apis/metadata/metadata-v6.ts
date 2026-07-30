import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import type { MetadataRequestTopic } from './metadata-v12.ts'

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
  name: string
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
  Metadata Request (Version: 6) => [topics] allow_auto_topic_creation
    topics => name
      name => NULLABLE_STRING
    allow_auto_topic_creation => BOOLEAN
*/
export function createRequest (
  topics: Array<string | MetadataRequestTopic> | null,
  allowAutoTopicCreation: boolean = true,
  _includeTopicAuthorizedOperations: boolean = false
): Writer {
  return Writer.create()
    .appendArray(topics, (w, topic) => w.appendString(typeof topic === 'string' ? topic : topic.name ?? '', false), false, false)
    .appendBoolean(allowAutoTopicCreation)
}

/*
  Metadata Response (Version: 6) => throttle_time_ms [brokers] cluster_id controller_id [topics]
    throttle_time_ms => INT32
    brokers => node_id host port rack
      node_id => INT32
      host => STRING
      port => INT32
      rack => NULLABLE_STRING
    cluster_id => NULLABLE_STRING
    controller_id => INT32
    topics => error_code name is_internal [partitions]
      error_code => INT16
       name => STRING
      is_internal => BOOLEAN
      partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes] [offline_replicas]
        error_code => INT16
        partition_index => INT32
        leader_id => INT32
        replica_nodes => INT32
        isr_nodes => INT32
        offline_replicas => INT32
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
        host: r.readString(false),
        port: r.readInt32(),
        rack: r.readNullableString(false)
      }
    }, false, false)!,
    clusterId: reader.readNullableString(false),
    controllerId: reader.readInt32(),
    topics: reader.readArray((r, i) => {
      const errorCode = r.readInt16()

      if (errorCode !== 0) {
        errors.push([`/topics/${i}`, [errorCode, null]])
      }

      return {
        errorCode,
        name: r.readString(false),
        topicId: '00000000-0000-0000-0000-000000000000',
        isInternal: r.readBoolean(),
        topicAuthorizedOperations: -2147483648,
        partitions: r.readArray((r, j) => {
          const errorCode = r.readInt16()

          if (errorCode !== 0) {
            errors.push([`/topics/${i}/partitions/${j}`, [errorCode, null]])
          }

          return {
            errorCode,
            partitionIndex: r.readInt32(),
            leaderId: r.readInt32(),
            leaderEpoch: -1,
            replicaNodes: r.readArray(() => r.readInt32(), false, false)!,
            isrNodes: r.readArray(() => r.readInt32(), false, false)!,
            offlineReplicas: r.readArray(() => r.readInt32(), false, false)!
          }
        }, false, false)!
      }
    }, false, false)!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<MetadataRequest, MetadataResponse>(3, 6, createRequest, parseResponse, false, false)
