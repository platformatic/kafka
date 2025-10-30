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
  clusterAuthorizedOperations: number
}

/*
 Metadata Request (Version: 8) => [topics] allow_auto_topic_creation include_cluster_authorized_operations include_topic_authorized_operations
  topics => name
    name => STRING
  allow_auto_topic_creation => BOOLEAN
  include_cluster_authorized_operations => BOOLEAN
  include_topic_authorized_operations => BOOLEAN
*/
export function createRequest (
  topics: string[] | null,
  allowAutoTopicCreation = false,
  includeTopicAuthorizedOperations = false,
  includeClusterAuthorizedOperations = false
): Writer {
  return Writer.create()
    .appendArray(topics, (w, topic) => w.appendString(topic, false), false, false)
    .appendBoolean(allowAutoTopicCreation)
    .appendBoolean(includeClusterAuthorizedOperations)
    .appendBoolean(includeTopicAuthorizedOperations)
}

/*
 Metadata Response (Version: 8) => throttle_time_ms [brokers] cluster_id controller_id [topics] cluster_authorized_operations
  throttle_time_ms => INT32
  brokers => node_id host port rack
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  cluster_id => NULLABLE_STRING
  controller_id => INT32
  topics => error_code name is_internal [partitions] topic_authorized_operations
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas]
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
  cluster_authorized_operations => INT32
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
    brokers: reader.readArray(
      (r: Reader) => ({
        nodeId: r.readInt32(),
        host: r.readString(false),
        port: r.readInt32(),
        rack: r.readNullableString(false)
      }),
      false,
      false
    ),
    clusterId: reader.readNullableString(false),
    controllerId: reader.readInt32(),
    topics: reader.readArray(
      (r, i) => {
        const ec = r.readInt16()
        if (ec !== 0) errors.push([`/topics/${i}`, ec])
        return {
          errorCode: ec,
          name: r.readString(false),
          isInternal: r.readBoolean(),
          partitions: r.readArray(
            (r2, j) => {
              const pec = r2.readInt16()
              if (pec !== 0) errors.push([`/topics/${i}/partitions/${j}`, pec])
              return {
                errorCode: pec,
                partitionIndex: r2.readInt32(),
                leaderId: r2.readInt32(),
                leaderEpoch: r2.readInt32(),
                replicaNodes: r2.readArray(() => r2.readInt32(), false, false)!,
                isrNodes: r2.readArray(() => r2.readInt32(), false, false)!,
                offlineReplicas: r2.readArray(() => r2.readInt32(), false, false)!
              }
            },
            false,
            false
          ),
          topicAuthorizedOperations: r.readInt32()
        }
      },
      false,
      false
    ),
    clusterAuthorizedOperations: reader.readInt32()
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}

export const api = createAPI<MetadataRequest, MetadataResponse>(3, 8, createRequest, parseResponse, false, false)
