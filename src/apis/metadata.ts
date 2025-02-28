import BufferList from 'bl'
import { ERROR_RESPONSE_WITH_ERROR, KafkaError } from '../error.ts'
import {
  EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE,
  INT8_SIZE,
  sizeOfCompactable,
  sizeOfCompactableLength,
  UUID_SIZE
} from '../protocol/definitions.ts'
import { Reader } from '../protocol/reader.ts'
import { Writer } from '../protocol/writer.ts'
import { createAPI } from './index.ts'

export type MetadataRequest = [
  topics: string[],
  allowAutoTopicCreation: boolean,
  includeTopicAuthorizedOperations: boolean
]

export interface MetadataResponsePartition {
  errorCode: number
  partitionIndex: number
  leaderId: number
  leaderEpoch: number
  replicaNodes: number
  isrNodes: number
  offlineReplicas: number
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
  rack: string | null
}

export interface MetadataResponse {
  throttleTimeMs: number
  brokers: MetadataResponseBroker[]
  clusterId: string | null
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

  Buffer allocations: topics + 2
*/
function createRequest (
  topics: string[],
  allowAutoTopicCreation: boolean = false,
  includeTopicAuthorizedOperations: boolean = false
): BufferList {
  const buffer = new BufferList()
  const writer = Writer.create(sizeOfCompactableLength(topics)).writeCompactableLength(topics).appendTo(buffer)

  for (const topic of topics) {
    writer
      .reset(UUID_SIZE + sizeOfCompactable(topic) + EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE)
      .skip(UUID_SIZE)
      .writeCompactable(topic)
      .writeTaggedFields()
      .appendTo(buffer)
  }

  writer
    .reset(INT8_SIZE + INT8_SIZE + EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE)
    .writeBoolean(allowAutoTopicCreation)
    .writeBoolean(includeTopicAuthorizedOperations)
    .writeTaggedFields()
    .appendTo(buffer)

  return buffer
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
function parseResponse (raw: BufferList): MetadataResponse {
  const reader = Reader.from(raw)
  let hasError = false

  const response: MetadataResponse = {
    throttleTimeMs: reader.readInt32(),
    brokers: [],
    clusterId: null,
    controllerId: 0,
    topics: []
  }

  const brokersLength = reader.readCollectionSize()

  for (let i = 0; i < brokersLength; i++) {
    response.brokers.push({
      nodeId: reader.readInt32(),
      host: reader.readCompactableString()!,
      port: reader.readInt32(),
      rack: reader.readCompactableString()
    })

    reader.readTaggedFields()
  }

  response.clusterId = reader.readCompactableString()
  response.controllerId = reader.readInt32()

  const topicsLength = reader.readCollectionSize()

  for (let i = 0; i < topicsLength; i++) {
    const errorCode = reader.readInt16()
    const name = reader.readCompactableString()!
    const topicId = reader.readUUID()
    const isInternal = reader.readBoolean()

    if (errorCode !== 0) {
      hasError = true
    }

    const partitions: MetadataResponsePartition[] = []
    const partitionsLength = reader.readCollectionSize()

    for (let j = 0; j < partitionsLength; j++) {
      const errorCode = reader.readInt16()
      const partitionIndex = reader.readInt32()
      const leaderId = reader.readInt32()
      const leaderEpoch = reader.readInt32()
      const replicaNodes = reader.readInt32()
      const isrNodes = reader.readInt32()
      const offlineReplicas = reader.readInt32()

      if (errorCode !== 0) {
        hasError = true
      }

      partitions.push({
        errorCode,
        partitionIndex,
        leaderId,
        leaderEpoch,
        replicaNodes,
        isrNodes,
        offlineReplicas
      })

      reader.readTaggedFields()
    }

    response.topics.push({
      errorCode,
      name,
      topicId,
      isInternal,
      partitions,
      topicAuthorizedOperations: reader.readInt32()
    })

    reader.readTaggedFields()
  }

  if (hasError) {
    throw new KafkaError('Received response with error while executing API Metadata(v12)', {
      code: ERROR_RESPONSE_WITH_ERROR,
      response
    })
  }

  return response
}

export const metadataV12 = createAPI<MetadataRequest, MetadataResponse>(3, 12, createRequest, parseResponse)
