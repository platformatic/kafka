import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface AlterPartitionRequestPartition {
  partitionIndex: number
  leaderEpoch: number
  newIsrWithEpochs: AlterPartitionRequestBroker[]
  leaderRecoveryState?: number
  partitionEpoch: number
}

export interface AlterPartitionRequestBroker {
  brokerId: number
  brokerEpoch: bigint
}

export interface AlterPartitionRequestTopic {
  topicName: string
  partitions: AlterPartitionRequestPartition[]
}

export type AlterPartitionRequest = Parameters<typeof createRequest>

export interface AlterPartitionResponsePartition {
  partitionIndex: number
  errorCode: number
  leaderId: number
  leaderEpoch: number
  isr: number[]
  leaderRecoveryState: number
  partitionEpoch: number
}

export interface AlterPartitionResponseTopic {
  topicName: string
  topicId: string
  partitions: AlterPartitionResponsePartition[]
}

export interface AlterPartitionResponse {
  throttleTimeMs: number
  errorCode: number
  topics: AlterPartitionResponseTopic[]
}

/*
  AlterPartition Request (Version: 0) => broker_id broker_epoch [topics] TAG_BUFFER
    broker_id => INT32
    broker_epoch => INT64
    topics => topic_name [partitions] TAG_BUFFER
      topic_name => COMPACT_STRING
      partitions => partition_index leader_epoch [new_isr] partition_epoch TAG_BUFFER
        partition_index => INT32
        leader_epoch => INT32
        new_isr => INT32
        partition_epoch => INT32
*/
export function createRequest (brokerId: number, brokerEpoch: bigint, topics: AlterPartitionRequestTopic[]): Writer {
  return Writer.create()
    .appendInt32(brokerId)
    .appendInt64(brokerEpoch)
    .appendArray(topics, (writer, topic) => {
      writer.appendString(topic.topicName).appendArray(topic.partitions, (writer, partition) => {
        writer
          .appendInt32(partition.partitionIndex)
          .appendInt32(partition.leaderEpoch)
          .appendArray(partition.newIsrWithEpochs, (writer, broker) => writer.appendInt32(broker.brokerId), true, false)
          .appendInt32(partition.partitionEpoch)
      })
    })
    .appendTaggedFields()
}

/*
  AlterPartition Response (Version: 0) => throttle_time_ms error_code [topics] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    topics => topic_name [partitions] TAG_BUFFER
      topic_name => COMPACT_STRING
      partitions => partition_index error_code leader_id leader_epoch [isr] partition_epoch TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        leader_id => INT32
        leader_epoch => INT32
        isr => INT32
        partition_epoch => INT32
*/
export function parseResponse (_correlationId: number, apiKey: number, apiVersion: number, reader: Reader): AlterPartitionResponse {
  const errors: ResponseErrorWithLocation[] = []
  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()

  if (errorCode !== 0) {
    errors.push(['/', [errorCode, null]])
  }

  const response: AlterPartitionResponse = {
    throttleTimeMs,
    errorCode,
    topics: reader.readArray((reader, topicIndex) => ({
      topicName: reader.readString(),
      topicId: '00000000-0000-0000-0000-000000000000',
      partitions: reader.readArray((reader, partitionIndex) => {
        const partition = {
          partitionIndex: reader.readInt32(),
          errorCode: reader.readInt16(),
          leaderId: reader.readInt32(),
          leaderEpoch: reader.readInt32(),
          isr: reader.readArray(reader => reader.readInt32(), true, false),
          leaderRecoveryState: 0,
          partitionEpoch: reader.readInt32()
        }

        if (partition.errorCode !== 0) {
          errors.push([`/topics/${topicIndex}/partitions/${partitionIndex}`, [partition.errorCode, null]])
        }

        return partition
      })
    }))
  }

  reader.readTaggedFields()

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<AlterPartitionRequest, AlterPartitionResponse>(56, 0, createRequest, parseResponse)
