import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import type { AlterPartitionRequestBroker } from './alter-partition-v0.ts'

export interface AlterPartitionRequestPartition {
  partitionIndex: number
  leaderEpoch: number
  newIsrWithEpochs: AlterPartitionRequestBroker[]
  leaderRecoveryState: number
  partitionEpoch: number
}

export interface AlterPartitionRequestTopic {
  topicId: string
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
  topicId: string
  partitions: AlterPartitionResponsePartition[]
}

export interface AlterPartitionResponse {
  throttleTimeMs: number
  errorCode: number
  topics: AlterPartitionResponseTopic[]
}

/*
  AlterPartition Request (Version: 2) => broker_id broker_epoch [topics] TAG_BUFFER
    broker_id => INT32
    broker_epoch => INT64
    topics => topic_id [partitions] TAG_BUFFER
      topic_id => UUID
      partitions => partition_index leader_epoch [new_isr] leader_recovery_state partition_epoch TAG_BUFFER
        partition_index => INT32
        leader_epoch => INT32
        new_isr => INT32
        leader_recovery_state => INT8
        partition_epoch => INT32
*/
export function createRequest (brokerId: number, brokerEpoch: bigint, topics: AlterPartitionRequestTopic[]): Writer {
  return Writer.create()
    .appendInt32(brokerId)
    .appendInt64(brokerEpoch)
    .appendArray(topics, (writer, topic) => {
      writer.appendUUID(topic.topicId).appendArray(topic.partitions, (writer, partition) => {
        writer
          .appendInt32(partition.partitionIndex)
          .appendInt32(partition.leaderEpoch)
          .appendArray(partition.newIsrWithEpochs, (writer, broker) => writer.appendInt32(broker.brokerId), true, false)
          .appendInt8(partition.leaderRecoveryState)
          .appendInt32(partition.partitionEpoch)
      })
    })
    .appendTaggedFields()
}

/*
  AlterPartition Response (Version: 2) => throttle_time_ms error_code [topics] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    topics => topic_id [partitions] TAG_BUFFER
      topic_id => UUID
      partitions => partition_index error_code leader_id leader_epoch [isr] leader_recovery_state partition_epoch TAG_BUFFER
        partition_index => INT32
        error_code => INT16
        leader_id => INT32
        leader_epoch => INT32
        isr => INT32
        leader_recovery_state => INT8
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
      topicId: reader.readUUID(),
      partitions: reader.readArray((reader, partitionIndex) => {
        const partition = {
          partitionIndex: reader.readInt32(),
          errorCode: reader.readInt16(),
          leaderId: reader.readInt32(),
          leaderEpoch: reader.readInt32(),
          isr: reader.readArray(reader => reader.readInt32(), true, false),
          leaderRecoveryState: reader.readInt8(),
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

export const api = createAPI<AlterPartitionRequest, AlterPartitionResponse>(56, 2, createRequest, parseResponse)
