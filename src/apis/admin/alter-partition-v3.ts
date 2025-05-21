import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface AlterPartitionRequestISR {
  brokerId: number
  brokerEpoch: bigint
}

export interface AlterPartitionRequestPartition {
  partitionIndex: number
  leaderEpoch: number
  newIsrWithEpochs: AlterPartitionRequestISR[]
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
  isr: number
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
  AlterPartition Request (Version: 3) => broker_id broker_epoch [topics] TAG_BUFFER
    broker_id => INT32
    broker_epoch => INT64
    topics => topic_id [partitions] TAG_BUFFER
      topic_id => UUID
      partitions => partition_index leader_epoch [new_isr_with_epochs] leader_recovery_state partition_epoch TAG_BUFFER
        partition_index => INT32
        leader_epoch => INT32
        new_isr_with_epochs => broker_id broker_epoch TAG_BUFFER
          broker_id => INT32
          broker_epoch => INT64
        leader_recovery_state => INT8
        partition_epoch => INT32
*/
export function createRequest (brokerId: number, brokerEpoch: bigint, topic: AlterPartitionRequestTopic[]): Writer {
  return Writer.create()
    .appendInt32(brokerId)
    .appendInt64(brokerEpoch)
    .appendArray(topic, (w, t) => {
      w.appendString(t.topicId).appendArray(t.partitions, (w, p) => {
        w.appendInt32(p.partitionIndex)
          .appendInt32(p.leaderEpoch)
          .appendArray(p.newIsrWithEpochs, (w, n) => {
            w.appendInt32(n.brokerId).appendInt64(n.brokerEpoch)
          })
          .appendInt8(p.leaderRecoveryState)
          .appendInt32(p.partitionEpoch)
      })
    })
    .appendTaggedFields()
}

/*
  AlterPartition Response (Version: 3) => throttle_time_ms error_code [topics] TAG_BUFFER
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
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): AlterPartitionResponse {
  const errors: ResponseErrorWithLocation[] = []

  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()

  if (errorCode !== 0) {
    errors.push(['/', errorCode])
  }

  const response: AlterPartitionResponse = {
    throttleTimeMs,
    errorCode,
    topics: reader.readArray((r, i) => {
      return {
        topicId: r.readString(),
        partitions: r.readArray((r, j) => {
          const partition = {
            partitionIndex: r.readInt32(),
            errorCode: r.readInt16(),
            leaderId: r.readInt32(),
            leaderEpoch: r.readInt32(),
            isr: r.readInt32(),
            leaderRecoveryState: r.readInt8(),
            partitionEpoch: r.readInt32()
          }

          if (partition.errorCode !== 0) {
            errors.push([`/topics/${i}/partitions/${j}`, partition.errorCode])
          }

          return partition
        })
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<AlterPartitionRequest, AlterPartitionResponse>(56, 3, createRequest, parseResponse)
