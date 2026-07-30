import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import type {
  AlterPartitionRequestPartition,
  AlterPartitionRequestTopic,
  AlterPartitionResponse,
  AlterPartitionResponsePartition,
  AlterPartitionResponseTopic
} from './alter-partition-v2.ts'
import type { AlterPartitionRequestBroker } from './alter-partition-v0.ts'

export type {
  AlterPartitionRequestPartition,
  AlterPartitionRequestTopic,
  AlterPartitionResponse,
  AlterPartitionResponsePartition,
  AlterPartitionResponseTopic
}

export type AlterPartitionRequestISR = AlterPartitionRequestBroker

export type AlterPartitionRequest = Parameters<typeof createRequest>

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
export function createRequest (brokerId: number, brokerEpoch: bigint, topics: AlterPartitionRequestTopic[]): Writer {
  return Writer.create()
    .appendInt32(brokerId)
    .appendInt64(brokerEpoch)
    .appendArray(topics, (w, t) => {
      w.appendUUID(t.topicId).appendArray(t.partitions, (w, p) => {
        w.appendInt32(p.partitionIndex)
          .appendInt32(p.leaderEpoch)
          .appendArray(p.newIsrWithEpochs, (w, broker) => {
            w.appendInt32(broker.brokerId).appendInt64(broker.brokerEpoch)
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
    errors.push(['/', [errorCode, null]])
  }

  const response: AlterPartitionResponse = {
    throttleTimeMs,
    errorCode,
    topics: reader.readArray((r, i) => {
      const topic = {
        topicId: r.readUUID(),
        partitions: r.readArray((r, j) => {
          const partition = {
            partitionIndex: r.readInt32(),
            errorCode: r.readInt16(),
            leaderId: r.readInt32(),
            leaderEpoch: r.readInt32(),
            isr: r.readArray(r => r.readInt32(), true, false),
            leaderRecoveryState: r.readInt8(),
            partitionEpoch: r.readInt32()
          }

          if (partition.errorCode !== 0) {
            errors.push([`/topics/${i}/partitions/${j}`, [partition.errorCode, null]])
          }

          return partition
        })
      }
      return topic
    })
  }

  reader.readTaggedFields()

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<AlterPartitionRequest, AlterPartitionResponse>(56, 3, createRequest, parseResponse)
