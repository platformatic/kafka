import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface AddPartitionsToTxnRequestTopic {
  name: string
  partitions: number[]
}

export interface AddPartitionsToTxnRequestTransaction {
  transactionalId: string
  producerId: bigint
  producerEpoch: number
  verifyOnly: boolean
  topics: AddPartitionsToTxnRequestTopic[]
}

export type AddPartitionsToTxnRequest = Parameters<typeof createRequest>

export interface AddPartitionsToTxnResponsePartition {
  partitionIndex: number
  partitionErrorCode: number
}

export interface AddPartitionsToTxnResponseTopic {
  name: string
  resultsByPartition: AddPartitionsToTxnResponsePartition[]
}

export interface AddPartitionsToTxnResponseTransaction {
  transactionalId: string
  topicResults: AddPartitionsToTxnResponseTopic[]
}

export interface AddPartitionsToTxnResponse {
  throttleTimeMs: number
  errorCode: number
  resultsByTransaction: AddPartitionsToTxnResponseTransaction[]
}

/*
  AddPartitionsToTxn Request (Version: 3) => transactional_id producer_id producer_epoch [topics] TAG_BUFFER
    transactional_id => COMPACT_STRING
    producer_id => INT64
    producer_epoch => INT16
    topics => name [partitions] TAG_BUFFER
      name => COMPACT_STRING
      partitions => INT32
*/
export function createRequest (transactions: AddPartitionsToTxnRequestTransaction[]): Writer {
  const transaction = transactions[0]

  return Writer.create()
    .appendString(transaction.transactionalId)
    .appendInt64(transaction.producerId)
    .appendInt16(transaction.producerEpoch)
    .appendArray(transaction.topics, (w, topic) => {
      w.appendString(topic.name).appendArray(topic.partitions, (w, partition) => w.appendInt32(partition), true, false)
    })
    .appendTaggedFields()
}

/*
  AddPartitionsToTxn Response (Version: 3) => throttle_time_ms [results_by_topic] TAG_BUFFER
    throttle_time_ms => INT32
    results_by_topic => name [results_by_partition] TAG_BUFFER
      name => COMPACT_STRING
      results_by_partition => partition_index partition_error_code TAG_BUFFER
        partition_index => INT32
        partition_error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): AddPartitionsToTxnResponse {
  const errors: ResponseErrorWithLocation[] = []

  const throttleTimeMs = reader.readInt32()

  const response: AddPartitionsToTxnResponse = {
    throttleTimeMs,
    errorCode: 0,
    resultsByTransaction: [
      {
        transactionalId: '',
        topicResults: reader.readArray((r, j) => {
          const topic = {
            name: r.readString(),
            resultsByPartition: r.readArray((r, k) => {
              const partition = {
                partitionIndex: r.readInt32(),
                partitionErrorCode: r.readInt16()
              }
              r.readTaggedFields()

              if (partition.partitionErrorCode !== 0) {
                errors.push([
                  `/results_by_transaction/0/topic_results/${j}/results_by_partitions/${k}`,
                  [partition.partitionErrorCode, null]
                ])
              }

              return partition
            }, true, false)
          }
          r.readTaggedFields()
          return topic
        }, true, false)
      }
    ]
  }

  reader.readTaggedFields()

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<AddPartitionsToTxnRequest, AddPartitionsToTxnResponse>(24, 3, createRequest, parseResponse)
