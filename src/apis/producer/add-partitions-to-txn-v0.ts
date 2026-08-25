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
  AddPartitionsToTxn Request (Version: 0) => transactional_id producer_id producer_epoch [topics]
    transactional_id => STRING
    producer_id => INT64
    producer_epoch => INT16
    topics => name [partitions]
      name => STRING
      partitions => INT32
*/
export function createRequest (
  transactions: AddPartitionsToTxnRequestTransaction[]
): Writer {
  const transaction = transactions[0]

  return Writer.create()
    .appendString(transaction.transactionalId, false)
    .appendInt64(transaction.producerId)
    .appendInt16(transaction.producerEpoch)
    .appendArray(
      transaction.topics,
      (w, topic) => {
        w.appendString(topic.name, false).appendArray(
          topic.partitions,
          (w, partition) => w.appendInt32(partition),
          false,
          false
        )
      },
      false,
      false
    )
}

/*
  AddPartitionsToTxn Response (Version: 0) => throttle_time_ms [errors]
    throttle_time_ms => INT32
    errors => name [partition_errors]
      name => STRING
      partition_errors => partition_index error_code
        partition_index => INT32
        error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): AddPartitionsToTxnResponse {
  const errors: ResponseErrorWithLocation[] = []
  const response: AddPartitionsToTxnResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: 0,
    // The legacy response has one implicit transaction. Preserve the current response shape for clients.
    resultsByTransaction: [
      {
        transactionalId: '',
        topicResults: reader.readArray(
          (r, topicIndex) => ({
            name: r.readString(false),
            resultsByPartition: r.readArray(
              (r, partitionIndex) => {
                const partition = { partitionIndex: r.readInt32(), partitionErrorCode: r.readInt16() }
                if (partition.partitionErrorCode !== 0) {
                  errors.push([
                    `/results_by_transaction/0/topic_results/${topicIndex}/results_by_partitions/${partitionIndex}`,
                    [partition.partitionErrorCode, null]
                  ])
                }
                return partition
              },
              false,
              false
            )
          }),
          false,
          false
        )
      }
    ]
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}

export const api = createAPI<AddPartitionsToTxnRequest, AddPartitionsToTxnResponse>(
  24,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
