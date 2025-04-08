import type BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export type DescribeTransactionsRequest = Parameters<typeof createRequest>

export interface DescribeTransactionsResponseTopic {
  topic: string
  partitions: number[]
}

export interface DescribeTransactionsResponseTransactionState {
  errorCode: number
  transactionalId: string
  transactionState: string
  transactionTimeoutMs: number
  transactionStartTimeMs: bigint
  producerId: bigint
  producerEpoch: number
  topics: DescribeTransactionsResponseTopic[]
}

export interface DescribeTransactionsResponse {
  throttleTimeMs: number
  transactionStates: DescribeTransactionsResponseTransactionState[]
}

/*
DescribeTransactions Request (Version: 0) => [transactional_ids] TAG_BUFFER
  transactional_ids => COMPACT_STRING
*/
export function createRequest (transactionalIds: string[]): Writer {
  return Writer.create()
    .appendArray(transactionalIds, (w, t) => w.appendString(t), true, false)
    .appendTaggedFields()
}

/*
DescribeTransactions Response (Version: 0) => throttle_time_ms [transaction_states] TAG_BUFFER
  throttle_time_ms => INT32
  transaction_states => error_code transactional_id transaction_state transaction_timeout_ms transaction_start_time_ms producer_id producer_epoch [topics] TAG_BUFFER
    error_code => INT16
    transactional_id => COMPACT_STRING
    transaction_state => COMPACT_STRING
    transaction_timeout_ms => INT32
    transaction_start_time_ms => INT64
    producer_id => INT64
    producer_epoch => INT16
    topics => topic [partitions] TAG_BUFFER
      topic => COMPACT_STRING
      partitions => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): DescribeTransactionsResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: DescribeTransactionsResponse = {
    throttleTimeMs: reader.readInt32(),
    transactionStates: reader.readArray((r, i) => {
      const state = {
        errorCode: r.readInt16(),
        transactionalId: r.readString(),
        transactionState: r.readString(),
        transactionTimeoutMs: r.readInt32(),
        transactionStartTimeMs: r.readInt64(),
        producerId: r.readInt64(),
        producerEpoch: r.readInt16(),
        topics: r.readArray(r => {
          return {
            topic: r.readString(),
            partitions: r.readArray(r => r.readInt32(), true, false)!
          }
        })
      }

      if (state.errorCode !== 0) {
        errors.push([`/transaction_states/${i}`, state.errorCode])
      }

      return state
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<DescribeTransactionsRequest, DescribeTransactionsResponse>(
  65,
  0,
  createRequest,
  parseResponse
)
