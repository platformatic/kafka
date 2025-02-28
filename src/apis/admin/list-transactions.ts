import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { type TransactionState } from '../enumerations.ts'
import { createAPI } from '../index.ts'

export type ListTransactionsRequest = Parameters<typeof createRequest>

export interface ListTransactionsResponseTransactionState {
  transactionalId: string
  producerId: bigint
  transactionState: string
}

export interface ListTransactionsResponse {
  throttleTimeMs: number
  errorCode: number
  unknownStateFilters: string[]
  transactionStates: ListTransactionsResponseTransactionState[]
}

/*
  ListTransactions Request (Version: 1) => [state_filters] [producer_id_filters] duration_filter TAG_BUFFER
    state_filters => COMPACT_STRING
    producer_id_filters => INT64
    duration_filter => INT64
*/
function createRequest (stateFilters: TransactionState[], producerIdFilters: bigint[], durationFilter: bigint): Writer {
  return Writer.create()
    .appendArray(stateFilters, (w, t) => w.appendString(t), true, false)
    .appendArray(producerIdFilters, (w, p) => w.appendInt64(p), true, false)
    .appendInt64(durationFilter)
    .appendTaggedFields()
}

/*
  ListTransactions Response (Version: 1) => throttle_time_ms error_code [unknown_state_filters] [transaction_states] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    unknown_state_filters => COMPACT_STRING
    transaction_states => transactional_id producer_id transaction_state TAG_BUFFER
      transactional_id => COMPACT_STRING
      producer_id => INT64
      transaction_state => COMPACT_STRING
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): ListTransactionsResponse {
  const reader = Reader.from(raw)

  const response: ListTransactionsResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    unknownStateFilters: reader.readArray(r => r.readString()!, true, false)!,
    transactionStates: reader.readArray(r => {
      return {
        transactionalId: r.readString()!,
        producerId: r.readInt64(),
        transactionState: r.readString()!
      }
    })!
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { errors: { '': response.errorCode }, response })
  }

  return response
}

export const listTransactionsV0 = createAPI<ListTransactionsRequest, ListTransactionsResponse>(
  66,
  1,
  createRequest,
  parseResponse
)
