import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export const TransactionStates = [
  'EMPTY',
  'ONGOING',
  'PREPARE_ABORT',
  'COMMITTING',
  'ABORTING',
  'COMPLETE_COMMIT',
  'COMPLETE_ABORT'
] as const
export type KafkaTransactionState =
  | 'Empty'
  | 'Ongoing'
  | 'PrepareCommit'
  | 'PrepareAbort'
  | 'CompleteCommit'
  | 'CompleteAbort'
  | 'Dead'
  | 'PrepareEpochFence'
export type TransactionState = (typeof TransactionStates)[number] | KafkaTransactionState

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
  ListTransactions Request (Version: 0) => [state_filters] [producer_id_filters] TAG_BUFFER
    state_filters => COMPACT_STRING
    producer_id_filters => INT64
*/
export function createRequest (
  stateFilters: Array<TransactionState | KafkaTransactionState>,
  producerIdFilters: bigint[],
  _durationFilter: bigint,
  _transactionalIdPattern: NullableString
): Writer {
  return Writer.create()
    .appendArray(stateFilters, (w, state) => w.appendString(normalizeState(state)), true, false)
    .appendArray(producerIdFilters, (w, p) => w.appendInt64(p), true, false)
    .appendTaggedFields()
}

/*
  ListTransactions Response (Version: 0) => throttle_time_ms error_code [unknown_state_filters] [transaction_states] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    unknown_state_filters => COMPACT_STRING
    transaction_states => transactional_id producer_id transaction_state TAG_BUFFER
      transactional_id => COMPACT_STRING
      producer_id => INT64
      transaction_state => COMPACT_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ListTransactionsResponse {
  const response: ListTransactionsResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    unknownStateFilters: reader.readArray(r => r.readString(), true, false)!,
    transactionStates: reader.readArray(r => {
      return {
        transactionalId: r.readString(),
        producerId: r.readInt64(),
        transactionState: r.readString()
      }
    })
  }

  reader.readTaggedFields()

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

function normalizeState (state: TransactionState | KafkaTransactionState): KafkaTransactionState {
  switch (state) {
    case 'EMPTY': return 'Empty'
    case 'ONGOING': return 'Ongoing'
    case 'PREPARE_ABORT': return 'PrepareAbort'
    case 'COMMITTING': return 'PrepareCommit'
    case 'ABORTING': return 'PrepareAbort'
    case 'COMPLETE_COMMIT': return 'CompleteCommit'
    case 'COMPLETE_ABORT': return 'CompleteAbort'
    default: return state
  }
}

export const api = createAPI<ListTransactionsRequest, ListTransactionsResponse>(66, 0, createRequest, parseResponse)
