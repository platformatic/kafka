import type BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type EndTxnRequest = Parameters<typeof createRequest>

export interface EndTxnResponse {
  throttleTimeMs: number
  errorCode: number
}

/*
  EndTxn Request (Version: 4) => transactional_id producer_id producer_epoch committed TAG_BUFFER
    transactional_id => COMPACT_STRING
    producer_id => INT64
    producer_epoch => INT16
    committed => BOOLEAN
*/
export function createRequest (
  transactionalId: string,
  producerId: bigint,
  producerEpoch: number,
  committed: boolean
): Writer {
  return Writer.create()
    .appendString(transactionalId, true)
    .appendInt64(producerId)
    .appendInt16(producerEpoch)
    .appendBoolean(committed)
    .appendTaggedFields()
}

/*
  EndTxn Response (Version: 4) => throttle_time_ms error_code TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): EndTxnResponse {
  const reader = Reader.from(raw)

  const response = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const endTxnV4 = createAPI<EndTxnRequest, EndTxnResponse>(26, 4, createRequest, parseResponse)
