import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type AddOffsetsToTxnRequest = Parameters<typeof createRequest>

export interface AddOffsetsToTxnResponse {
  throttleTimeMs: number
  errorCode: number
}

/*
  AddOffsetsToTxn Request (Version: 1) => transactional_id producer_id producer_epoch group_id
    transactional_id => STRING
    producer_id => INT64
    producer_epoch => INT16
    group_id => STRING
*/
export function createRequest (
  transactionalId: string,
  producerId: bigint,
  producerEpoch: number,
  groupId: string
): Writer {
  return Writer.create()
    .appendString(transactionalId, false)
    .appendInt64(producerId)
    .appendInt16(producerEpoch)
    .appendString(groupId, false)
}

/*
  AddOffsetsToTxn Response (Version: 1) => throttle_time_ms error_code
    throttle_time_ms => INT32
    error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): AddOffsetsToTxnResponse {
  const response = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<AddOffsetsToTxnRequest, AddOffsetsToTxnResponse>(25, 1, createRequest, parseResponse, false, false)
