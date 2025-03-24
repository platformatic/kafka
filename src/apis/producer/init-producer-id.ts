import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export interface InitProducerIdRequest {
  transactionalId: NullableString
  transactionTimeoutMs: number
  producerId: bigint
  producerEpoch: number
}

export interface InitProducerIdResponseCoordinator {
  key: string
  nodeId: number
  host: string
  port: number
  errorCode: number
  errorMessage: NullableString
}

export interface InitProducerIdResponse {
  throttleTimeMs: number
  errorCode: number
  producerId: bigint
  producerEpoch: number
}

/*
  InitProducerId Request (Version: 5) => transactional_id transaction_timeout_ms producer_id producer_epoch TAG_BUFFER
    transactional_id => COMPACT_NULLABLE_STRING
    transaction_timeout_ms => INT32
    producer_id => INT64
    producer_epoch => INT16
*/
export function createRequest (request: InitProducerIdRequest): Writer {
  return Writer.create()
    .appendString(request.transactionalId, true)
    .appendInt32(request.transactionTimeoutMs)
    .appendInt64(request.producerId)
    .appendInt16(request.producerEpoch)
    .appendTaggedFields()
}

/*
  InitProducerId Response (Version: 5) => throttle_time_ms error_code producer_id producer_epoch TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    producer_id => INT64
    producer_epoch => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): InitProducerIdResponse {
  const reader = Reader.from(raw)

  const response = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    producerId: reader.readInt64(),
    producerEpoch: reader.readInt16()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const initProducerIdV5 = createAPI<InitProducerIdRequest, InitProducerIdResponse>(
  22,
  5,
  createRequest,
  parseResponse
)
