import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type InitProducerIdRequest = Parameters<typeof createRequest>

export interface InitProducerIdResponse {
  throttleTimeMs: number
  errorCode: number
  producerId: bigint
  producerEpoch: number
}

/*
  InitProducerId Request (Version: 2) => transactional_id transaction_timeout_ms TAG_BUFFER
  InitProducerId Response (Version: 2) => throttle_time_ms error_code producer_id producer_epoch TAG_BUFFER
*/
export function createRequest (
  transactionalId: NullableString,
  transactionTimeoutMs: number,
  _producerId?: bigint,
  _producerEpoch?: number
): Writer {
  return Writer.create().appendString(transactionalId, true).appendInt32(transactionTimeoutMs).appendTaggedFields()
}

export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): InitProducerIdResponse {
  const response = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    producerId: reader.readInt64(),
    producerEpoch: reader.readInt16()
  }
  reader.readTaggedFields()

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<InitProducerIdRequest, InitProducerIdResponse>(
  22,
  2,
  createRequest,
  parseResponse
)
