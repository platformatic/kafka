import type BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type UnregisterBrokerRequest = Parameters<typeof createRequest>

export interface UnregisterBrokerResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
}

/*
UnregisterBroker Request (Version: 0) => broker_id TAG_BUFFER
  broker_id => INT32
*/
export function createRequest (brokerId: number): Writer {
  return Writer.create().appendInt32(brokerId).appendTaggedFields()
}

/*
  UnregisterBroker Response (Version: 0) => throttle_time_ms error_code error_message TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): UnregisterBrokerResponse {
  const reader = Reader.from(raw)

  const response: UnregisterBrokerResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    errorMessage: reader.readNullableString()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const api = createAPI<UnregisterBrokerRequest, UnregisterBrokerResponse>(64, 0, createRequest, parseResponse)
