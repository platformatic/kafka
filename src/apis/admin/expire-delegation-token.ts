import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../index.ts'

export type ExpireDelegationTokenRequest = Parameters<typeof createRequest>

export interface ExpireDelegationTokenResponse {
  errorCode: number
  expiryTimestampMs: bigint
  throttleTimeMs: number
}

/*
  ExpireDelegationToken Request (Version: 2) => hmac expiry_time_period_ms TAG_BUFFER
    hmac => COMPACT_BYTES
    expiry_time_period_ms => INT64
*/
function createRequest (hmac: Buffer, expiryTimePeriodMs: bigint): Writer {
  return Writer.create().appendBytes(hmac).appendInt64(expiryTimePeriodMs).appendTaggedFields()
}

/*
  ExpireDelegationToken Response (Version: 2) => error_code expiry_timestamp_ms throttle_time_ms TAG_BUFFER
    error_code => INT16
    expiry_timestamp_ms => INT64
    throttle_time_ms => INT32
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): ExpireDelegationTokenResponse {
  const reader = Reader.from(raw)

  const response: ExpireDelegationTokenResponse = {
    errorCode: reader.readInt16(),
    expiryTimestampMs: reader.readInt64(),
    throttleTimeMs: reader.readInt32()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { errors: { '': response.errorCode }, response })
  }

  return response
}

export const expireDelegationTokenV2 = createAPI<ExpireDelegationTokenRequest, ExpireDelegationTokenResponse>(
  40,
  2,
  createRequest,
  parseResponse
)
