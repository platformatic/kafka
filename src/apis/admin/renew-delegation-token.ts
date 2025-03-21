import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type RenewDelegationTokenRequest = Parameters<typeof createRequest>

export interface RenewDelegationTokenResponse {
  errorCode: number
  expiryTimestampMs: bigint
  throttleTimeMs: number
}

/*
  RenewDelegationToken Request (Version: 2) => hmac renew_period_ms TAG_BUFFER
    hmac => COMPACT_BYTES
    renew_period_ms => INT64
*/
function createRequest (hmac: Buffer, renewPeriodMs: bigint): Writer {
  return Writer.create().appendBytes(hmac).appendInt64(renewPeriodMs).appendTaggedFields()
}

/*
  RenewDelegationToken Response (Version: 2) => error_code expiry_timestamp_ms throttle_time_ms TAG_BUFFER
    error_code => INT16
    expiry_timestamp_ms => INT64
    throttle_time_ms => INT32
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): RenewDelegationTokenResponse {
  const reader = Reader.from(raw)

  const response: RenewDelegationTokenResponse = {
    errorCode: reader.readInt16(),
    expiryTimestampMs: reader.readInt64(),
    throttleTimeMs: reader.readInt32()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const renewDelegationTokenV2 = createAPI<RenewDelegationTokenRequest, RenewDelegationTokenResponse>(
  39,
  2,
  createRequest,
  parseResponse
)
