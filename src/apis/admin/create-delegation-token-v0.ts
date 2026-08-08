import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import { type NullableString } from '../../protocol/definitions.ts'

export interface CreateDelegationTokenRequestRenewer {
  principalType: string
  principalName: string
}

export type CreateDelegationTokenRequest = Parameters<typeof createRequest>

export interface CreateDelegationTokenResponse {
  errorCode: number
  principalType: string
  principalName: string
  tokenRequesterPrincipalType: string
  tokenRequesterPrincipalName: string
  issueTimestampMs: bigint
  expiryTimestampMs: bigint
  maxTimestampMs: bigint
  tokenId: string
  hmac: Buffer
  throttleTimeMs: number
}

/*
  CreateDelegationToken Request (Version: 0) => [renewers] max_lifetime_ms
    renewers => principal_type principal_name
      principal_type => STRING
      principal_name => STRING
    max_lifetime_ms => INT64
*/
export function createRequest (
  _ownerPrincipalType: NullableString,
  _ownerPrincipalName: NullableString,
  renewers: CreateDelegationTokenRequestRenewer[],
  maxLifetimeMs: bigint
): Writer {
  return Writer.create()
    .appendArray(
      renewers,
      (w, r) => w.appendString(r.principalType, false).appendString(r.principalName, false),
      false,
      false
    )
    .appendInt64(maxLifetimeMs)
}

/*
  CreateDelegationToken Response (Version: 0) => error_code principal_type principal_name issue_timestamp_ms expiry_timestamp_ms max_timestamp_ms token_id hmac throttle_time_ms
    error_code => INT16
    principal_type => STRING
    principal_name => STRING
    issue_timestamp_ms => INT64
    expiry_timestamp_ms => INT64
    max_timestamp_ms => INT64
    token_id => STRING
    hmac => BYTES
    throttle_time_ms => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): CreateDelegationTokenResponse {
  const response: CreateDelegationTokenResponse = {
    errorCode: reader.readInt16(),
    principalType: reader.readString(false),
    principalName: reader.readString(false),
    tokenRequesterPrincipalType: '',
    tokenRequesterPrincipalName: '',
    issueTimestampMs: reader.readInt64(),
    expiryTimestampMs: reader.readInt64(),
    maxTimestampMs: reader.readInt64(),
    tokenId: reader.readString(false),
    hmac: reader.readBytes(false),
    throttleTimeMs: reader.readInt32()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<CreateDelegationTokenRequest, CreateDelegationTokenResponse>(
  38,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
