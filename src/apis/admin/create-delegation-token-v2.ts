import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { createAPI } from '../definitions.ts'

/*
  CreateDelegationToken Request (Version: 2) => [renewers] max_lifetime_ms TAG_BUFFER
  CreateDelegationToken Response (Version: 2) => error_code principal_type principal_name issue_timestamp_ms expiry_timestamp_ms max_timestamp_ms token_id hmac throttle_time_ms TAG_BUFFER
*/
import type {
  CreateDelegationTokenRequestRenewer,
  CreateDelegationTokenResponse
} from './create-delegation-token-v0.ts'

export type { CreateDelegationTokenRequestRenewer, CreateDelegationTokenResponse }

export type CreateDelegationTokenRequest = Parameters<typeof createRequest>

export function createRequest (
  _ownerPrincipalType: NullableString,
  _ownerPrincipalName: NullableString,
  renewers: CreateDelegationTokenRequestRenewer[],
  maxLifetimeMs: bigint
): Writer {
  return Writer.create()
    .appendArray(
      renewers,
      (writer, renewer) => writer.appendString(renewer.principalType).appendString(renewer.principalName)
    )
    .appendInt64(maxLifetimeMs)
    .appendTaggedFields()
}

export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): CreateDelegationTokenResponse {
  const response: CreateDelegationTokenResponse = {
    errorCode: reader.readInt16(),
    principalType: reader.readString(),
    principalName: reader.readString(),
    tokenRequesterPrincipalType: '',
    tokenRequesterPrincipalName: '',
    issueTimestampMs: reader.readInt64(),
    expiryTimestampMs: reader.readInt64(),
    maxTimestampMs: reader.readInt64(),
    tokenId: reader.readString(),
    hmac: reader.readBytes(),
    throttleTimeMs: reader.readInt32()
  }
  reader.readTaggedFields()

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<CreateDelegationTokenRequest, CreateDelegationTokenResponse>(38, 2, createRequest, parseResponse)
