import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export interface DescribeDelegationTokenRequestOwner {
  principalType: string
  principalName: string
}

export type DescribeDelegationTokenRequest = Parameters<typeof createRequest>

export interface DescribeDelegationTokenResponseRenewer {
  principalType: string
  principalName: string
}

export interface DescribeDelegationTokenResponseToken {
  principalType: string
  principalName: string
  tokenRequesterPrincipalType: string
  tokenRequesterPrincipalName: string
  issueTimestamp: bigint
  expiryTimestamp: bigint
  maxTimestamp: bigint
  tokenId: string
  hmac: Buffer
  renewers: DescribeDelegationTokenResponseRenewer[]
}

export interface DescribeDelegationTokenResponse {
  errorCode: number
  tokens: DescribeDelegationTokenResponseToken[]
  throttleTimeMs: number
}

/*
  DescribeDelegationToken Request (Version: 0) => [owners]
    owners => principal_type principal_name
      principal_type => STRING
      principal_name => STRING
*/
export function createRequest (owners: DescribeDelegationTokenRequestOwner[] | null): Writer {
  return Writer.create().appendArray(
    owners,
    (w, owner) => w.appendString(owner.principalType, false).appendString(owner.principalName, false),
    false,
    false
  )
}

/*
  DescribeDelegationToken Response (Version: 0) => error_code [tokens] throttle_time_ms
    error_code => INT16
    tokens => principal_type principal_name issue_timestamp expiry_timestamp max_timestamp token_id hmac [renewers]
      principal_type => STRING
      principal_name => STRING
      issue_timestamp => INT64
      expiry_timestamp => INT64
      max_timestamp => INT64
      token_id => STRING
      hmac => BYTES
      renewers => principal_type principal_name
        principal_type => STRING
        principal_name => STRING
    throttle_time_ms => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeDelegationTokenResponse {
  const response: DescribeDelegationTokenResponse = {
    errorCode: reader.readInt16(),
    tokens: reader.readArray(
      r => ({
        principalType: r.readString(false),
        principalName: r.readString(false),
        tokenRequesterPrincipalType: '',
        tokenRequesterPrincipalName: '',
        issueTimestamp: r.readInt64(),
        expiryTimestamp: r.readInt64(),
        maxTimestamp: r.readInt64(),
        tokenId: r.readString(false),
        hmac: r.readBytes(false),
        renewers: r.readArray(
          r => ({ principalType: r.readString(false), principalName: r.readString(false) }),
          false,
          false
        )
      }),
      false,
      false
    ),
    throttleTimeMs: reader.readInt32()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<DescribeDelegationTokenRequest, DescribeDelegationTokenResponse>(
  41,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
