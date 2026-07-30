import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

/*
  DescribeDelegationToken Request (Version: 2) => [owners] TAG_BUFFER
  DescribeDelegationToken Response (Version: 2) => error_code [tokens] throttle_time_ms TAG_BUFFER
*/
import type {
  DescribeDelegationTokenRequestOwner,
  DescribeDelegationTokenResponse,
  DescribeDelegationTokenResponseRenewer,
  DescribeDelegationTokenResponseToken
} from './describe-delegation-token-v0.ts'

export type {
  DescribeDelegationTokenRequestOwner,
  DescribeDelegationTokenResponse,
  DescribeDelegationTokenResponseRenewer,
  DescribeDelegationTokenResponseToken
}

export type DescribeDelegationTokenRequest = Parameters<typeof createRequest>

export function createRequest (owners: DescribeDelegationTokenRequestOwner[] | null): Writer {
  return Writer.create()
    .appendArray(owners, (writer, owner) => writer.appendString(owner.principalType).appendString(owner.principalName))
    .appendTaggedFields()
}

export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeDelegationTokenResponse {
  const response: DescribeDelegationTokenResponse = {
    errorCode: reader.readInt16(),
    tokens: reader.readArray(r => ({
      principalType: r.readString(),
      principalName: r.readString(),
      tokenRequesterPrincipalType: '',
      tokenRequesterPrincipalName: '',
      issueTimestamp: r.readInt64(),
      expiryTimestamp: r.readInt64(),
      maxTimestamp: r.readInt64(),
      tokenId: r.readString(),
      hmac: r.readBytes(),
      renewers: r.readArray(
        r => ({ principalType: r.readString(), principalName: r.readString() }) as DescribeDelegationTokenResponseRenewer
      )
    })) as DescribeDelegationTokenResponseToken[],
    throttleTimeMs: reader.readInt32()
  }
  reader.readTaggedFields()

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<DescribeDelegationTokenRequest, DescribeDelegationTokenResponse>(41, 2, createRequest, parseResponse)
