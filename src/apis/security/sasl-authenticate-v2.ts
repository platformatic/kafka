import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { type API, createAPI } from '../definitions.ts'

export type SaslAuthenticateRequest = Parameters<typeof createRequest>

export interface SaslAuthenticateResponse {
  errorCode: number
  errorMessage: NullableString
  authBytes: Buffer
  sessionLifetimeMs: bigint
}

export type SASLAuthenticationAPI = API<[Buffer], SaslAuthenticateResponse>

/*
  SaslAuthenticate Request (Version: 2) => auth_bytes TAG_BUFFER
    auth_bytes => COMPACT_BYTES
*/
export function createRequest (authBytes: Buffer): Writer {
  return Writer.create().appendBytes(authBytes).appendTaggedFields()
}

/*
  SaslAuthenticate Response (Version: 2) => error_code error_message auth_bytes session_lifetime_ms TAG_BUFFER
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    auth_bytes => COMPACT_BYTES
    session_lifetime_ms => INT64
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): SaslAuthenticateResponse {
  const response: SaslAuthenticateResponse = {
    errorCode: reader.readInt16(),
    errorMessage: reader.readNullableString(),
    authBytes: reader.readBytes(),
    sessionLifetimeMs: reader.readInt64()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, response.errorMessage] }, response)
  }

  return response
}

export const api = createAPI<SaslAuthenticateRequest, SaslAuthenticateResponse>(36, 2, createRequest, parseResponse)
