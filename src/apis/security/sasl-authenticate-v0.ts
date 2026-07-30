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
  SaslAuthenticate Request (Version: 0) => auth_bytes
    auth_bytes => BYTES
*/
export function createRequest (authBytes: Buffer): Writer {
  return Writer.create().appendBytes(authBytes, false)
}

/*
  SaslAuthenticate Response (Version: 0) => error_code error_message auth_bytes
    error_code => INT16
    error_message => NULLABLE_STRING
    auth_bytes => BYTES
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): SaslAuthenticateResponse {
  const response: SaslAuthenticateResponse = {
    errorCode: reader.readInt16(),
    errorMessage: reader.readNullableString(false),
    authBytes: reader.readBytes(false),
    // Version 0 predates reauthentication, so expose the existing response shape with no scheduled reauthentication.
    sessionLifetimeMs: 0n
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, response.errorMessage] }, response)
  }

  return response
}

export const api = createAPI<SaslAuthenticateRequest, SaslAuthenticateResponse>(36, 0, createRequest, parseResponse, false, false)
