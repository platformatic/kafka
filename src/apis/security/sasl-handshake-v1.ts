import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type SaslHandshakeRequest = Parameters<typeof createRequest>

export interface SaslHandshakeResponse {
  errorCode?: number
  mechanisms?: string[]
}

/*
  SaslHandshake Request (Version: 1) => mechanism
    mechanism => STRING
*/
export function createRequest (mechanism: string): Writer {
  return Writer.create().appendString(mechanism, false)
}

/*
  SaslHandshake Response (Version: 1) => error_code [mechanisms]
    error_code => INT16
    mechanisms => STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): SaslHandshakeResponse {
  const response: SaslHandshakeResponse = {
    errorCode: reader.readInt16(),
    mechanisms: reader.readArray(
      r => {
        return r.readString(false)!
      },
      false,
      false
    )!
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode! }, response)
  }

  return response
}

export const api = createAPI<SaslHandshakeRequest, SaslHandshakeResponse>(
  17,
  1,
  createRequest,
  parseResponse,
  false,
  false
)
