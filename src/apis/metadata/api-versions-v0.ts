import { ResponseError } from '../../errors.ts'
import { protocolAPIsById } from '../../protocol/apis.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type ApiVersionsRequest = Parameters<typeof createRequest>

export interface ApiVersionsResponseApi {
  apiKey: number
  name: string
  minVersion: number
  maxVersion: number
}

export interface ApiVersionsResponse {
  errorCode: number
  apiKeys: ApiVersionsResponseApi[]
  throttleTimeMs: number
}

/* ApiVersions Request (Version: 0) => */
export function createRequest (_clientSoftwareName: string, _clientSoftwareVersion: string): Writer {
  return Writer.create()
}

/* ApiVersions Response (Version: 0) => error_code [api_keys] */
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ApiVersionsResponse {
  const response: ApiVersionsResponse = {
    errorCode: reader.readInt16(),
    apiKeys: reader.readArray(
      r => {
        const apiKey = r.readInt16()
        return { apiKey, name: protocolAPIsById[apiKey], minVersion: r.readInt16(), maxVersion: r.readInt16() }
      },
      false,
      false
    ),
    throttleTimeMs: 0
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<ApiVersionsRequest, ApiVersionsResponse>(18, 0, createRequest, parseResponse, false, false)
