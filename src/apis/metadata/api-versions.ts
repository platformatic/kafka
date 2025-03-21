import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { protocolAPIsById } from '../../protocol/apis.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type ApiVersionsRequest = Parameters<typeof createRequest>

export interface ApiVersionsResponseApi {
  apiKey: number
  name: string
  minVersion: number
  maxVersion: number
}

export type ApiVersionsResponse = {
  errorCode: number
  apiKeys: ApiVersionsResponseApi[]
  throttleTimeMs: number
}

/*
  ApiVersions Request (Version: 4) => client_software_name client_software_version TAG_BUFFER
    client_software_name => COMPACT_STRING
    client_software_version => COMPACT_STRING
*/
function createRequest (clientSoftwareName: string, clientSoftwareVersion: string): Writer {
  return Writer.create().appendString(clientSoftwareName).appendString(clientSoftwareVersion).appendTaggedFields()
}

/*
  ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms TAG_BUFFER
    error_code => INT16
    api_keys => api_key min_version max_version TAG_BUFFER
      api_key => INT16
      min_version => INT16
      max_version => INT16
    throttle_time_ms => INT32
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): ApiVersionsResponse {
  const reader = Reader.from(raw)

  const response: ApiVersionsResponse = {
    errorCode: reader.readInt16(),
    apiKeys: reader.readArray(r => {
      const apiKey = r.readInt16()

      return {
        apiKey,
        name: protocolAPIsById[apiKey],
        minVersion: r.readInt16(),
        maxVersion: r.readInt16()
      }
    })!,
    throttleTimeMs: reader.readInt32()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const apiVersionsV4 = createAPI<ApiVersionsRequest, ApiVersionsResponse>(
  18,
  4,
  createRequest,
  parseResponse,
  true,
  false
)
