import BufferList from 'bl'
import { ERROR_RESPONSE_WITH_ERROR, KafkaError } from '../error.ts'
import { EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE, sizeOfCompactable } from '../protocol/definitions.ts'
import { Reader } from '../protocol/reader.ts'
import { Writer } from '../protocol/writer.ts'
import { KafkaAPIById } from './definitions.ts'
import { createAPI } from './index.ts'

export type ApiVersionsRequest = [clientSoftwareName: string, clientSoftwareVersion: string]

export interface ApiVersionsResponseApi {
  apiKey: number
  name: string
  minVersion: number
  maxVersion: number
}
export type ApiVersionsResponse = {
  apiKeys: ApiVersionsResponseApi[]
  throttleTime: number
}

/*
  ApiVersions Request (Version: 4) => client_software_name client_software_version TAG_BUFFER
    client_software_name => COMPACT_STRING
    client_software_version => COMPACT_STRING

  Buffer allocations: 1
*/
function createRequest (clientSoftwareName: string, clientSoftwareVersion: string): BufferList {
  return Writer.create(
    sizeOfCompactable(clientSoftwareName) +
      sizeOfCompactable(clientSoftwareVersion) +
      EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE
  )
    .writeCompactable(clientSoftwareName)
    .writeCompactable(clientSoftwareVersion)
    .writeTaggedFields()
    .asBufferList()
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
function parseResponse (raw: BufferList): ApiVersionsResponse {
  const reader = Reader.from(raw)

  // Read the error, if any
  const errorCode = reader.readInt16()

  if (errorCode !== 0) {
    throw new KafkaError('Received response with error while executing API APIVersions(v4)', {
      code: ERROR_RESPONSE_WITH_ERROR,
      errorCode
    })
  }

  const apiKeys: ApiVersionsResponseApi[] = []
  const size = reader.readCollectionSize()

  for (let i = 0; i < size; i++) {
    const apiKey = reader.readInt16()
    apiKeys.push({ apiKey, name: KafkaAPIById[apiKey], minVersion: reader.readInt16(), maxVersion: reader.readInt16() })
    reader.readTaggedFields()
  }

  return {
    apiKeys,
    throttleTime: reader.readInt32()
  }
}

export const apiVersionsV4 = createAPI<ApiVersionsRequest, ApiVersionsResponse>(
  18,
  4,
  createRequest,
  parseResponse,
  false
)
