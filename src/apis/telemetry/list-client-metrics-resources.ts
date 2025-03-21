import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type ListClientMetricsResourcesRequest = Parameters<typeof createRequest>

export interface ListClientMetricsResourcesResource {
  name: string
}

export interface ListClientMetricsResourcesResponse {
  throttleTimeMs: number
  errorCode: number
  clientMetricsResources: ListClientMetricsResourcesResource[]
}

/*
  ListClientMetricsResources Request (Version: 0) => TAG_BUFFER
*/
function createRequest (): Writer {
  return Writer.create().appendTaggedFields()
}

/*
ListClientMetricsResources Response (Version: 0) => throttle_time_ms error_code [client_metrics_resources] TAG_BUFFER
  throttle_time_ms => INT32
  error_code => INT16
  client_metrics_resources => name TAG_BUFFER
    name => COMPACT_STRING
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): ListClientMetricsResourcesResponse {
  const reader = Reader.from(raw)

  const response: ListClientMetricsResourcesResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    clientMetricsResources: reader.readArray(r => {
      return {
        name: r.readString()!
      }
    })!
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const listClientMetricsResourcesV0 = createAPI<
  ListClientMetricsResourcesRequest,
  ListClientMetricsResourcesResponse
>(74, 0, createRequest, parseResponse)
