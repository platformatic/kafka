import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import { ConfigResourceTypes, type ConfigResourceTypeValue } from '../enumerations.ts'

export type ListClientMetricsResourcesRequest = Parameters<typeof createRequest>

export interface ListClientMetricsResourcesResource {
  resourceName: string
  resourceType: ConfigResourceTypeValue
}

export interface ListClientMetricsResourcesResponse {
  throttleTimeMs: number
  errorCode: number
  configResources: ListClientMetricsResourcesResource[]
}

/*
  ListClientMetricsResources Request (Version: 0) => TAG_BUFFER
*/
export function createRequest (_resourceTypes?: ConfigResourceTypeValue[]): Writer {
  return Writer.create().appendTaggedFields()
}

/*
 ListClientMetricsResources Response (Version: 0) => throttle_time_ms error_code [config_resources] TAG_BUFFER
  throttle_time_ms => INT32
  error_code => INT16
  config_resources => resource_name TAG_BUFFER
    resource_name => COMPACT_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ListClientMetricsResourcesResponse {
  const response: ListClientMetricsResourcesResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    configResources: reader.readArray(r => {
      const resource = {
        resourceName: r.readString(),
        resourceType: ConfigResourceTypes.CLIENT_METRICS
      }
      r.readTaggedFields()
      return resource
    }, true, false)
  }
  reader.readTaggedFields()

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<ListClientMetricsResourcesRequest, ListClientMetricsResourcesResponse>(
  74,
  0,
  createRequest,
  parseResponse
)
