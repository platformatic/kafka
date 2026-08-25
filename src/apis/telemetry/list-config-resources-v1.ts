import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { type ConfigResourceTypeValue } from '../enumerations.ts'
import { createAPI } from '../definitions.ts'

export type ListConfigResourcesRequest = Parameters<typeof createRequest>

export interface ListConfigResourcesResource {
  resourceName: string
  resourceType: ConfigResourceTypeValue
}

export interface ListConfigResourcesResponse {
  throttleTimeMs: number
  errorCode: number
  configResources: ListConfigResourcesResource[]
}

/*
  ListConfigResources Request (Version: 1) => [resource_types] TAG_BUFFER
    resource_types => INT8
*/
export function createRequest (resourceTypes: ConfigResourceTypeValue[] = []): Writer {
  return Writer.create().appendArray(resourceTypes, (w, resourceType) => w.appendInt8(resourceType), true, false).appendTaggedFields()
}

/*
 ListConfigResources Response (Version: 1) => throttle_time_ms error_code [config_resources] TAG_BUFFER
   throttle_time_ms => INT32
   error_code => INT16
   config_resources => resource_name resource_type TAG_BUFFER
     resource_name => COMPACT_STRING
     resource_type => INT8
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ListConfigResourcesResponse {
  const response: ListConfigResourcesResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    configResources: reader.readArray(r => {
      const resource = {
        resourceName: r.readString(),
        resourceType: r.readInt8() as ConfigResourceTypeValue
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

export const api = createAPI<ListConfigResourcesRequest, ListConfigResourcesResponse>(
  74,
  1,
  createRequest,
  parseResponse
)
