import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface IncrementalAlterConfigsRequestConfig {
  name: string
  configOperation: number
  value?: NullableString
}

export interface IncrementalAlterConfigsRequestResource {
  resourceType: number
  resourceName: string
  configs: IncrementalAlterConfigsRequestConfig[]
}

export type IncrementalAlterConfigsRequest = Parameters<typeof createRequest>

export interface IncrementalAlterConfigsResponseResult {
  errorCode: number
  errorMessage: NullableString
  resourceType: number
  resourceName: string
}

export interface IncrementalAlterConfigsResponse {
  throttleTimeMs: number
  responses: IncrementalAlterConfigsResponseResult[]
}

/*
  IncrementalAlterConfigs Request (Version: 1) => [resources] validate_only TAG_BUFFER
    resources => resource_type resource_name [configs] TAG_BUFFER
      resource_type => INT8
      resource_name => COMPACT_STRING
      configs => name config_operation value TAG_BUFFER
        name => COMPACT_STRING
        config_operation => INT8
        value => COMPACT_NULLABLE_STRING
    validate_only => BOOLEAN
*/
export function createRequest (resources: IncrementalAlterConfigsRequestResource[], validateOnly: boolean): Writer {
  return Writer.create()
    .appendArray(resources, (w, r) => {
      w.appendInt8(r.resourceType)
        .appendString(r.resourceName)
        .appendArray(r.configs, (w, r) => {
          w.appendString(r.name).appendInt8(r.configOperation).appendString(r.value)
        })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
}

/*
  IncrementalAlterConfigs Response (Version: 1) => throttle_time_ms [responses] TAG_BUFFER
    throttle_time_ms => INT32
    responses => error_code error_message resource_type resource_name TAG_BUFFER
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      resource_type => INT8
      resource_name => COMPACT_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): IncrementalAlterConfigsResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: IncrementalAlterConfigsResponse = {
    throttleTimeMs: reader.readInt32(),
    responses: reader.readArray((r, i) => {
      const errorCode = r.readInt16()

      if (errorCode !== 0) {
        errors.push([`/responses/${i}`, errorCode])
      }

      return {
        errorCode,
        errorMessage: r.readString(),
        resourceType: r.readInt8(),
        resourceName: r.readString()!
      }
    })!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const incrementalAlterConfigsV1 = createAPI<IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse>(
  44,
  1,
  createRequest,
  parseResponse
)
