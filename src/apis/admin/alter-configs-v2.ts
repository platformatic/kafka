import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface AlterConfigsRequestConfig {
  name: string
  value?: NullableString
}

export interface AlterConfigsRequestResource {
  resourceType: number
  resourceName: string
  configs: AlterConfigsRequestConfig[]
}

export type AlterConfigsRequest = Parameters<typeof createRequest>

export interface AlterConfigsResponseResult {
  errorCode: number
  errorMessage: NullableString
  resourceType: number
  resourceName: string
}

export interface AlterConfigsResponse {
  throttleTimeMs: number
  responses: AlterConfigsResponseResult[]
}

/*
  AlterConfigs Request (Version: 2) => [resources] validate_only TAG_BUFFER
    resources => resource_type resource_name [configs] TAG_BUFFER
      resource_type => INT8
      resource_name => COMPACT_STRING
      configs => name value TAG_BUFFER
        name => COMPACT_STRING
        value => COMPACT_NULLABLE_STRING
    validate_only => BOOLEAN
*/
export function createRequest (resources: AlterConfigsRequestResource[], validateOnly: boolean): Writer {
  return Writer.create()
    .appendArray(resources, (w, r) => {
      w.appendInt8(r.resourceType)
        .appendString(r.resourceName)
        .appendArray(r.configs, (w, r) => {
          w.appendString(r.name).appendString(r.value)
        })
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
}

/*
  AlterConfigs Response (Version: 2) => throttle_time_ms [responses] TAG_BUFFER
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
  reader: Reader
): AlterConfigsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: AlterConfigsResponse = {
    throttleTimeMs: reader.readInt32(),
    responses: reader.readArray((r, i) => {
      const errorCode = r.readInt16()

      if (errorCode !== 0) {
        errors.push([`/responses/${i}`, errorCode])
      }

      return {
        errorCode,
        errorMessage: r.readNullableString(),
        resourceType: r.readInt8(),
        resourceName: r.readString()
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<AlterConfigsRequest, AlterConfigsResponse>(33, 2, createRequest, parseResponse)
