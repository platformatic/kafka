import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface DescribeConfigsRequestResource {
  resourceType: number
  resourceName: string
  configurationKeys: string[]
}

export type DescribeConfigsRequest = Parameters<typeof createRequest>

export interface DescribeConfigsResponseSynonym {
  name: string
  value: NullableString
  source: number
}

export interface DescribeConfigsResponseConfig {
  name: string
  value: NullableString
  readOnly: boolean
  configSource: number
  isSensitive: boolean
  synonyms: DescribeConfigsResponseSynonym[]
  configType: number
  documentation: NullableString
}

export interface DescribeConfigsResponseResult {
  errorCode: number
  errorMessage: NullableString
  resourceType: number
  resourceName: string
  configs: DescribeConfigsResponseConfig[]
}

export interface DescribeConfigsResponse {
  throttleTimeMs: number
  results: DescribeConfigsResponseResult[]
}

/*
  DescribeConfigs Request (Version: 4) => [resources] include_synonyms include_documentation TAG_BUFFER
    resources => resource_type resource_name [configuration_keys] TAG_BUFFER
      resource_type => INT8
      resource_name => COMPACT_STRING
      configuration_keys => COMPACT_STRING
    include_synonyms => BOOLEAN
    include_documentation => BOOLEAN
*/
export function createRequest (
  resources: DescribeConfigsRequestResource[],
  includeSynonyms: boolean,
  includeDocumentation: boolean
): Writer {
  return Writer.create()
    .appendArray(resources, (w, r) => {
      w.appendInt8(r.resourceType)
        .appendString(r.resourceName)
        .appendArray(r.configurationKeys, (w, c) => w.appendString(c), true, false)
    })
    .appendBoolean(includeSynonyms)
    .appendBoolean(includeDocumentation)
    .appendTaggedFields()
}

/*
  DescribeConfigs Response (Version: 4) => throttle_time_ms [results] TAG_BUFFER
    throttle_time_ms => INT32
    results => error_code error_message resource_type resource_name [configs] TAG_BUFFER
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      resource_type => INT8
      resource_name => COMPACT_STRING
      configs => name value read_only config_source is_sensitive [synonyms] config_type documentation TAG_BUFFER
        name => COMPACT_STRING
        value => COMPACT_NULLABLE_STRING
        read_only => BOOLEAN
        config_source => INT8
        is_sensitive => BOOLEAN
        synonyms => name value source TAG_BUFFER
          name => COMPACT_STRING
          value => COMPACT_NULLABLE_STRING
          source => INT8
        config_type => INT8
        documentation => COMPACT_NULLABLE_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeConfigsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: DescribeConfigsResponse = {
    throttleTimeMs: reader.readInt32(),
    results: reader.readArray((r, i) => {
      const errorCode = r.readInt16()
      const errorMessage = r.readNullableString()

      if (errorCode !== 0) {
        errors.push([`/results/${i}`, [errorCode, errorMessage]])
      }

      return {
        errorCode,
        errorMessage,
        resourceType: r.readInt8(),
        resourceName: r.readString(),
        configs: r.readArray(r => {
          return {
            name: r.readString(),
            value: r.readNullableString(),
            readOnly: r.readBoolean(),
            configSource: r.readInt8(),
            isSensitive: r.readBoolean(),
            synonyms: r.readArray(
              r => {
                return {
                  name: r.readString(),
                  value: r.readNullableString(),
                  source: r.readInt8()
                }
              },
              true,
              false
            ),
            configType: r.readInt8(),
            documentation: r.readNullableString()
          }
        })
      }
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<DescribeConfigsRequest, DescribeConfigsResponse>(32, 4, createRequest, parseResponse)
