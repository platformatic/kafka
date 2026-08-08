import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import { type ConfigResourceTypeValue, type ConfigSourceValue, type ConfigTypeValue } from '../enumerations.ts'

export interface DescribeConfigsRequestResource {
  resourceType: ConfigResourceTypeValue
  resourceName: string
  configurationKeys?: string[] | null | undefined
}

export type DescribeConfigsRequest = Parameters<typeof createRequest>

export interface DescribeConfigsResponseSynonym {
  name: string
  value: NullableString
  source: ConfigSourceValue
}

export interface DescribeConfigsResponseConfig {
  name: string
  value: NullableString
  readOnly: boolean
  configSource: ConfigSourceValue
  isSensitive: boolean
  synonyms: DescribeConfigsResponseSynonym[]
  configType: ConfigTypeValue
  documentation: NullableString
}

export interface DescribeConfigsResponseResult {
  errorCode: number
  errorMessage: NullableString
  resourceType: ConfigResourceTypeValue
  resourceName: string
  configs: DescribeConfigsResponseConfig[]
}

export interface DescribeConfigsResponse {
  throttleTimeMs: number
  results: DescribeConfigsResponseResult[]
}

/*
  DescribeConfigs Request (Version: 1) => [resources] include_synonyms
    resources => resource_type resource_name [configuration_keys]
      resource_type => INT8
      resource_name => STRING
      configuration_keys => STRING
    include_synonyms => BOOLEAN
*/
export function createRequest (
  resources: DescribeConfigsRequestResource[],
  includeSynonyms: boolean = false,
  _includeDocumentation: boolean = false
): Writer {
  return Writer.create()
    .appendArray(
      resources,
      (w, r) => {
        w.appendInt8(r.resourceType)
          .appendString(r.resourceName, false)
          .appendArray(r.configurationKeys, (w, c) => w.appendString(c, false), false, false)
      },
      false,
      false
    )
    .appendBoolean(includeSynonyms)
}

/*
  DescribeConfigs Response (Version: 1) => throttle_time_ms [results]
    throttle_time_ms => INT32
    results => error_code error_message resource_type resource_name [configs]
      error_code => INT16
      error_message => NULLABLE_STRING
      resource_type => INT8
      resource_name => STRING
      configs => name value read_only config_source is_sensitive [synonyms]
        name => STRING
        value => NULLABLE_STRING
        read_only => BOOLEAN
        config_source => INT8
        is_sensitive => BOOLEAN
        synonyms => name value source
          name => STRING
          value => NULLABLE_STRING
          source => INT8
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
    results: reader.readArray(
      (r, i) => {
        const errorCode = r.readInt16()
        const errorMessage = r.readNullableString(false)

        if (errorCode !== 0) {
          errors.push([`/results/${i}`, [errorCode, errorMessage]])
        }

        return {
          errorCode,
          errorMessage,
          resourceType: r.readInt8() as ConfigResourceTypeValue,
          resourceName: r.readString(false),
          configs: r.readArray(
            r => {
              return {
                name: r.readString(false),
                value: r.readNullableString(false),
                readOnly: r.readBoolean(),
                configSource: r.readInt8() as ConfigSourceValue,
                isSensitive: r.readBoolean(),
                synonyms: r.readArray(
                  r => {
                    return {
                      name: r.readString(false),
                      value: r.readNullableString(false),
                      source: r.readInt8() as ConfigSourceValue
                    }
                  },
                  false,
                  false
                ),
                configType: 0,
                documentation: null
              }
            },
            false,
            false
          )
        }
      },
      false,
      false
    )
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<DescribeConfigsRequest, DescribeConfigsResponse>(
  32,
  1,
  createRequest,
  parseResponse,
  false,
  false
)
