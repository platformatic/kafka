import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import { type ConfigResourceTypeValue, type ConfigSourceValue, type ConfigTypeValue } from '../enumerations.ts'

type LegacyConfigSourceValue = ConfigSourceValue | -1

export interface DescribeConfigsRequestResource {
  resourceType: ConfigResourceTypeValue
  resourceName: string
  configurationKeys?: string[] | null | undefined
}

export type DescribeConfigsRequest = Parameters<typeof createRequest>

export interface DescribeConfigsResponseConfig {
  name: string
  value: NullableString
  readOnly: boolean
  configSource: LegacyConfigSourceValue
  isSensitive: boolean
  synonyms: DescribeConfigsResponseSynonym[]
  configType: ConfigTypeValue
  documentation: NullableString
}

export interface DescribeConfigsResponseSynonym {
  name: string
  value: NullableString
  source: ConfigSourceValue
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
  DescribeConfigs Request (Version: 0) => [resources]
    resources => resource_type resource_name [configuration_keys]
      resource_type => INT8
      resource_name => STRING
      configuration_keys => STRING
*/
export function createRequest (
  resources: DescribeConfigsRequestResource[],
  _includeSynonyms: boolean = false,
  _includeDocumentation: boolean = false
): Writer {
  return Writer.create().appendArray(
    resources,
    (w, r) => {
      w.appendInt8(r.resourceType)
        .appendString(r.resourceName, false)
        .appendArray(r.configurationKeys, (w, key) => w.appendString(key, false), false, false)
    },
    false,
    false
  )
}

/*
  DescribeConfigs Response (Version: 0) => throttle_time_ms [results]
    throttle_time_ms => INT32
    results => error_code error_message resource_type resource_name [configs]
      error_code => INT16
      error_message => NULLABLE_STRING
      resource_type => INT8
      resource_name => STRING
      configs => name value read_only is_default is_sensitive
        name => STRING
        value => NULLABLE_STRING
        read_only => BOOLEAN
        is_default => BOOLEAN
        is_sensitive => BOOLEAN
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
              const name = r.readString(false)
              const value = r.readNullableString(false)
              const readOnly = r.readBoolean()
              r.readBoolean()
              return {
                name,
                value,
                readOnly,
                configSource: -1,
                isSensitive: r.readBoolean(),
                synonyms: [],
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

export const api = createAPI<DescribeConfigsRequest, DescribeConfigsResponse>(32, 0, createRequest, parseResponse, false, false)
