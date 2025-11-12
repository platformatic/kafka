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
  DescribeConfigs Request (Version: 3) => [resources] include_synonyms include_documentation TAG_BUFFER
    resources => resource_type resource_name [configuration_keys] TAG_BUFFER
      resource_type => INT8
      resource_name => STRING
      configuration_keys => STRING
    include_synonyms => BOOLEAN
    include_documentation => BOOLEAN
*/
export function createRequest (
  resources: DescribeConfigsRequestResource[],
  includeSynonyms: boolean,
  includeDocumentation: boolean
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
    .appendBoolean(includeDocumentation)
}

/*
  DescribeConfigs Response (Version: 3) => throttle_time_ms [results] TAG_BUFFER
    throttle_time_ms => INT32
    results => error_code error_message resource_type resource_name [configs] TAG_BUFFER
      error_code => INT16
      error_message => NULLABLE_STRING
      resource_type => INT8
      resource_name => STRING
      configs => name value read_only config_source is_sensitive [synonyms] config_type documentation TAG_BUFFER
        name => STRING
        value => NULLABLE_STRING
        read_only => BOOLEAN
        config_source => INT8
        is_sensitive => BOOLEAN
        synonyms => name value source TAG_BUFFER
          name => STRING
          value => NULLABLE_STRING
          source => INT8
        config_type => INT8
        documentation => NULLABLE_STRING
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

        if (errorCode !== 0) {
          errors.push([`/results/${i}`, errorCode])
        }

        return {
          errorCode,
          errorMessage: r.readNullableString(false),
          resourceType: r.readInt8(),
          resourceName: r.readString(false),
          configs: r.readArray(
            r => {
              return {
                name: r.readString(false),
                value: r.readNullableString(false),
                readOnly: r.readBoolean(),
                configSource: r.readInt8(),
                isSensitive: r.readBoolean(),
                synonyms: r.readArray(
                  r => {
                    return {
                      name: r.readString(false),
                      value: r.readNullableString(false),
                      source: r.readInt8()
                    }
                  },
                  false,
                  false
                ),
                configType: r.readInt8(),
                documentation: r.readNullableString(false)
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
  3,
  createRequest,
  parseResponse,
  false,
  false
)
