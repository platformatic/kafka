import { ResponseError } from '../../errors.ts'
import { protocolAPIsById } from '../../protocol/apis.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import { readKnownTaggedFields } from '../tagged-fields.ts'

export type ApiVersionsRequest = Parameters<typeof createRequest>

export interface ApiVersionsResponseApi {
  apiKey: number
  name: string
  minVersion: number
  maxVersion: number
}

export interface ApiVersionsResponseSupportedFeature {
  name: string
  minVersion: number
  maxVersion: number
}

export interface ApiVersionsResponseFinalizedFeature {
  name: string
  maxVersionLevel: number
  minVersionLevel: number
}

export interface ApiVersionsResponse {
  errorCode: number
  apiKeys: ApiVersionsResponseApi[]
  throttleTimeMs: number
  supportedFeatures?: ApiVersionsResponseSupportedFeature[]
  finalizedFeaturesEpoch?: bigint
  finalizedFeatures?: ApiVersionsResponseFinalizedFeature[]
  zkMigrationReady?: boolean
}

/*
  ApiVersions Request (Version: 3) => client_software_name client_software_version TAG_BUFFER
    client_software_name => COMPACT_STRING
    client_software_version => COMPACT_STRING
*/
export function createRequest (clientSoftwareName: string, clientSoftwareVersion: string): Writer {
  return Writer.create().appendString(clientSoftwareName).appendString(clientSoftwareVersion).appendTaggedFields()
}

/*
  ApiVersions Response (Version: 3) => error_code [api_keys] throttle_time_ms TAG_BUFFER
    error_code => INT16
    api_keys => api_key min_version max_version TAG_BUFFER
      api_key => INT16
      min_version => INT16
      max_version => INT16
    throttle_time_ms => INT32
    TAG_BUFFER => supported_features finalized_features_epoch finalized_features zk_migration_ready
      supported_features (0) => [name min_version max_version TAG_BUFFER]
        name => COMPACT_STRING
        min_version => INT16
        max_version => INT16
      finalized_features_epoch (1) => INT64
      finalized_features (2) => [name max_version_level min_version_level TAG_BUFFER]
        name => COMPACT_STRING
        max_version_level => INT16
        min_version_level => INT16
      zk_migration_ready (3) => BOOLEAN
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ApiVersionsResponse {
  const response: ApiVersionsResponse = {
    errorCode: reader.readInt16(),
    apiKeys: reader.readArray(
      r => {
        const apiKey = r.readInt16()

        const api = {
          apiKey,
          name: protocolAPIsById[apiKey],
          minVersion: r.readInt16(),
          maxVersion: r.readInt16()
        }
        r.readTaggedFields()
        return api
      },
      true,
      false
    ),
    throttleTimeMs: reader.readInt32(),
    supportedFeatures: [],
    finalizedFeaturesEpoch: -1n,
    finalizedFeatures: [],
    zkMigrationReady: false
  }
  readKnownTaggedFields(reader, {
    0: payload => {
      response.supportedFeatures = payload.readArray(
        r => ({ name: r.readString(), minVersion: r.readInt16(), maxVersion: r.readInt16() }),
        true,
        true
      )
    },
    1: payload => {
      response.finalizedFeaturesEpoch = payload.readInt64()
    },
    2: payload => {
      response.finalizedFeatures = payload.readArray(
        r => ({ name: r.readString(), maxVersionLevel: r.readInt16(), minVersionLevel: r.readInt16() }),
        true,
        true
      )
    },
    3: payload => {
      response.zkMigrationReady = payload.readBoolean()
    }
  })

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<ApiVersionsRequest, ApiVersionsResponse>(18, 3, createRequest, parseResponse, true, false)
