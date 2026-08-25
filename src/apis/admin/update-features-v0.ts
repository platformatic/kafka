import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'
import { FeatureUpgradeTypes, type FeatureUpgradeTypeValue } from '../enumerations.ts'

export interface UpdateFeaturesRequestFeature {
  feature: string
  maxVersionLevel: number
  upgradeType: FeatureUpgradeTypeValue
}

export type UpdateFeaturesRequest = Parameters<typeof createRequest>

export interface UpdateFeaturesResponseResult {
  feature: string
  errorCode: number
  errorMessage: NullableString
}

export interface UpdateFeaturesResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
  results: UpdateFeaturesResponseResult[]
}

/*
  UpdateFeatures Request (Version: 0) => timeout_ms [feature_updates] TAG_BUFFER
    timeout_ms => INT32
    feature_updates => feature max_version_level allow_downgrade
      feature => COMPACT_STRING
      max_version_level => INT16
      allow_downgrade => BOOLEAN
*/
export function createRequest (timeoutMs: number, featureUpdates: UpdateFeaturesRequestFeature[], _validateOnly: boolean): Writer {
  return Writer.create().appendInt32(timeoutMs).appendArray(featureUpdates, (writer, feature) => {
    writer.appendString(feature.feature).appendInt16(feature.maxVersionLevel)
      .appendBoolean(feature.upgradeType !== FeatureUpgradeTypes.UPGRADE)
  }).appendTaggedFields()
}

/*
  UpdateFeatures Response (Version: 0) => throttle_time_ms error_code error_message [results] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    results => feature error_code error_message TAG_BUFFER
      feature => COMPACT_STRING
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export function parseResponse (_correlationId: number, apiKey: number, apiVersion: number, reader: Reader): UpdateFeaturesResponse {
  const errors: ResponseErrorWithLocation[] = []
  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()
  const errorMessage = reader.readNullableString()

  if (errorCode !== 0) {
    errors.push(['', [errorCode, errorMessage]])
  }

  const response: UpdateFeaturesResponse = {
    throttleTimeMs,
    errorCode,
    errorMessage,
    results: reader.readArray((reader, index) => {
      const result = {
        feature: reader.readString(),
        errorCode: reader.readInt16(),
        errorMessage: reader.readNullableString()
      }

      if (result.errorCode !== 0) {
        errors.push([`/results/${index}`, [result.errorCode, result.errorMessage]])
      }

      return result
    })
  }

  reader.readTaggedFields()

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<UpdateFeaturesRequest, UpdateFeaturesResponse>(57, 0, createRequest, parseResponse)
