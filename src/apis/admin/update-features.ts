import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface UpdateFeaturesRequestFeature {
  feature: string
  maxVersionLevel: number
  upgradeType: number
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
  UpdateFeatures Request (Version: 1) => timeout_ms [feature_updates] validate_only TAG_BUFFER
    timeout_ms => INT32
    feature_updates => feature max_version_level upgrade_type TAG_BUFFER
      feature => COMPACT_STRING
      max_version_level => INT16
      upgrade_type => INT8
    validate_only => BOOLEAN
*/
export function createRequest (
  timeoutMs: number,
  featureUpdates: UpdateFeaturesRequestFeature[],
  validateOnly: boolean
): Writer {
  return Writer.create()
    .appendInt32(timeoutMs)
    .appendArray(featureUpdates, (w, f) => {
      w.appendString(f.feature).appendInt16(f.maxVersionLevel).appendInt8(f.upgradeType)
    })
    .appendBoolean(validateOnly)
    .appendTaggedFields()
}

/*
  UpdateFeatures Response (Version: 1) => throttle_time_ms error_code error_message [results] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    results => feature error_code error_message TAG_BUFFER
      feature => COMPACT_STRING
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): UpdateFeaturesResponse {
  const errors: ResponseErrorWithLocation[] = []

  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()

  if (errorCode !== 0) {
    errors.push(['', errorCode])
  }

  const response: UpdateFeaturesResponse = {
    throttleTimeMs,
    errorCode,
    errorMessage: reader.readNullableString(),
    results: reader.readArray((r, i) => {
      const result = {
        feature: r.readString(),
        errorCode: r.readInt16(),
        errorMessage: r.readNullableString()
      }

      if (result.errorCode !== 0) {
        errors.push([`/results/${i}`, result.errorCode])
      }

      return result
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<UpdateFeaturesRequest, UpdateFeaturesResponse>(57, 1, createRequest, parseResponse)
