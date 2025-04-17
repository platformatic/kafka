import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export type GetTelemetrySubscriptionsRequest = Parameters<typeof createRequest>

export interface GetTelemetrySubscriptionsResponse {
  throttleTimeMs: number
  errorCode: number
  clientInstanceId: string
  subscriptionId: number
  acceptedCompressionTypes: number[]
  pushIntervalMs: number
  telemetryMaxBytes: number
  deltaTemporality: boolean
  requestedMetrics: string[]
}

/*
  GetTelemetrySubscriptions Request (Version: 0) => client_instance_id TAG_BUFFER
    client_instance_id => UUID
*/
export function createRequest (clientInstanceId?: NullableString): Writer {
  return Writer.create().appendUUID(clientInstanceId).appendTaggedFields()
}

/*
  GetTelemetrySubscriptions Response (Version: 0) => throttle_time_ms error_code client_instance_id subscription_id [accepted_compression_types] push_interval_ms telemetry_max_bytes delta_temporality [requested_metrics] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    client_instance_id => UUID
    subscription_id => INT32
    accepted_compression_types => INT8
    push_interval_ms => INT32
    telemetry_max_bytes => INT32
    delta_temporality => BOOLEAN
    requested_metrics => COMPACT_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): GetTelemetrySubscriptionsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()

  if (errorCode !== 0) {
    errors.push(['', errorCode])
  }

  const response: GetTelemetrySubscriptionsResponse = {
    throttleTimeMs,
    errorCode,
    clientInstanceId: reader.readUUID(),
    subscriptionId: reader.readInt32(),
    acceptedCompressionTypes: reader.readArray(r => r.readInt8(), true, false)!,
    pushIntervalMs: reader.readInt32(),
    telemetryMaxBytes: reader.readInt32(),
    deltaTemporality: reader.readBoolean(),
    requestedMetrics: reader.readArray(r => r.readString(), true, false)!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse>(
  71,
  0,
  createRequest,
  parseResponse
)
