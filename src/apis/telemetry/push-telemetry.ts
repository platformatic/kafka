import type BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type PushTelemetryRequest = Parameters<typeof createRequest>

export interface PushTelemetryResponse {
  throttleTimeMs: number
  errorCode: number
}

/*
  PushTelemetry Request (Version: 0) => client_instance_id subscription_id terminating compression_type metrics TAG_BUFFER
    client_instance_id => UUID
    subscription_id => INT32
    terminating => BOOLEAN
    compression_type => INT8
    metrics => COMPACT_BYTES
*/
export function createRequest (
  clientInstanceId: string,
  subscriptionId: number,
  terminating: boolean,
  compressionType: number,
  metrics: Buffer
): Writer {
  return Writer.create()
    .appendUUID(clientInstanceId)
    .appendInt32(subscriptionId)
    .appendBoolean(terminating)
    .appendInt8(compressionType)
    .appendBytes(metrics)
    .appendTaggedFields()
}

/*
  PushTelemetry Response (Version: 0) => throttle_time_ms error_code TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): PushTelemetryResponse {
  const reader = Reader.from(raw)

  const response: PushTelemetryResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const api = createAPI<PushTelemetryRequest, PushTelemetryResponse>(72, 0, createRequest, parseResponse)
