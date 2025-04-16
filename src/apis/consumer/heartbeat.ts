import type BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type HeartbeatRequest = Parameters<typeof createRequest>

export interface HeartbeatResponse {
  throttleTimeMs: number
  errorCode: number
}

/*
  Heartbeat Request (Version: 4) => group_id generation_id member_id group_instance_id TAG_BUFFER
    group_id => COMPACT_STRING
    generation_id => INT32
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
*/
export function createRequest (
  groupId: string,
  generationId: number,
  memberId: string,
  groupInstanceId?: NullableString
): Writer {
  return Writer.create()
    .appendString(groupId)
    .appendInt32(generationId)
    .appendString(memberId)
    .appendString(groupInstanceId)
    .appendTaggedFields()
}

/*
  Heartbeat Response (Version: 4) => throttle_time_ms error_code TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): HeartbeatResponse {
  const reader = Reader.from(raw)

  const response: HeartbeatResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const api = createAPI<HeartbeatRequest, HeartbeatResponse>(12, 4, createRequest, parseResponse)
