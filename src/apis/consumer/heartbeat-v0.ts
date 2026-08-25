import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
export type HeartbeatRequest = Parameters<typeof createRequest>
export interface HeartbeatResponse {
  throttleTimeMs: number
  errorCode: number
}
/*
  Heartbeat Request (Version: 0) => group_id generation_id member_id
    group_id => STRING
    generation_id => INT32
    member_id => STRING
*/
export function createRequest (
  groupId: string,
  generationId: number,
  memberId: string,
  _groupInstanceId?: NullableString
): Writer {
  return Writer.create().appendString(groupId, false).appendInt32(generationId).appendString(memberId, false)
}
/*
  Heartbeat Response (Version: 0) => error_code
    error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): HeartbeatResponse {
  const response = { throttleTimeMs: 0, errorCode: reader.readInt16() }
  if (response.errorCode !== 0) { throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response) }
  return response
}
export const api = createAPI<HeartbeatRequest, HeartbeatResponse>(12, 0, createRequest, parseResponse, false, false)
