import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export interface SyncGroupRequestAssignment {
  memberId: string
  assignment: Buffer
}

export type SyncGroupRequest = Parameters<typeof createRequest>

export interface SyncGroupResponse {
  throttleTimeMs: number
  errorCode: number
  protocolType: NullableString
  protocolName: NullableString
  assignment: Buffer
}

/*
  SyncGroup Request (Version: 4) => group_id generation_id member_id group_instance_id [assignments] TAG_BUFFER
    group_id => COMPACT_STRING
    generation_id => INT32
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    assignments => member_id assignment TAG_BUFFER
      member_id => COMPACT_STRING
      assignment => COMPACT_BYTES

*/
export function createRequest (
  groupId: string,
  generationId: number,
  memberId: string,
  groupInstanceId: NullableString,
  assignments: SyncGroupRequestAssignment[]
): Writer {
  return Writer.create()
    .appendString(groupId)
    .appendInt32(generationId)
    .appendString(memberId)
    .appendString(groupInstanceId)
    .appendArray(assignments, (w, a) => w.appendString(a.memberId).appendBytes(a.assignment))
    .appendTaggedFields()
}

/*
  SyncGroup Response (Version: 4) => throttle_time_ms error_code assignment TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    assignment => COMPACT_BYTES
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): SyncGroupResponse {
  const response: SyncGroupResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    protocolType: null,
    protocolName: null,
    assignment: reader.readBytes()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<SyncGroupRequest, SyncGroupResponse>(14, 4, createRequest, parseResponse)
