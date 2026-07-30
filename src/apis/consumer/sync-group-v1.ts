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
  SyncGroup Request (Version: 1) => group_id generation_id member_id [assignments]
    group_id => STRING
    generation_id => INT32
    member_id => STRING
    assignments => member_id assignment
      member_id => STRING
      assignment => BYTES
*/
export function createRequest (
  groupId: string,
  generationId: number,
  memberId: string,
  _groupInstanceId: NullableString,
  _protocolType: NullableString,
  _protocolName: NullableString,
  assignments: SyncGroupRequestAssignment[]
): Writer {
  return Writer.create()
    .appendString(groupId, false)
    .appendInt32(generationId)
    .appendString(memberId, false)
    .appendArray(assignments, (w, a) => w.appendString(a.memberId, false).appendBytes(a.assignment, false), false, false)
}

/*
  SyncGroup Response (Version: 1) => throttle_time_ms error_code assignment
    throttle_time_ms => INT32
    error_code => INT16
    assignment => BYTES
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
    assignment: reader.readBytes(false)
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<SyncGroupRequest, SyncGroupResponse>(14, 1, createRequest, parseResponse, false, false)
