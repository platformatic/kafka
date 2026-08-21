import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export interface LeaveGroupRequestMember {
  memberId: string
  groupInstanceId?: NullableString
  reason?: NullableString
}

export type LeaveGroupRequest = Parameters<typeof createRequest>

export interface LeaveGroupResponseMember {
  memberId: string
  groupInstanceId: NullableString
  errorCode: number
}

export interface LeaveGroupResponse {
  throttleTimeMs: number
  errorCode: number
  members: LeaveGroupResponseMember[]
}

/*
  LeaveGroup Request (Version: 1) => group_id member_id
    group_id => STRING
    member_id => STRING
*/
export function createRequest (groupId: string, members: LeaveGroupRequestMember[]): Writer {
  return Writer.create()
    .appendString(groupId, false)
    .appendString(members[0]?.memberId, false)
}

/*
  LeaveGroup Response (Version: 1) => throttle_time_ms error_code
    throttle_time_ms => INT32
    error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): LeaveGroupResponse {
  const response: LeaveGroupResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    members: []
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<LeaveGroupRequest, LeaveGroupResponse>(13, 1, createRequest, parseResponse, false, false)
