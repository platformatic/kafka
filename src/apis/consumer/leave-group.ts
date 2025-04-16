import type BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export interface LeaveGroupRequestMember {
  memberId: string
  groupInstanceId?: NullableString
  reason?: NullableString
}

export type LeaveGroupRequest = Parameters<typeof createRequest>

export interface LeaveGroupResponseMember {
  memberId: NullableString
  groupInstanceId: NullableString
  errorCode: number
}

export interface LeaveGroupResponse {
  throttleTimeMs: number
  errorCode: number
  members: LeaveGroupResponseMember[]
}

/*
  LeaveGroup Request (Version: 5) => group_id [members] TAG_BUFFER
    group_id => COMPACT_STRING
    members => member_id group_instance_id reason TAG_BUFFER
      member_id => COMPACT_STRING
      group_instance_id => COMPACT_NULLABLE_STRING
      reason => COMPACT_NULLABLE_STRING
*/
export function createRequest (groupId: string, members: LeaveGroupRequestMember[]): Writer {
  return Writer.create()
    .appendString(groupId)
    .appendArray(members, (w, m) => {
      w.appendString(m.memberId).appendString(m.groupInstanceId).appendString(m.reason)
    })
    .appendTaggedFields()
}

/*
  LeaveGroup Response (Version: 5) => throttle_time_ms error_code [members] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    members => member_id group_instance_id error_code TAG_BUFFER
      member_id => COMPACT_STRING
      group_instance_id => COMPACT_NULLABLE_STRING
      error_code => INT16

*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): LeaveGroupResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()

  if (errorCode !== 0) {
    errors.push(['', errorCode])
  }

  const response: LeaveGroupResponse = {
    throttleTimeMs,
    errorCode,
    members: reader.readArray((r, i) => {
      const member: LeaveGroupResponseMember = {
        memberId: r.readNullableString(),
        groupInstanceId: r.readNullableString(),
        errorCode: r.readInt16()
      }

      if (member.errorCode !== 0) {
        errors.push([`/members/${i}`, member.errorCode])
      }

      return member
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<LeaveGroupRequest, LeaveGroupResponse>(13, 5, createRequest, parseResponse)
