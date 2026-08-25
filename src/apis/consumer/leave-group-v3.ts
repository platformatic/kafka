import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

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
  LeaveGroup Request (Version: 3) => group_id [members]
    group_id => STRING
    members => member_id group_instance_id
      member_id => STRING
      group_instance_id => NULLABLE_STRING
*/
export function createRequest (groupId: string, members: LeaveGroupRequestMember[]): Writer {
  return Writer.create()
    .appendString(groupId, false)
    .appendArray(members, (writer, member) => {
      writer.appendString(member.memberId, false).appendString(member.groupInstanceId, false)
    }, false, false)
}

/*
  LeaveGroup Response (Version: 3) => throttle_time_ms error_code [members]
    throttle_time_ms => INT32
    error_code => INT16
    members => member_id group_instance_id error_code
      member_id => STRING
      group_instance_id => NULLABLE_STRING
      error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): LeaveGroupResponse {
  const errors: ResponseErrorWithLocation[] = []
  const throttleTimeMs = reader.readInt32()
  const errorCode = reader.readInt16()

  if (errorCode !== 0) {
    errors.push(['', [errorCode, null]])
  }

  const response: LeaveGroupResponse = {
    throttleTimeMs,
    errorCode,
    members: reader.readArray((memberReader, index) => {
      const member: LeaveGroupResponseMember = {
        memberId: memberReader.readString(false),
        groupInstanceId: memberReader.readNullableString(false),
        errorCode: memberReader.readInt16()
      }

      if (member.errorCode !== 0) {
        errors.push([`/members/${index}`, [member.errorCode, null]])
      }

      return member
    }, false, false)
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<LeaveGroupRequest, LeaveGroupResponse>(13, 3, createRequest, parseResponse, false, false)
