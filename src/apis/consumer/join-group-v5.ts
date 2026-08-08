import { ResponseError } from '../../errors.ts'
import { EMPTY_BUFFER, type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export interface JoinGroupRequestProtocol {
  name: string
  metadata?: Buffer | null
}

export type JoinGroupRequest = Parameters<typeof createRequest>

export interface JoinGroupResponseMember {
  memberId: string
  groupInstanceId: NullableString
  metadata: Buffer
}

export interface JoinGroupResponse {
  throttleTimeMs: number
  errorCode: number
  generationId: number
  protocolType: NullableString
  protocolName: NullableString
  leader: string
  skipAssignment: boolean
  memberId: string
  members: JoinGroupResponseMember[]
}

/*
  JoinGroup Request (Version: 5) => group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols]
    group_id => STRING
    session_timeout_ms => INT32
    rebalance_timeout_ms => INT32
    member_id => STRING
    group_instance_id => NULLABLE_STRING
    protocol_type => STRING
    protocols => name metadata
      name => STRING
      metadata => BYTES
*/
export function createRequest (
  groupId: string,
  sessionTimeoutMs: number,
  rebalanceTimeoutMs: number,
  memberId: string,
  groupInstanceId: NullableString,
  protocolType: string,
  protocols: JoinGroupRequestProtocol[],
  _reason?: NullableString
): Writer {
  return Writer.create()
    .appendString(groupId, false)
    .appendInt32(sessionTimeoutMs)
    .appendInt32(rebalanceTimeoutMs)
    .appendString(memberId, false)
    .appendString(groupInstanceId, false)
    .appendString(protocolType, false)
    .appendArray(protocols, (w, protocol) => {
      w.appendString(protocol.name, false).appendBytes(protocol.metadata ?? EMPTY_BUFFER, false)
    }, false, false)
}

/*
  JoinGroup Response (Version: 5) => throttle_time_ms error_code generation_id protocol_name leader member_id [members]
    throttle_time_ms => INT32
    error_code => INT16
    generation_id => INT32
    protocol_name => STRING
    leader => STRING
    member_id => STRING
    members => member_id group_instance_id metadata
      member_id => STRING
      group_instance_id => NULLABLE_STRING
      metadata => BYTES
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): JoinGroupResponse {
  const response: JoinGroupResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    generationId: reader.readInt32(),
    protocolType: null,
    protocolName: reader.readString(false),
    leader: reader.readString(false),
    skipAssignment: false,
    memberId: reader.readString(false),
    members: reader.readArray(r => ({
      memberId: r.readString(false),
      groupInstanceId: r.readNullableString(false),
      metadata: r.readBytes(false)
    }), false, false)
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<JoinGroupRequest, JoinGroupResponse>(11, 5, createRequest, parseResponse, false, false)
