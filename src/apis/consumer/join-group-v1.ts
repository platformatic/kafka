import { ResponseError } from '../../errors.ts'
import { EMPTY_BUFFER, type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import { type JoinGroupRequest, type JoinGroupRequestProtocol, type JoinGroupResponse } from './join-group-v0.ts'
export type {
  JoinGroupRequest,
  JoinGroupRequestProtocol,
  JoinGroupResponse,
  JoinGroupResponseMember
} from './join-group-v0.ts'
/*
  JoinGroup Request (Version: 1) => group_id session_timeout_ms rebalance_timeout_ms member_id protocol_type [protocols]
    group_id => STRING
    session_timeout_ms => INT32
    rebalance_timeout_ms => INT32
    member_id => STRING
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
  _groupInstanceId: NullableString,
  protocolType: string,
  protocols: JoinGroupRequestProtocol[],
  _reason?: NullableString
): Writer {
  return Writer.create()
    .appendString(groupId, false)
    .appendInt32(sessionTimeoutMs)
    .appendInt32(rebalanceTimeoutMs)
    .appendString(memberId, false)
    .appendString(protocolType, false)
    .appendArray(
      protocols,
      (w, protocol) => w.appendString(protocol.name, false).appendBytes(protocol.metadata ?? EMPTY_BUFFER, false),
      false,
      false
    )
}
/* JoinGroup Response (Version: 1) has the Version 0 response schema. */
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): JoinGroupResponse {
  const response = {
    throttleTimeMs: 0,
    errorCode: reader.readInt16(),
    generationId: reader.readInt32(),
    protocolType: null,
    protocolName: reader.readString(false),
    leader: reader.readString(false),
    skipAssignment: false,
    memberId: reader.readString(false),
    members: reader.readArray(
      r => ({ memberId: r.readString(false), groupInstanceId: null, metadata: r.readBytes(false) }),
      false,
      false
    )
  }
  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }
  return response
}
export const api = createAPI<JoinGroupRequest, JoinGroupResponse>(11, 1, createRequest, parseResponse, false, false)
