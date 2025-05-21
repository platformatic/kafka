import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export type DescribeGroupsRequest = Parameters<typeof createRequest>

export interface DescribeGroupsResponseMember {
  memberId: string
  groupInstanceId: NullableString
  clientId: string
  clientHost: string
  memberMetadata: Buffer
  memberAssignment: Buffer
}

export interface DescribeGroupsResponseGroup {
  errorCode: number
  groupId: string
  groupState: string
  protocolType: string
  protocolData: string
  members: DescribeGroupsResponseMember[]
  authorizedOperations: number
}

export interface DescribeGroupsResponse {
  throttleTimeMs: number
  groups: DescribeGroupsResponseGroup[]
}

/*
  DescribeGroups Request (Version: 5) => [groups] include_authorized_operations TAG_BUFFER
    groups => COMPACT_STRING
    include_authorized_operations => BOOLEAN
*/
export function createRequest (groups: string[], includeAuthorizedOperations: boolean): Writer {
  return Writer.create()
    .appendArray(groups, (w, g) => w.appendString(g), true, false)
    .appendBoolean(includeAuthorizedOperations)
    .appendTaggedFields()
}

/*
DescribeGroups Response (Version: 5) => throttle_time_ms [groups] TAG_BUFFER
  throttle_time_ms => INT32
  groups => error_code group_id group_state protocol_type protocol_data [members] authorized_operations TAG_BUFFER
    error_code => INT16
    group_id => COMPACT_STRING
    group_state => COMPACT_STRING
    protocol_type => COMPACT_STRING
    protocol_data => COMPACT_STRING
    members => member_id group_instance_id client_id client_host member_metadata member_assignment TAG_BUFFER
      member_id => COMPACT_STRING
      group_instance_id => COMPACT_NULLABLE_STRING
      client_id => COMPACT_STRING
      client_host => COMPACT_STRING
      member_metadata => COMPACT_BYTES
      member_assignment => COMPACT_BYTES
    authorized_operations => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeGroupsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: DescribeGroupsResponse = {
    throttleTimeMs: reader.readInt32(),
    groups: reader.readArray((r, i) => {
      const group: DescribeGroupsResponseGroup = {
        errorCode: r.readInt16(),
        groupId: r.readString(),
        groupState: r.readString(),
        protocolType: r.readString(),
        protocolData: r.readString(),
        members: r.readArray(r => {
          return {
            memberId: r.readString(),
            groupInstanceId: r.readNullableString(),
            clientId: r.readString(),
            clientHost: r.readString(),
            memberMetadata: r.readBytes(),
            memberAssignment: r.readBytes()
          }
        }),
        authorizedOperations: r.readInt32()
      }

      if (group.errorCode !== 0) {
        errors.push([`/groups/${i}`, group.errorCode])
      }

      return group
    })
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<DescribeGroupsRequest, DescribeGroupsResponse>(15, 5, createRequest, parseResponse)
