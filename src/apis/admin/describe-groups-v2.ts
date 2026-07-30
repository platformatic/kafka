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
  DescribeGroups Request (Version: 2) => [groups]
    groups => STRING
  includeAuthorizedOperations remains a public argument for a consistent caller API,
  but this protocol version has no corresponding wire field.
*/
export function createRequest (groups: string[], _includeAuthorizedOperations?: boolean): Writer {
  return Writer.create().appendArray(groups, (w, group) => w.appendString(group, false), false, false)
}

/*
  DescribeGroups Response (Version: 2) => throttle_time_ms [groups]
    throttle_time_ms => INT32
    groups => error_code group_id group_state protocol_type protocol_data [members]
      error_code => INT16
      group_id => STRING
      group_state => STRING
      protocol_type => STRING
      protocol_data => STRING
      members => member_id client_id client_host member_metadata member_assignment
        member_id => STRING
        client_id => STRING
        client_host => STRING
        member_metadata => BYTES
        member_assignment => BYTES
  Static membership and authorized operations are unavailable and normalized for callers.
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
    groups: reader.readArray(
      (r, index) => {
        const group: DescribeGroupsResponseGroup = {
          errorCode: r.readInt16(),
          groupId: r.readString(false),
          groupState: r.readString(false),
          protocolType: r.readString(false),
          protocolData: r.readString(false),
          members: r.readArray(
            r => ({
              memberId: r.readString(false),
              groupInstanceId: null,
              clientId: r.readString(false),
              clientHost: r.readString(false),
              memberMetadata: r.readBytes(false),
              memberAssignment: r.readBytes(false)
            }),
            false,
            false
          ),
          authorizedOperations: -2147483648
        }
        if (group.errorCode !== 0) {
          errors.push([`/groups/${index}`, [group.errorCode, null]])
        }
        return group
      },
      false,
      false
    )
  }
  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }
  return response
}

export const api = createAPI<DescribeGroupsRequest, DescribeGroupsResponse>(
  15,
  2,
  createRequest,
  parseResponse,
  false,
  false
)
