import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import { type ConsumerGroupStateValue } from '../enumerations.ts'

export type ListGroupsRequest = Parameters<typeof createRequest>
export interface ListGroupsResponseGroup {
  groupId: string
  protocolType: string
  groupState: string
  groupType: string
}
export interface ListGroupsResponse {
  throttleTimeMs: number
  errorCode: number
  groups: ListGroupsResponseGroup[]
}

/*
  ListGroups Request (Version: 0) =>
  State and type filters are retained for the Admin public call shape but this wire version has an empty body.
*/
export function createRequest (_statesFilter: ConsumerGroupStateValue[], _typesFilter: string[]): Writer {
  return Writer.create()
}

/*
  ListGroups Response (Version: 0) => error_code [groups]
    error_code => INT16
    groups => group_id protocol_type
      group_id => STRING
      protocol_type => STRING
  throttle_time_ms, group_state, and group_type are normalized for callers.
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ListGroupsResponse {
  const response = {
    throttleTimeMs: 0,
    errorCode: reader.readInt16(),
    groups: reader.readArray(
      r => ({ groupId: r.readString(false), protocolType: r.readString(false), groupState: '', groupType: '' }),
      false,
      false
    )
  }
  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }
  return response
}

export const api = createAPI<ListGroupsRequest, ListGroupsResponse>(16, 0, createRequest, parseResponse, false, false)
