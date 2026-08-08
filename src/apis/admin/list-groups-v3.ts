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
  ListGroups Request (Version: 3) => TAG_BUFFER
  State and type filters are retained in the public creator signature for callers,
  but this version has no corresponding fields in its request body.
*/
export function createRequest (_statesFilter: ConsumerGroupStateValue[], _typesFilter: string[]): Writer {
  return Writer.create().appendTaggedFields()
}

/*
  ListGroups Response (Version: 3) => throttle_time_ms error_code [groups] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    groups => group_id protocol_type TAG_BUFFER
      group_id => COMPACT_STRING
      protocol_type => COMPACT_STRING
  This version has no group_state or group_type fields. They are normalized to empty strings.
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): ListGroupsResponse {
  const response: ListGroupsResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    groups: reader.readArray(r => ({
      groupId: r.readString(),
      protocolType: r.readString(),
      groupState: '',
      groupType: ''
    }))
  }
  reader.readTaggedFields()

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<ListGroupsRequest, ListGroupsResponse>(16, 3, createRequest, parseResponse, true, true)
