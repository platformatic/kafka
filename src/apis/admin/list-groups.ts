import type BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'
import { type ConsumerGroupState } from '../enumerations.ts'

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
  ListGroups Request (Version: 5) => [states_filter] [types_filter] TAG_BUFFER
    states_filter => COMPACT_STRING
    types_filter => COMPACT_STRING
*/
export function createRequest (statesFilter: ConsumerGroupState[], typesFilter: string[]): Writer {
  return Writer.create()
    .appendArray(statesFilter, (w, s) => w.appendString(s as string), true, false)
    .appendArray(typesFilter, (w, t) => w.appendString(t), true, false)
    .appendTaggedFields()
}

/*
  ListGroups Response (Version: 5) => throttle_time_ms error_code [groups] TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    groups => group_id protocol_type group_state group_type TAG_BUFFER
      group_id => COMPACT_STRING
      protocol_type => COMPACT_STRING
      group_state => COMPACT_STRING
      group_type => COMPACT_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): ListGroupsResponse {
  const reader = Reader.from(raw)

  const response: ListGroupsResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    groups: reader.readArray(r => {
      return {
        groupId: r.readNullableString(),
        protocolType: r.readString(),
        groupState: r.readString(),
        groupType: r.readString()
      } as ListGroupsResponseGroup
    })
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '': response.errorCode }, response)
  }

  return response
}

export const api = createAPI<ListGroupsRequest, ListGroupsResponse>(16, 5, createRequest, parseResponse)
