import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../index.ts'

export type DeleteGroupsRequest = Parameters<typeof createRequest>

export interface DeleteGroupsResponseGroup {
  groupId: string
  errorCode: number
}

export interface DeleteGroupsResponse {
  throttleTimeMs: number
  results: DeleteGroupsResponseGroup[]
}

/*
  DeleteGroups Request (Version: 2) => [groups_names] TAG_BUFFER
    groups_names => COMPACT_STRING
*/
function createRequest (groupsNames: string[]): Writer {
  return Writer.create()
    .appendArray(groupsNames, (w, r) => w.appendString(r), true, false)
    .appendTaggedFields()
}

/*
  DeleteGroups Response (Version: 2) => throttle_time_ms [results] TAG_BUFFER
    throttle_time_ms => INT32
    results => group_id error_code TAG_BUFFER
      group_id => COMPACT_STRING
      error_code => INT16
*/
function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): DeleteGroupsResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: DeleteGroupsResponse = {
    throttleTimeMs: reader.readInt32(),
    results: reader.readArray((r, i) => {
      const group = {
        groupId: r.readString()!,
        errorCode: r.readInt16()
      }

      if (group.errorCode !== 0) {
        errors.push([`/results/${i}`, group.errorCode])
      }

      return group
    })!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, { errors: Object.fromEntries(errors), response })
  }

  return response
}

export const deleteGroupsV2 = createAPI<DeleteGroupsRequest, DeleteGroupsResponse>(42, 2, createRequest, parseResponse)
