import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

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
  DeleteGroups Request (Version: 0) => [groups_names]
    groups_names => STRING
*/
export function createRequest (groupsNames: string[]): Writer {
  return Writer.create().appendArray(groupsNames, (writer, groupName) => writer.appendString(groupName, false), false, false)
}

/*
  DeleteGroups Response (Version: 0) => throttle_time_ms [results]
    throttle_time_ms => INT32
    results => group_id error_code
      group_id => STRING
      error_code => INT16
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DeleteGroupsResponse {
  const errors: ResponseErrorWithLocation[] = []

  const response: DeleteGroupsResponse = {
    throttleTimeMs: reader.readInt32(),
    results: reader.readArray((r, i) => {
      const group = {
        groupId: r.readString(false),
        errorCode: r.readInt16()
      }

      if (group.errorCode !== 0) {
        errors.push([`/results/${i}`, [group.errorCode, null]])
      }

      return group
    }, false, false)
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const api = createAPI<DeleteGroupsRequest, DeleteGroupsResponse>(42, 0, createRequest, parseResponse, false, false)
