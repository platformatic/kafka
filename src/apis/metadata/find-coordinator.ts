import BufferList from 'bl'
import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../definitions.ts'

export type FindCoordinatorRequest = Parameters<typeof createRequest>

export interface FindCoordinatorResponseCoordinator {
  key: string
  nodeId: number
  host: string
  port: number
  errorCode: number
  errorMessage: NullableString
}

export interface FindCoordinatorResponse {
  throttleTimeMs: number
  coordinators: FindCoordinatorResponseCoordinator[]
}

/*
  FindCoordinator Request (Version: 6) => key_type [coordinator_keys] TAG_BUFFER
    key_type => INT8
    coordinator_keys => COMPACT_STRING
*/
export function createRequest (keyType: number, coordinatorKeys: string[]): Writer {
  return Writer.create()
    .appendInt8(keyType)
    .appendArray(coordinatorKeys, (w, k) => w.appendString(k), true, false)
    .appendTaggedFields()
}

/*
  FindCoordinator Response (Version: 6) => throttle_time_ms [coordinators] TAG_BUFFER
    throttle_time_ms => INT32
    coordinators => key node_id host port error_code error_message TAG_BUFFER
      key => COMPACT_STRING
      node_id => INT32
      host => COMPACT_STRING
      port => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
): FindCoordinatorResponse {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: FindCoordinatorResponse = {
    throttleTimeMs: reader.readInt32(),
    coordinators: reader.readArray((r, i) => {
      const coordinator = {
        key: r.readString()!,
        nodeId: r.readInt32(),
        host: r.readString()!,
        port: r.readInt32(),
        errorCode: r.readInt16(),
        errorMessage: r.readString()
      }

      if (coordinator.errorCode !== 0) {
        errors.push([`/coordinators/${i}`, coordinator.errorCode])
      }

      return coordinator
    })!
  }

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, Object.fromEntries(errors), response)
  }

  return response
}

export const findCoordinatorV6 = createAPI<FindCoordinatorRequest, FindCoordinatorResponse>(
  10,
  6,
  createRequest,
  parseResponse
)
