import { ResponseError } from '../../errors.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type FindCoordinatorRequest = Parameters<typeof createRequest>

export interface FindCoordinatorResponseCoordinator {
  key: string
  nodeId: number
  host: string
  port: number
  errorCode: number
  errorMessage: string | null
}

export interface FindCoordinatorResponse {
  throttleTimeMs: number
  coordinators: FindCoordinatorResponseCoordinator[]
}

/*
  FindCoordinator Request (Version: 3) => key key_type TAG_BUFFER
    key => COMPACT_STRING
    key_type => INT8
    TAG_BUFFER => TAGGED_FIELDS
*/
export function createRequest (keyType: number, coordinatorKeys: string[]): Writer {
  return Writer.create().appendString(coordinatorKeys[0] ?? '').appendInt8(keyType).appendTaggedFields()
}

/*
  FindCoordinator Response (Version: 3) => throttle_time_ms error_code error_message node_id host port TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    TAG_BUFFER => TAGGED_FIELDS
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): FindCoordinatorResponse {
  const response: FindCoordinatorResponse = {
    throttleTimeMs: reader.readInt32(),
    coordinators: [
      {
        key: '',
        errorCode: reader.readInt16(),
        errorMessage: reader.readNullableString(),
        nodeId: reader.readInt32(),
        host: reader.readString(),
        port: reader.readInt32()
      }
    ]
  }
  reader.readTaggedFields()

  const coordinator = response.coordinators[0]
  if (coordinator.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/coordinators/0': [coordinator.errorCode, coordinator.errorMessage] }, response)
  }

  return response
}

export const api = createAPI<FindCoordinatorRequest, FindCoordinatorResponse>(10, 3, createRequest, parseResponse, true, true)
