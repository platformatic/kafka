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
  FindCoordinator Request (Version: 1) => key key_type
    key => STRING
    key_type => INT8
*/
export function createRequest (keyType: number, coordinatorKeys: string[]): Writer {
  return Writer.create().appendString(coordinatorKeys[0] ?? '', false).appendInt8(keyType)
}

/*
  FindCoordinator Response (Version: 1) => throttle_time_ms error_code error_message node_id host port
    throttle_time_ms => INT32
    error_code => INT16
    error_message => NULLABLE_STRING
    node_id => INT32
    host => STRING
    port => INT32
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
        errorMessage: reader.readNullableString(false),
        nodeId: reader.readInt32(),
        host: reader.readString(false),
        port: reader.readInt32()
      }
    ]
  }

  const coordinator = response.coordinators[0]
  if (coordinator.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/coordinators/0': [coordinator.errorCode, coordinator.errorMessage] }, response)
  }

  return response
}

export const api = createAPI<FindCoordinatorRequest, FindCoordinatorResponse>(10, 1, createRequest, parseResponse, false, false)
