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

/* FindCoordinator Request (Version: 0) => group_id */
export function createRequest (_keyType: number, coordinatorKeys: string[]): Writer {
  return Writer.create().appendString(coordinatorKeys[0] ?? '', false)
}

/* FindCoordinator Response (Version: 0) => error_code coordinator_id host port */
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): FindCoordinatorResponse {
  const coordinator = {
    key: '',
    errorCode: reader.readInt16(),
    errorMessage: null,
    nodeId: reader.readInt32(),
    host: reader.readString(false),
    port: reader.readInt32()
  }
  const response = { throttleTimeMs: 0, coordinators: [coordinator] }

  if (coordinator.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/coordinators/0': [coordinator.errorCode, null] }, response)
  }

  return response
}

export const api = createAPI<FindCoordinatorRequest, FindCoordinatorResponse>(
  10,
  0,
  createRequest,
  parseResponse,
  false,
  false
)
