import { inspect } from 'node:util'
import { type JoinGroupResponse, joinGroupV9 } from '../src/apis/consumer/join-group.ts'
import { type Connection } from '../src/connection.ts'
import { ProtocolError, ResponseError } from '../src/errors.ts'
import { invokeAPIWithRetry } from '../src/utils.ts'

export function inspectResponse (label: string, response: unknown): string {
  return inspect({ label, response }, false, 10)
}

export async function performAPICallWithRetry<T> (
  operationId: string,
  operation: () => T,
  attempts: number = 0,
  maxAttempts: number = 3,
  hidden: boolean = false
): Promise<T> {
  const response = await invokeAPIWithRetry(
    operationId,
    operation,
    attempts,
    maxAttempts,
    undefined,
    (error: Error) => {
      console.log(
        operationId,
        `Retrying in 1000ms after retriable error: [${(error as ProtocolError).apiId}] ${error.message}`
      )
    },
    (_: Error, attempts: number) => {
      console.log(operationId, `Failed after ${attempts} attempts. Throwing the last error.`)
    }
  )

  if (!hidden) {
    console.log(operationId, inspectResponse(operationId, response))
  }

  return response
}

export async function joinGroup (
  connection: Connection,
  groupId: string,
  hidden: boolean = false
): Promise<JoinGroupResponse> {
  try {
    return await performAPICallWithRetry(
      'JoinGroup',
      () =>
        joinGroupV9.async(connection, groupId, 60000, 2000, '', null, 'whatever', [{ name: 'whatever' }], 'whatever'),
      0,
      3,
      hidden
    )
  } catch (e) {
    // Kafka will initially ask us to rejoin usin the provided group id
    if (e.code !== ResponseError.code || e.cause.errors[0].apiId !== 'MEMBER_ID_REQUIRED') {
      throw e
    }

    return performAPICallWithRetry(
      'JoinGroup',
      () =>
        joinGroupV9.async(
          connection,
          groupId,
          60000,
          2000,
          (e.response as JoinGroupResponse).memberId!,
          null,
          'whatever',
          [{ name: 'whatever' }],
          'whatever'
        ),
      0,
      3,
      hidden
    )
  }
}

export { setTimeout as sleep } from 'node:timers/promises'
