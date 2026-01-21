import { inspect } from 'node:util'
import { type JoinGroupResponse, api as joinGroupV9 } from '../src/apis/consumer/join-group-v9.ts'
import { type ProtocolError, ResponseError } from '../src/errors.ts'
import { type Connection } from '../src/network/connection.ts'
import { sleep } from '../src/utils.ts'

export async function invokeAPIWithRetry<ReturnType> (
  operationId: string,
  operation: () => ReturnType,
  attempts: number = 0,
  maxAttempts: number = 3,
  retryDelay: number = 1000,
  onFailedAttempt?: (error: Error, attempts: number) => void,
  beforeFailure?: (error: Error, attempts: number) => void
): Promise<ReturnType> {
  try {
    return await operation()
  } catch (e) {
    const error = e.cause?.errors[0]

    if (error?.canRetry) {
      if (attempts >= maxAttempts) {
        beforeFailure?.(error, attempts)
        console.log(operationId, `Failed after ${attempts} attempts. Throwing the last error.`)
        throw e
      }

      onFailedAttempt?.(error, attempts)
      await sleep(retryDelay)

      return invokeAPIWithRetry(
        operationId,
        operation,
        attempts + 1,
        maxAttempts,
        retryDelay,
        onFailedAttempt,
        beforeFailure
      )
    }

    throw e
  }
}
export async function performAPICallWithRetry<ReturnType> (
  operationId: string,
  operation: () => ReturnType,
  attempts: number = 0,
  maxAttempts: number = 3,
  hidden: boolean = false
): Promise<ReturnType> {
  const response = await invokeAPIWithRetry(
    operationId,
    operation,
    attempts,
    maxAttempts,
    undefined,
    error => {
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
    console.log(operationId, inspect({ operationId, response }, false, 10))
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
