import BufferList from 'bl'
import { setTimeout as sleep } from 'node:timers/promises'

export function inspectBuffer (label: string, buffer: Buffer | BufferList): string {
  const formatted = buffer
    .toString('hex')
    .replaceAll(/(.{4})/g, '$1 ')
    .trim()

  return `${label} (${buffer.length} bytes): ${formatted}`
}

export async function invokeAPIWithRetry<T> (
  operationId: string,
  operation: () => T,
  attempts: number = 0,
  maxAttempts: number = 3,
  retryDelay: number = 1000,
  onFailedAttempt?: (error: Error, attempts: number) => void,
  beforeFailure?: (error: Error, attempts: number) => void
): Promise<T> {
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
