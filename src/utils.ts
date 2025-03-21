import { Ajv } from 'ajv'
import ajvErrors from 'ajv-errors'
import type BufferList from 'bl'
import { setTimeout as sleep } from 'node:timers/promises'

export const ajv = new Ajv({ allErrors: true })
// @ts-ignore
ajvErrors(ajv)

ajv.addKeyword({
  keyword: 'bigint',
  validate (_: unknown, x: unknown) {
    return typeof x === 'bigint'
  },
  error: {
    message: 'must be bigint'
  }
})

ajv.addKeyword({
  keyword: 'function',
  validate (_: unknown, x: unknown) {
    return typeof x === 'function'
  },
  error: {
    message: 'must be function'
  }
})

export function niceJoin (array: string[], lastSeparator: string = ' and ', separator: string = ', '): string {
  switch (array.length) {
    case 0:
      return ''
    case 1:
      return array[0]
    case 2:
      return array.join(lastSeparator)
    default:
      return array.slice(0, -1).join(separator) + lastSeparator + array.at(-1)!
  }
}

export function groupByProperty<K, T> (entries: T[], property: keyof T): [K, T[]][] {
  const grouped: Map<K, T[]> = new Map()
  const result: [K, T[]][] = []

  for (const entry of entries) {
    const value = entry[property] as K
    let values = grouped.get(value)

    if (!values) {
      values = []
      grouped.set(value, values)
      result.push([value, values])
    }

    values.push(entry)
  }

  return result
}

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
