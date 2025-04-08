import { Unpromise } from '@watchable/unpromise'
import { Ajv } from 'ajv'
import ajvErrors from 'ajv-errors'
import type BufferList from 'bl'
import { setTimeout as sleep } from 'node:timers/promises'
import { inspect } from 'node:util'

export type DebugDumpLogger = (...args: any[]) => void

export { setTimeout as sleep } from 'node:timers/promises'

export const ajv = new Ajv({ allErrors: true, coerceTypes: false, strict: true })
// @ts-ignore
ajvErrors(ajv)

let debugDumpLogger: DebugDumpLogger = console.error

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
  keyword: 'map',
  validate (_: unknown, x: unknown) {
    return x instanceof Map
  },
  error: {
    message: 'must be Map'
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

ajv.addKeyword({
  keyword: 'buffer',
  validate (_: unknown, x: unknown) {
    return Buffer.isBuffer(x)
  },
  error: {
    message: 'must be Buffer'
  }
})

export class NumericMap extends Map<string, number> {
  getWithDefault (key: string, fallback: number): number {
    return this.get(key) ?? fallback
  }

  preIncrement (key: string, value: number, fallback: number): number {
    let existing = this.getWithDefault(key, fallback)
    existing += value
    this.set(key, existing)
    return existing
  }

  postIncrement (key: string, value: number, fallback: number): number {
    const existing = this.getWithDefault(key, fallback)
    this.set(key, existing + value)
    return existing
  }
}

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

export function listErrorMessage (type: string[]): string {
  return `should be one of ${niceJoin(type, ' or ')}`
}

export function enumErrorMessage (type: Record<string, unknown>, keysOnly: boolean = false): string {
  if (keysOnly) {
    return `should be one of ${niceJoin(Object.keys(type), ' or ')}`
  }

  return `should be one of ${niceJoin(
    Object.entries(type).map(([k, v]) => `${v} (${k})`),
    ' or '
  )}`
}

export function groupByProperty<Key, Value> (entries: Value[], property: keyof Value): [Key, Value[]][] {
  const grouped: Map<Key, Value[]> = new Map()
  const result: [Key, Value[]][] = []

  for (const entry of entries) {
    const value = entry[property] as Key
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

export function humanize (label: string, buffer: Buffer | BufferList): string {
  const formatted = buffer
    .toString('hex')
    .replaceAll(/(.{4})/g, '$1 ')
    .trim()

  return `${label} (${buffer.length} bytes): ${formatted}`
}

export function setDebugDumpLogger (logger: DebugDumpLogger) {
  debugDumpLogger = logger
}

export function debugDump (...values: unknown[]) {
  debugDumpLogger(...values.map(v => (typeof v === 'string' ? v : inspect(v, false, 10))))
}

export async function executeWithTimeout<T = unknown> (
  promise: Promise<T>,
  timeout: number,
  timeoutValue = 'timeout'
): Promise<T | string> {
  const ac = new AbortController()

  return Unpromise.race([promise, sleep(timeout, timeoutValue, { signal: ac.signal, ref: false })]).then((value: T) => {
    ac.abort()
    return value
  })
}
