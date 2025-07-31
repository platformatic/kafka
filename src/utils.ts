import { Ajv2020 } from 'ajv/dist/2020.js'
import debug from 'debug'
import { inspect } from 'node:util'
import { type DynamicBuffer } from './protocol/dynamic-buffer.ts'

export interface EnumerationDefinition<T> {
  allowed: T[]
  errorMessage?: string
}

export type KeywordSchema<T> = { schema: T }

// See: ajv/dist/types/index.d.ts
export interface DataValidationContext {
  parentData: {
    [k: string | number]: any
  }
}

export type DebugDumpLogger = (...args: any[]) => void

export { setTimeout as sleep } from 'node:timers/promises'

export const ajv = new Ajv2020({ allErrors: true, coerceTypes: false, strict: true })

export const loggers: Record<string, debug.Debugger> = {
  protocol: debug('plt:kafka:protocol'),
  client: debug('plt:kafka:client'),
  producer: debug('plt:kafka:producer'),
  consumer: debug('plt:kafka:consumer'),
  'consumer:heartbeat': debug('plt:kafka:consumer:heartbeat'),
  admin: debug('plt:kafka:admin')
}

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

ajv.addKeyword({
  keyword: 'gteProperty',
  validate (property: string, current: number, _: unknown, context?: DataValidationContext) {
    const root = context?.parentData as Record<string, number>
    return current >= root[property]
  },
  error: {
    message ({ schema }: KeywordSchema<string>): string {
      return `must be greater than or equal to $dataVar$/${schema}`
    }
  }
})

ajv.addKeyword({
  keyword: 'lteProperty',
  validate (property: string, current: number, _: unknown, context?: DataValidationContext) {
    const root = context?.parentData as Record<string, number>
    return current < root[property]
  },
  error: {
    message ({ schema }: KeywordSchema<string>): string {
      return `must be less than or equal to $dataVar$/${schema}`
    }
  }
})

ajv.addKeyword({
  keyword: 'enumeration', // This mimics the enum keyword but defines a custom error message
  validate (property: EnumerationDefinition<string | number>, current: string | number) {
    return property.allowed.includes(current)
  },
  error: {
    message ({ schema }: KeywordSchema<EnumerationDefinition<string>>): string {
      return schema.errorMessage!
    }
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

export function groupByProperty<Key extends PropertyKey, Value> (
  entries: readonly Value[],
  property: keyof Value
): [Key, Value[]][] {
  const buckets: Record<Key, Value[]> = Object.create(null)

  for (let i = 0, len = entries.length; i < len; ++i) {
    const e = entries[i]
    const key = e[property] as unknown as Key
    ;(buckets[key] ||= []).push(e)
  }

  return Object.entries(buckets) as unknown as [Key, Value[]][]
}

export function humanize (label: string, buffer: Buffer | DynamicBuffer): string {
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
  debugDumpLogger(new Date().toISOString(), ...values.map(v => (typeof v === 'string' ? v : inspect(v, false, 10))))
}
