type SafeParseSuccess<T> = {
  success: true
  data: T
}

type SafeParseFailure = {
  success: false
  error: {
    message: string
  }
}

type SafeParseResult<T> = SafeParseSuccess<T> | SafeParseFailure

export interface Schema<T = unknown> {
  safeParse: (value: unknown) => SafeParseResult<T>
}

export interface DirectEvent {
  id: string
  event_type: string
  payload: Record<string, unknown>
  created_at: string
}

export interface DirectOrder {
  id: string
  customer_id: string
  amount: string
  status: string
  created_at: string
}

function isRecord (value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isString (value: unknown): value is string {
  return typeof value === 'string'
}

function isNumber (value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

function createSchema<T> (isValid: (value: unknown) => value is T, schemaName: string): Schema<T> {
  return {
    safeParse (value) {
      if (isValid(value)) {
        return {
          success: true,
          data: value
        }
      }

      return {
        success: false,
        error: {
          message: `Invalid ${schemaName}`
        }
      }
    }
  }
}

function isDirectEvent (value: unknown): value is DirectEvent {
  if (!isRecord(value)) return false
  return isString(value.id) && isString(value.event_type) && isRecord(value.payload) && isString(value.created_at)
}

function isDirectOrder (value: unknown): value is DirectOrder {
  if (!isRecord(value)) return false
  return (
    isString(value.id) &&
    isString(value.customer_id) &&
    isString(value.amount) &&
    isString(value.status) &&
    isString(value.created_at)
  )
}

export const DIRECT_EVENT_SCHEMA = createSchema(isDirectEvent, 'DirectEvent')

export const DIRECT_ORDER_SCHEMA = createSchema(isDirectOrder, 'DirectOrder')

export const TOPICS = {
  EVENTS: 'direct-events',
  ORDERS: 'direct-orders'
} as const

// Stress test schemas — same validation as direct but with larger payload fields

export interface StressEvent {
  id: string
  event_type: string
  payload: Record<string, unknown>
  created_at: string
}

export interface StressOrder {
  id: string
  customer_id: string
  amount: string
  status: string
  created_at: string
}

export interface StressMetric {
  id: string
  metric_name: string
  value: number
  tags: Record<string, unknown>
  created_at: string
}

export interface StressLog {
  id: string
  level: string
  message: string
  context: Record<string, unknown>
  created_at: string
}

function isStressEvent (value: unknown): value is StressEvent {
  if (!isRecord(value)) return false
  return isString(value.id) && isString(value.event_type) && isRecord(value.payload) && isString(value.created_at)
}

function isStressOrder (value: unknown): value is StressOrder {
  if (!isRecord(value)) return false
  return (
    isString(value.id) &&
    isString(value.customer_id) &&
    isString(value.amount) &&
    isString(value.status) &&
    isString(value.created_at)
  )
}

function isStressMetric (value: unknown): value is StressMetric {
  if (!isRecord(value)) return false
  return (
    isString(value.id) &&
    isString(value.metric_name) &&
    isNumber(value.value) &&
    isRecord(value.tags) &&
    isString(value.created_at)
  )
}

function isStressLog (value: unknown): value is StressLog {
  if (!isRecord(value)) return false
  return (
    isString(value.id) &&
    isString(value.level) &&
    isString(value.message) &&
    isRecord(value.context) &&
    isString(value.created_at)
  )
}

export const STRESS_EVENT_SCHEMA = createSchema(isStressEvent, 'StressEvent')

export const STRESS_ORDER_SCHEMA = createSchema(isStressOrder, 'StressOrder')

export const STRESS_METRIC_SCHEMA = createSchema(isStressMetric, 'StressMetric')

export const STRESS_LOG_SCHEMA = createSchema(isStressLog, 'StressLog')

export const STRESS_TOPICS = {
  EVENTS: 'stress-events',
  ORDERS: 'stress-orders',
  METRICS: 'stress-metrics',
  LOGS: 'stress-logs'
} as const
