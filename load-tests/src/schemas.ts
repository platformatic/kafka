import { z } from 'zod/v4'

export const DIRECT_EVENT_SCHEMA = z.object({
  id: z.string(),
  event_type: z.string(),
  payload: z.record(z.string(), z.unknown()),
  created_at: z.string(),
})
export type DirectEvent = z.output<typeof DIRECT_EVENT_SCHEMA>

export const DIRECT_ORDER_SCHEMA = z.object({
  id: z.string(),
  customer_id: z.string(),
  amount: z.string(),
  status: z.string(),
  created_at: z.string(),
})
export type DirectOrder = z.output<typeof DIRECT_ORDER_SCHEMA>

export const TOPICS = {
  EVENTS: 'direct-events',
  ORDERS: 'direct-orders',
} as const

// Stress test schemas — same validation as direct but with larger payload fields

export const STRESS_EVENT_SCHEMA = z.object({
  id: z.string(),
  event_type: z.string(),
  payload: z.record(z.string(), z.unknown()),
  created_at: z.string(),
})
export type StressEvent = z.output<typeof STRESS_EVENT_SCHEMA>

export const STRESS_ORDER_SCHEMA = z.object({
  id: z.string(),
  customer_id: z.string(),
  amount: z.string(),
  status: z.string(),
  created_at: z.string(),
})
export type StressOrder = z.output<typeof STRESS_ORDER_SCHEMA>

export const STRESS_METRIC_SCHEMA = z.object({
  id: z.string(),
  metric_name: z.string(),
  value: z.number(),
  tags: z.record(z.string(), z.unknown()),
  created_at: z.string(),
})
export type StressMetric = z.output<typeof STRESS_METRIC_SCHEMA>

export const STRESS_LOG_SCHEMA = z.object({
  id: z.string(),
  level: z.string(),
  message: z.string(),
  context: z.record(z.string(), z.unknown()),
  created_at: z.string(),
})
export type StressLog = z.output<typeof STRESS_LOG_SCHEMA>

export const STRESS_TOPICS = {
  EVENTS: 'stress-events',
  ORDERS: 'stress-orders',
  METRICS: 'stress-metrics',
  LOGS: 'stress-logs',
} as const
