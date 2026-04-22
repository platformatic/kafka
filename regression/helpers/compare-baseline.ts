import { ok } from 'node:assert'
import type { BenchmarkResult } from './index.ts'

export interface BaselineThresholds {
  throughputFailRatio: number
  latencyFailRatio: number
  memoryFailRatio?: number
}

export interface BaselineComparison {
  throughputRatio: number
  durationRatio: number
  memoryRatio?: number
}

export function compareBenchmarkBaseline (
  current: BenchmarkResult,
  baseline: BenchmarkResult,
  thresholds: BaselineThresholds = { throughputFailRatio: 0.85, latencyFailRatio: 1.2 }
): BaselineComparison {
  const name = current.name || baseline.name
  const throughputRatio = current.messagesPerSecond / baseline.messagesPerSecond
  const currentLatencyMs = current.durationMs / current.messages
  const baselineLatencyMs = baseline.durationMs / baseline.messages
  const durationRatio = currentLatencyMs / baselineLatencyMs
  const currentMaxRss = maxSampleValue(current, 'rss')
  const baselineMaxRss = maxSampleValue(baseline, 'rss')
  const memoryRatio = currentMaxRss && baselineMaxRss ? currentMaxRss / baselineMaxRss : undefined

  ok(
    throughputRatio >= thresholds.throughputFailRatio,
    `${name} throughput regression: current=${current.messagesPerSecond} baseline=${baseline.messagesPerSecond}`
  )
  ok(
    durationRatio <= thresholds.latencyFailRatio,
    `${name} latency regression: current=${currentLatencyMs}ms/message baseline=${baselineLatencyMs}ms/message`
  )

  if (typeof thresholds.memoryFailRatio === 'number' && typeof memoryRatio === 'number') {
    ok(memoryRatio <= thresholds.memoryFailRatio, `${name} memory regression: current=${currentMaxRss} baseline=${baselineMaxRss}`)
  }

  return { throughputRatio, durationRatio, memoryRatio }
}

function maxSampleValue (result: BenchmarkResult, key: 'rss' | 'heapUsed'): number | undefined {
  if (!result.samples?.length) {
    return undefined
  }

  return Math.max(...result.samples.map(sample => sample[key]))
}
