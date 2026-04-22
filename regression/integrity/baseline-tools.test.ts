import { deepStrictEqual, strictEqual } from 'node:assert'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { test } from 'node:test'
import { FileBaselineStore, createBaselineDocument } from '../helpers/baseline-store.ts'
import { runRegressionBenchmark } from '../helpers/benchmark-harness.ts'
import { compareBenchmarkBaseline } from '../helpers/compare-baseline.ts'
import type { BenchmarkResult } from '../helpers/index.ts'
import { aggregateBenchmarkRuns } from '../performance/helpers.ts'

function createBenchmarkResult (name: string, messagesPerSecond: number, durationMs: number): BenchmarkResult {
  return {
    name,
    startedAt: new Date(0).toISOString(),
    durationMs,
    messages: 100,
    messagesPerSecond
  }
}

test('regression baseline store persists and reloads lane documents', async () => {
  const root = await mkdtemp(join(tmpdir(), 'plt-kafka-regression-'))

  try {
    const store = new FileBaselineStore(root)
    const result = createBenchmarkResult('producer-single', 100, 1000)
    const document = createBaselineDocument('local', { 'producer-single': result }, { commit: 'abc123' })

    await store.write('local', document)
    const reloaded = await store.read('local')

    deepStrictEqual(reloaded, document)
    strictEqual(await store.read('missing'), undefined)
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

test('regression benchmark harness emits machine-readable metrics', async () => {
  const result = await runRegressionBenchmark('unit-benchmark', 5, async () => ({ bytes: 25 }))

  strictEqual(result.name, 'unit-benchmark')
  strictEqual(result.messages, 5)
  strictEqual(result.bytes, 25)
  strictEqual(result.messagesPerSecond > 0, true)
  strictEqual(Array.isArray(result.samples), true)
})

test('regression baseline comparison reports throughput, duration, and memory ratios', () => {
  const baseline: BenchmarkResult = {
    ...createBenchmarkResult('consumer-stream', 100, 1000),
    samples: [{ at: 0, heapUsed: 100, rss: 1000, userCpuMicros: 1, systemCpuMicros: 1, eventLoopDelayMean: 0, eventLoopDelayMax: 0 }]
  }
  const current: BenchmarkResult = {
    ...createBenchmarkResult('consumer-stream', 95, 1050),
    samples: [{ at: 0, heapUsed: 120, rss: 1100, userCpuMicros: 1, systemCpuMicros: 1, eventLoopDelayMean: 0, eventLoopDelayMax: 0 }]
  }

  deepStrictEqual(compareBenchmarkBaseline(current, baseline), {
    throughputRatio: 0.95,
    durationRatio: 1.05,
    memoryRatio: 1.1
  })
})

test('performance aggregation uses median samples for stable comparisons', () => {
  const aggregate = aggregateBenchmarkRuns('producer-single', [
    createBenchmarkResult('producer-single', 10, 1000),
    createBenchmarkResult('producer-single', 30, 300),
    createBenchmarkResult('producer-single', 20, 500)
  ])

  strictEqual(aggregate.messagesPerSecond, 20)
  strictEqual(aggregate.durationMs, 500)
  strictEqual(aggregate.runs?.length, 3)
})
