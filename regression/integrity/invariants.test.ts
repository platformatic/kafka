import { deepStrictEqual, strictEqual, throws } from 'node:assert'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { compareBenchmarkBaseline } from '../helpers/compare-baseline.ts'
import {
  assertContiguousIds,
  assertNoDuplicateIds,
  assertResourceStability,
  createResourceSampler,
  type BenchmarkResult
} from '../helpers/index.ts'

function messageWithId (id: number) {
  return { value: { id } }
}

test('regression invariants detect duplicate and missing message ids', () => {
  // Validate the helper behavior itself so future regression tests can rely on
  // these shared assertions for message loss and duplicate detection.
  assertNoDuplicateIds([messageWithId(0), messageWithId(1), messageWithId(2)] as never)
  assertContiguousIds([messageWithId(2), messageWithId(0), messageWithId(1)] as never, 3)

  throws(() => assertNoDuplicateIds([messageWithId(0), messageWithId(0)] as never), /duplicate message id 0/)
  throws(() => assertContiguousIds([messageWithId(0), messageWithId(2)] as never, 2))
})

test('baseline comparison enforces severe regression thresholds', () => {
  // Baseline comparison is intentionally threshold based: severe regressions fail,
  // while smaller noise can be handled by workflow summaries or warnings.
  const baseline: BenchmarkResult = {
    name: 'baseline',
    startedAt: new Date(0).toISOString(),
    durationMs: 1000,
    messages: 100,
    messagesPerSecond: 100
  }

  const current: BenchmarkResult = { ...baseline, messagesPerSecond: 90, durationMs: 1100 }
  deepStrictEqual(compareBenchmarkBaseline(current, baseline), {
    throughputRatio: 0.9,
    durationRatio: 1.1,
    memoryRatio: undefined
  })

  deepStrictEqual(compareBenchmarkBaseline({ ...baseline, durationMs: 1100, messages: 500 }, baseline), {
    throughputRatio: 1,
    durationRatio: 0.22000000000000003,
    memoryRatio: undefined
  })

  throws(() => compareBenchmarkBaseline({ ...baseline, messagesPerSecond: 80 }, baseline), /throughput regression/)
  throws(() => compareBenchmarkBaseline({ ...baseline, durationMs: 1300 }, baseline), /latency regression/)
})

test('resource sampler records process and event loop metrics', async () => {
  // Keep the sampler test short; it only proves the artifact data model can be
  // populated during longer benchmark or soak runs.
  const sampler = createResourceSampler(10)
  await sleep(35)
  const samples = sampler.stop()

  strictEqual(samples.length > 0, true)
  strictEqual(samples.every(sample => sample.heapUsed > 0), true)
  strictEqual(samples.every(sample => sample.rss > 0), true)
  assertResourceStability(samples, { minSamples: 1 })
})
