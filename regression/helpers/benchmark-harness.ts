import { performance } from 'node:perf_hooks'
import { createResourceSampler, type BenchmarkResult } from './index.ts'

export async function runRegressionBenchmark (
  name: string,
  messages: number,
  fn: () => Promise<{ bytes?: number } | void>
): Promise<BenchmarkResult> {
  const sampler = createResourceSampler()
  const startedAt = new Date()
  const start = performance.now()
  const result = await fn()
  const durationMs = performance.now() - start
  const samples = sampler.stop()
  const bytes = result?.bytes

  return {
    name,
    startedAt: startedAt.toISOString(),
    durationMs,
    messages,
    messagesPerSecond: messages / (durationMs / 1000),
    bytes,
    bytesPerSecond: typeof bytes === 'number' ? bytes / (durationMs / 1000) : undefined,
    samples
  }
}
