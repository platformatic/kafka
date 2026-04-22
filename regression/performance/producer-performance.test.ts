import { ok, strictEqual } from 'node:assert'
import { test } from 'node:test'
import {
  benchmarkBatchProduce,
  benchmarkSingleProduce,
  compareWithStoredBaseline,
  performanceIterations,
  performanceLane,
  performanceSamples,
  runSampledPerformanceBenchmark
} from './helpers.ts'
import { writeRegressionArtifact } from '../helpers/index.ts'

test('performance regression: single-message producer throughput stays within baseline envelope', { timeout: 120_000 }, async t => {
  const result = await runSampledPerformanceBenchmark('producer-single', () => benchmarkSingleProduce(t))
  const comparison = await compareWithStoredBaseline(result)

  await writeRegressionArtifact(`producer-single-performance-${performanceLane}`, { result, comparison })

  strictEqual(result.messages, performanceIterations())
  strictEqual(result.runs?.length, performanceSamples())
  ok(result.messagesPerSecond > 0)
  ok(result.bytesPerSecond === undefined || result.bytesPerSecond > 0)
})

test('performance regression: batched producer throughput stays within baseline envelope', { timeout: 120_000 }, async t => {
  const result = await runSampledPerformanceBenchmark('producer-batch', () => benchmarkBatchProduce(t))
  const comparison = await compareWithStoredBaseline(result)

  await writeRegressionArtifact(`producer-batch-performance-${performanceLane}`, { result, comparison })

  strictEqual(result.messages, performanceIterations())
  strictEqual(result.runs?.length, performanceSamples())
  ok(result.messagesPerSecond > 0)
  ok(result.bytesPerSecond === undefined || result.bytesPerSecond > 0)
})
