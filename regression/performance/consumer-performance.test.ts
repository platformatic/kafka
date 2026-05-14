import { ok, strictEqual } from 'node:assert'
import { test } from 'node:test'
import {
  benchmarkConsumerStream,
  compareWithStoredBaseline,
  performanceIterations,
  performanceLane,
  performanceSamples,
  runSampledPerformanceBenchmark
} from './helpers.ts'
import { writeRegressionArtifact } from '../helpers/index.ts'

test('performance regression: stream consumer throughput stays within baseline envelope', { timeout: 120_000 }, async t => {
  const result = await runSampledPerformanceBenchmark('consumer-stream', () => benchmarkConsumerStream(t))
  const comparison = await compareWithStoredBaseline(result)

  await writeRegressionArtifact(`consumer-stream-performance-${performanceLane}`, { result, comparison })

  strictEqual(result.messages, performanceIterations())
  strictEqual(result.runs?.length, performanceSamples())
  ok(result.messagesPerSecond > 0)
  ok(result.bytesPerSecond === undefined || result.bytesPerSecond > 0)
})
