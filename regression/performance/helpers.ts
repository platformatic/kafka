import type { TestContext } from 'node:test'
import { MessagesStreamModes, ProduceAcks } from '../../src/index.ts'
import { createBaselineStore } from '../helpers/baseline-store.ts'
import { runRegressionBenchmark } from '../helpers/benchmark-harness.ts'
import { compareBenchmarkBaseline, type BaselineComparison, type BaselineThresholds } from '../helpers/compare-baseline.ts'
import {
  collectMessages,
  createRegressionConsumer,
  createRegressionProducer,
  createRegressionTopic,
  type BenchmarkResult,
  type ConsumedValue
} from '../helpers/index.ts'

export const performanceLane = process.env.REGRESSION_LANE ?? 'local'

export function performanceIterations (): number {
  return Number(process.env.REGRESSION_PERFORMANCE_MESSAGES ?? process.env.REGRESSION_BENCHMARK_MESSAGES ?? 100)
}

export function performanceSamples (): number {
  return Number(process.env.REGRESSION_PERFORMANCE_SAMPLES ?? 3)
}

export function performanceWarmups (): number {
  return Number(process.env.REGRESSION_PERFORMANCE_WARMUPS ?? 0)
}

export function performanceThresholds (): BaselineThresholds {
  return {
    throughputFailRatio: Number(process.env.REGRESSION_THROUGHPUT_FAIL_RATIO ?? 0.85),
    latencyFailRatio: Number(process.env.REGRESSION_LATENCY_FAIL_RATIO ?? 1.2),
    memoryFailRatio: process.env.REGRESSION_MEMORY_FAIL_RATIO
      ? Number(process.env.REGRESSION_MEMORY_FAIL_RATIO)
      : undefined
  }
}

export function jsonDeserializers () {
  return {
    key: (data: Buffer | undefined) => data?.toString() ?? '',
    value: (data: Buffer | undefined) => JSON.parse(data?.toString() ?? '{}') as ConsumedValue,
    headerKey: (data: Buffer | undefined) => data?.toString() ?? '',
    headerValue: (data: Buffer | undefined) => data?.toString() ?? ''
  }
}

export async function benchmarkSingleProduce (t: TestContext, iterations = performanceIterations()): Promise<BenchmarkResult> {
  const topic = await createRegressionTopic(t, 1)
  const producer = createRegressionProducer(t)
  const bytes = Array.from({ length: iterations }, (_, id) => Buffer.byteLength(JSON.stringify({ id }))).reduce(
    (total, size) => total + size,
    0
  )

  return runRegressionBenchmark('producer-single', iterations, async () => {
    for (let id = 0; id < iterations; id++) {
      await producer.send({
        messages: [{ topic, key: String(id), value: JSON.stringify({ id }) }],
        acks: ProduceAcks.LEADER
      })
    }

    return { bytes }
  })
}

export async function benchmarkBatchProduce (t: TestContext, iterations = performanceIterations()): Promise<BenchmarkResult> {
  const topic = await createRegressionTopic(t, 1)
  const producer = createRegressionProducer(t)
  const messages = Array.from({ length: iterations }, (_, id) => ({ topic, key: String(id), value: JSON.stringify({ id }) }))
  const bytes = messages.reduce((total, message) => total + Buffer.byteLength(message.value), 0)

  return runRegressionBenchmark('producer-batch', iterations, async () => {
    await producer.send({ messages, acks: ProduceAcks.LEADER })
    return { bytes }
  })
}

export async function benchmarkConsumerStream (t: TestContext, iterations = performanceIterations()): Promise<BenchmarkResult> {
  const topic = await createRegressionTopic(t, 1)
  const producer = createRegressionProducer(t)
  const messages = Array.from({ length: iterations }, (_, id) => ({ topic, key: String(id), value: JSON.stringify({ id }) }))
  const bytes = messages.reduce((total, message) => total + Buffer.byteLength(message.value), 0)
  const consumer = createRegressionConsumer<string, ConsumedValue, string, string>(t, {
    deserializers: jsonDeserializers()
  })

  await producer.send({ messages })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()

  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: false })

  return runRegressionBenchmark('consumer-stream', iterations, async () => {
    await collectMessages(stream, iterations)
    return { bytes }
  })
}

export async function runSampledPerformanceBenchmark (
  name: string,
  run: () => Promise<BenchmarkResult>,
  samples = performanceSamples(),
  warmups = performanceWarmups()
): Promise<BenchmarkResult> {
  for (let index = 0; index < warmups; index++) {
    await run()
  }

  const runs: BenchmarkResult[] = []

  for (let index = 0; index < samples; index++) {
    runs.push(await run())
  }

  return aggregateBenchmarkRuns(name, runs)
}

export function aggregateBenchmarkRuns (name: string, runs: BenchmarkResult[]): BenchmarkResult {
  if (runs.length === 0) {
    throw new Error('Cannot aggregate zero benchmark runs.')
  }

  return {
    name,
    startedAt: runs[0]!.startedAt,
    durationMs: median(runs.map(run => run.durationMs))!,
    messages: runs[0]!.messages,
    messagesPerSecond: median(runs.map(run => run.messagesPerSecond))!,
    bytes: runs[0]!.bytes,
    bytesPerSecond: median(runs.map(run => run.bytesPerSecond).filter(value => typeof value === 'number')),
    samples: runs.flatMap(run => run.samples ?? []),
    runs
  }
}

export async function compareWithStoredBaseline (result: BenchmarkResult): Promise<BaselineComparison | undefined> {
  const store = createBaselineStore()
  const baseline = await store.read(performanceLane)
  const previous = baseline?.results[result.name]

  if (previous) {
    return compareBenchmarkBaseline(result, previous, performanceThresholds())
  }

  console.warn(`No regression baseline found for ${performanceLane}/${result.name}; skipping comparison.`)

  return undefined
}

function median (values: number[]): number | undefined
function median (values: Array<number | undefined>): number | undefined
function median (values: Array<number | undefined>): number | undefined {
  const sorted = values.filter(value => typeof value === 'number').sort((a, b) => a - b)

  if (sorted.length === 0) {
    return undefined
  }

  const middle = Math.floor(sorted.length / 2)

  if (sorted.length % 2 === 1) {
    return sorted[middle]
  }

  return (sorted[middle - 1]! + sorted[middle]!) / 2
}
