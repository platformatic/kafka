import { MessagesStreamModes, ProduceAcks } from '../../src/index.ts'
import {
  collectMessages,
  createRegressionConsumer,
  createRegressionProducer,
  createRegressionTopic,
  type BenchmarkResult,
  type ConsumedValue,
  writeRegressionArtifact
} from './index.ts'
import { runRegressionBenchmark } from './benchmark-harness.ts'
import { createBaselineDocument, createBaselineStore } from './baseline-store.ts'
import { compareBenchmarkBaseline } from './compare-baseline.ts'
import {
  performanceIterations,
  performanceSamples,
  performanceWarmups,
  runSampledPerformanceBenchmark
} from '../performance/helpers.ts'

const lane = process.env.REGRESSION_LANE ?? 'local'
const iterations = performanceIterations()

function jsonDeserializers () {
  return {
    key: (data: Buffer | undefined) => data?.toString() ?? '',
    value: (data: Buffer | undefined) => JSON.parse(data?.toString() ?? '{}') as ConsumedValue,
    headerKey: (data: Buffer | undefined) => data?.toString() ?? '',
    headerValue: (data: Buffer | undefined) => data?.toString() ?? ''
  }
}

async function benchmarkSingleProduce (): Promise<BenchmarkResult> {
  const topic = await createRegressionTopic({ after: () => {} } as never, 1)
  const producer = createRegressionProducer({ after: () => {} } as never)

  try {
    return await runRegressionBenchmark('producer-single', iterations, async () => {
      for (let id = 0; id < iterations; id++) {
        await producer.send({
          messages: [{ topic, key: String(id), value: JSON.stringify({ id }) }],
          acks: ProduceAcks.LEADER
        })
      }
    })
  } finally {
    await producer.close()
  }
}

async function benchmarkBatchProduce (): Promise<BenchmarkResult> {
  const topic = await createRegressionTopic({ after: () => {} } as never, 1)
  const producer = createRegressionProducer({ after: () => {} } as never)

  try {
    return await runRegressionBenchmark('producer-batch', iterations, async () => {
      await producer.send({
        messages: Array.from({ length: iterations }, (_, id) => ({ topic, key: String(id), value: JSON.stringify({ id }) })),
        acks: ProduceAcks.LEADER
      })
    })
  } finally {
    await producer.close()
  }
}

async function benchmarkConsumer (): Promise<BenchmarkResult> {
  const topic = await createRegressionTopic({ after: () => {} } as never, 1)
  const producer = createRegressionProducer({ after: () => {} } as never)
  const consumer = createRegressionConsumer<string, ConsumedValue, string, string>({ after: () => {} } as never, {
    deserializers: jsonDeserializers()
  })

  try {
    await producer.send({
      messages: Array.from({ length: iterations }, (_, id) => ({ topic, key: String(id), value: JSON.stringify({ id }) }))
    })
    await consumer.topics.trackAll(topic)
    await consumer.joinGroup()
    const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: false })

    return await runRegressionBenchmark('consumer-stream', iterations, async () => {
      await collectMessages(stream, iterations)
    })
  } finally {
    await consumer.close(true)
    await producer.close()
  }
}

async function main (): Promise<void> {
  const samples = performanceSamples()
  const warmups = performanceWarmups()
  const results = {
    'producer-single': await runSampledPerformanceBenchmark('producer-single', benchmarkSingleProduce, samples, warmups),
    'producer-batch': await runSampledPerformanceBenchmark('producer-batch', benchmarkBatchProduce, samples, warmups),
    'consumer-stream': await runSampledPerformanceBenchmark('consumer-stream', benchmarkConsumer, samples, warmups)
  }

  for (const result of Object.values(results)) {
    console.log(
      `${result.name}: ${result.messagesPerSecond.toFixed(2)} messages/s, ${result.durationMs.toFixed(2)}ms median over ${samples} samples`
    )
  }

  const store = createBaselineStore()
  const baseline = await store.read(lane)

  if (baseline) {
    for (const [name, result] of Object.entries(results)) {
      const previous = baseline.results[name]
      if (previous) {
        compareBenchmarkBaseline(result, previous)
      } else {
        console.warn(`No regression baseline found for ${lane}/${name}; skipping comparison.`)
      }
    }
  } else {
    console.warn(`No regression baseline found for ${lane}; skipping comparison.`)
  }

  const document = createBaselineDocument(lane, results, {
    commit: process.env.GITHUB_SHA,
    runner: process.env.RUNNER_NAME,
    kafkaVersion: process.env.KAFKA_VERSION
  })

  await writeRegressionArtifact(`benchmark-${lane}`, document)
  await store.write(lane, document)
}

await main()
