import { strictEqual } from 'node:assert'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { MessagesStreamModes } from '../../src/index.ts'
import {
  assertResourceStability,
  collectMessages,
  createRegressionConsumer,
  createRegressionTopic,
  createResourceSampler,
  produceJsonMessages,
  type ConsumedValue,
  writeRegressionArtifact
} from '../helpers/index.ts'

function jsonDeserializers () {
  return {
    key: (data: Buffer | undefined) => data?.toString() ?? '',
    value: (data: Buffer | undefined) => JSON.parse(data?.toString() ?? '{}') as ConsumedValue,
    headerKey: (data: Buffer | undefined) => data?.toString() ?? '',
    headerValue: (data: Buffer | undefined) => data?.toString() ?? ''
  }
}

test('regression: resource sampler captures stable memory during sustained consume', { timeout: 60_000 }, async t => {
  // This is the CI-friendly counterpart to the heavier memory job. It records the
  // same envelope data and fails on obvious runaway growth during a bounded run.
  const total = 300
  const topic = await createRegressionTopic(t, 1)
  await produceJsonMessages(t, topic, total)

  const consumer = createRegressionConsumer<string, ConsumedValue, string, string>(t, { deserializers: jsonDeserializers() })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()

  const sampler = createResourceSampler(50)
  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: true })
  const messages = await collectMessages(stream, total)
  // Leave one extra sampler interval after the stream closes so the final sample
  // captures post-consume memory instead of only in-flight consumption.
  await sleep(100)
  const samples = sampler.stop()

  await writeRegressionArtifact('resource-stability', { samples, total })

  strictEqual(messages.length, total)
  assertResourceStability(samples)
})
