import { deepStrictEqual, strictEqual } from 'node:assert'
import { test } from 'node:test'
import { MessagesStreamModes } from '../../src/index.ts'
import {
  collectMessages,
  createRegressionConsumer,
  createRegressionProducer,
  createRegressionTopic,
  type ConsumedValue
} from '../helpers/index.ts'

function jsonDeserializers () {
  return {
    key: (data: Buffer | undefined) => data?.toString() ?? '',
    value: (data: Buffer | undefined) => JSON.parse(data?.toString() ?? '{}') as ConsumedValue,
    headerKey: (data: Buffer | undefined) => data?.toString() ?? '',
    headerValue: (data: Buffer | undefined) => data?.toString() ?? ''
  }
}

test('regression #128: hot partitions do not starve colder partitions under mixed load', { timeout: 60_000 }, async t => {
  // Produce an intentionally skewed workload. The hot partition has many more
  // messages, but the consumer must still drain the colder partitions.
  const topic = await createRegressionTopic(t, 3)
  const producer = createRegressionProducer(t)
  const counts = [120, 20, 20]
  const messages = []

  for (let index = 0; index < counts[0]; index++) {
    for (const partition of [0, 1, 2]) {
      if (index >= counts[partition]!) {
        continue
      }

      messages.push({
        topic,
        partition,
        key: `${partition}-${index}`,
        value: JSON.stringify({ id: partition * 1000 + index, partition })
      })
    }
  }

  await producer.send({ messages })

  const consumer = createRegressionConsumer<string, ConsumedValue, string, string>(t, { deserializers: jsonDeserializers() })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()

  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: false })
  const consumed = await collectMessages(stream, messages.length)
  const consumedByPartition = new Map<number, number>()

  for (const message of consumed) {
    const partition = message.partition
    consumedByPartition.set(partition, (consumedByPartition.get(partition) ?? 0) + 1)
  }

  // The fairness invariant for this regression is no partition starvation: every
  // assigned partition must fully drain even when one partition is much hotter.
  deepStrictEqual(counts.map((_, partition) => consumedByPartition.get(partition)), counts)
  strictEqual(consumedByPartition.size, 3)
})
