import { strictEqual } from 'node:assert'
import { test } from 'node:test'
import { MessagesStreamModes } from '../../src/index.ts'
import {
  assertContiguousIds,
  assertNoDuplicateIds,
  collectMessages,
  createRegressionConsumer,
  createRegressionProducer,
  createRegressionTopic,
  regressionBootstrapServers,
  type ConsumedValue
} from '../helpers/index.ts'

function compatibilityBootstrapServers (): string[] {
  return (process.env.REGRESSION_COMPAT_BOOTSTRAP_SERVERS ?? regressionBootstrapServers.join(','))
    .split(',')
    .map(broker => broker.trim())
    .filter(Boolean)
}

function jsonDeserializers () {
  return {
    key: (data: Buffer | undefined) => data?.toString() ?? '',
    value: (data: Buffer | undefined) => JSON.parse(data?.toString() ?? '{}') as ConsumedValue,
    headerKey: (data: Buffer | undefined) => data?.toString() ?? '',
    headerValue: (data: Buffer | undefined) => data?.toString() ?? ''
  }
}

test('regression: reduced compatibility lane produces and consumes on configured Kafka version', { timeout: 60_000 }, async t => {
  // The workflow can run this same test against oldest/newest Kafka by changing
  // REGRESSION_COMPAT_BOOTSTRAP_SERVERS and KAFKA_VERSION, without changing code.
  const bootstrapBrokers = compatibilityBootstrapServers()
  const total = 12
  const topic = await createRegressionTopic(t, 1, bootstrapBrokers)
  const producer = createRegressionProducer(t, { bootstrapBrokers })

  await producer.send({
    messages: Array.from({ length: total }, (_, id) => ({ topic, key: String(id), value: JSON.stringify({ id }) }))
  })

  const consumer = createRegressionConsumer<string, ConsumedValue, string, string>(t, {
    bootstrapBrokers,
    deserializers: jsonDeserializers()
  })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()

  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: false })
  const messages = await collectMessages(stream, total)

  assertNoDuplicateIds(messages)
  assertContiguousIds(messages, total)
  strictEqual(typeof (process.env.KAFKA_VERSION ?? 'default'), 'string')
})
