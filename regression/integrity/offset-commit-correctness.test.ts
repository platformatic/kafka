import { deepStrictEqual } from 'node:assert'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { MessagesStreamModes } from '../../src/index.ts'
import {
  assertCommittedOffset,
  collectMessages,
  createRegressionConsumer,
  createRegressionGroupId,
  createRegressionTopic,
  produceJsonMessages,
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

test('regression: manual commit advances to the next consumed offset', { timeout: 60_000 }, async t => {
  const topic = await createRegressionTopic(t, 1)
  await produceJsonMessages(t, topic, 5)

  const consumer = createRegressionConsumer<string, ConsumedValue, string, string>(t, { deserializers: jsonDeserializers() })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()

  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: false })
  const messages = await collectMessages(stream, 3)
  await messages.at(-1)!.commit()

  await assertCommittedOffset(consumer, topic, 0, messages.at(-1)!.offset + 1n)
})

test('regression: timed autocommit and close persist processed offsets', { timeout: 60_000 }, async t => {
  const topic = await createRegressionTopic(t, 1)
  const groupId = createRegressionGroupId()
  await produceJsonMessages(t, topic, 5)

  const consumer = createRegressionConsumer<string, ConsumedValue, string, string>(t, {
    groupId,
    deserializers: jsonDeserializers()
  })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()

  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: 100 })
  const messages = await collectMessages(stream, 5)
  await sleep(200)
  await assertCommittedOffset(consumer, topic, 0, messages.at(-1)!.offset + 1n)

  const nextConsumer = createRegressionConsumer<string, ConsumedValue, string, string>(t, {
    groupId,
    deserializers: jsonDeserializers()
  })
  await nextConsumer.topics.trackAll(topic)
  await nextConsumer.joinGroup()
  const committed = await nextConsumer.listCommittedOffsets({ topics: [{ topic, partitions: [0] }] })

  deepStrictEqual(committed.get(topic), [5n])
})
