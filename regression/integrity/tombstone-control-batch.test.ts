import { deepStrictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { test } from 'node:test'
import { MessagesStreamModes, Producer, type Serializers, stringSerializers } from '../../src/index.ts'
import { createRegressionConsumer, createRegressionTopic, regressionBootstrapServers } from '../helpers/index.ts'

type MaybeString = string | undefined

function stringOrUndefinedDeserializers () {
  return {
    key: (data: Buffer | undefined) => data?.toString(),
    value: (data: Buffer | undefined) => data?.toString(),
    headerKey: (data: Buffer | undefined) => data?.toString(),
    headerValue: (data: Buffer | undefined) => data?.toString()
  }
}

test('regression: tombstones are delivered and transaction control batches are filtered', { timeout: 60_000 }, async t => {
  const topic = await createRegressionTopic(t, 1)
  const producer = new Producer<string, MaybeString, string, string>({
    clientId: `regression-control-${randomUUID()}`,
    bootstrapBrokers: regressionBootstrapServers,
    serializers: stringSerializers as unknown as Serializers<string, MaybeString, string, string>,
    idempotent: true,
    transactionalId: `regression-control-${randomUUID()}`,
    retryDelay: 500
  })
  t.after(() => producer.close())
  const consumer = createRegressionConsumer<string, MaybeString, string, string>(t, {
    deserializers: stringOrUndefinedDeserializers()
  })
  const seen: Array<{ key: MaybeString; value: MaybeString }> = []
  const done = once(consumer, 'regression:done')

  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()
  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.LATEST, autocommit: false })
  stream.on('data', message => {
    seen.push({ key: message.key, value: message.value })

    if (message.key === 'end') {
      consumer.emit('regression:done')
    }
  })

  const committed = await producer.beginTransaction()
  await committed.send({ messages: [{ topic, key: 'committed', value: 'visible' }] })
  await committed.commit()

  const aborted = await producer.beginTransaction()
  await aborted.send({ messages: [{ topic, key: 'aborted', value: 'hidden' }] })
  await aborted.abort()

  await producer.send({
    messages: [
      { topic, key: 'tombstone', value: undefined },
      { topic, key: 'end', value: 'end' }
    ]
  })

  await done
  await stream.close()

  deepStrictEqual(seen, [
    { key: 'committed', value: 'visible' },
    { key: 'tombstone', value: undefined },
    { key: 'end', value: 'end' }
  ])
})
