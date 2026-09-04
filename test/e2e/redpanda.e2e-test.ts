import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import {
  Admin,
  Consumer,
  MessagesStreamModes,
  ProduceAcks,
  Producer,
  stringDeserializers,
  stringSerializers
} from '../../src/index.ts'

const bootstrapBrokers = (process.env.REDPANDA_BOOTSTRAP_SERVERS ?? 'localhost:19092').split(',')

test('supports the common Redpanda Kafka workflow', async t => {
  const topic = `redpanda-smoke-${randomUUID()}`
  const groupId = `redpanda-smoke-${randomUUID()}`
  const admin = new Admin({
    clientId: `redpanda-admin-${randomUUID()}`,
    bootstrapBrokers,
    retryDelay: 250
  })
  const producer = new Producer<string, string, string, string>({
    clientId: `redpanda-producer-${randomUUID()}`,
    bootstrapBrokers,
    serializers: stringSerializers,
    autocreateTopics: false,
    retryDelay: 250
  })

  t.after(async () => {
    await producer.close()
    await admin.deleteTopics({ topics: [topic] }).catch(() => {})
    await admin.close()
  })

  const created = await admin.createTopics({ topics: [topic], partitions: 2, replicas: 1 })
  strictEqual(created.length, 1)
  strictEqual(created[0].name, topic)

  const metadata = await admin.metadata({ topics: [topic], forceUpdate: true })
  const topicMetadata = metadata.topics.get(topic)
  strictEqual(topicMetadata?.partitions.length, 2)
  ok(
    topicMetadata?.partitions.every(partition => partition.leader >= 0),
    'topic should have active leaders'
  )

  const records = [
    { topic, key: 'red', value: 'apple', partition: 0, headers: { color: 'warm' } },
    { topic, key: 'blue', value: 'berry', partition: 1, headers: { color: 'cool' } },
    { topic, key: 'green', value: 'pear', partition: 0, headers: { color: 'fresh' } },
    { topic, key: 'yellow', value: 'lemon', partition: 1, headers: { color: 'bright' } }
  ]
  await producer.send({ messages: records, acks: ProduceAcks.ALL })

  const consumer = new Consumer<string, string, string, string>({
    clientId: `redpanda-consumer-${randomUUID()}`,
    bootstrapBrokers,
    groupId,
    deserializers: stringDeserializers,
    autocommit: false,
    retryDelay: 250,
    sessionTimeout: 6000,
    rebalanceTimeout: 6000,
    heartbeatInterval: 1000
  })
  t.after(() => consumer.close(true))

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    maxFetches: 1,
    maxWaitTime: 1000
  })
  const messages = await Array.fromAsync(stream)
  await stream.close()

  messages.sort((left, right) => Number(left.offset - right.offset))
  deepStrictEqual(
    messages.map(({ key, value, partition }) => ({ key, value, partition })),
    records.map(({ key, value, partition }) => ({ key, value, partition }))
  )
  deepStrictEqual(
    messages.map(message => message.headerEntries),
    records.map(record => [['color', record.headers.color]])
  )
  ok(messages.every(message => typeof message.timestamp === 'bigint'))
  ok(messages.every(message => message.offset >= 0n))

  await consumer.commit({
    offsets: [0, 1].map(partition => {
      const lastMessage = messages.findLast(message => message.partition === partition)!
      return {
        topic,
        partition,
        offset: lastMessage.offset + 1n,
        leaderEpoch: lastMessage.leaderEpoch
      }
    })
  })
  const committed = await consumer.listCommittedOffsets({ topics: [{ topic, partitions: [0, 1] }] })
  deepStrictEqual(committed.get(topic), [2n, 2n])

  await consumer.close(true)
  const resumedConsumer = new Consumer<string, string, string, string>({
    clientId: `redpanda-resumed-consumer-${randomUUID()}`,
    bootstrapBrokers,
    groupId,
    deserializers: stringDeserializers,
    retryDelay: 250,
    sessionTimeout: 6000,
    rebalanceTimeout: 6000,
    heartbeatInterval: 1000
  })
  t.after(() => resumedConsumer.close(true))

  const resumedStream = await resumedConsumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.COMMITTED,
    maxFetches: 1,
    maxWaitTime: 1000
  })
  deepStrictEqual(await Array.fromAsync(resumedStream), [])
  await resumedStream.close()
})
