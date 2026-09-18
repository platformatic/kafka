import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { addAbortSignal } from 'node:stream'
import { test } from 'node:test'
import {
  Consumer,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  ProduceAcks,
  Producer,
  stringDeserializers,
  stringSerializers,
  type CommitOptionsPartition,
  type Message,
  type MessageToProduce,
  type MessagesStream,
  type Offsets
} from '../../src/index.ts'

test('supports the Azure Event Hubs Kafka workflow', { timeout: 240_000 }, async t => {
  const bootstrapServers = process.env.EVENTHUBS_BOOTSTRAP_SERVERS?.trim()
  const topic = process.env.EVENTHUBS_TOPIC?.trim()
  const password = process.env.EVENTHUBS_CONNECTION_STRING?.trim()
  ok(bootstrapServers, 'EVENTHUBS_BOOTSTRAP_SERVERS is required; see docs/eventhubs.md')
  ok(topic, 'EVENTHUBS_TOPIC is required; see docs/eventhubs.md')
  ok(password, 'EVENTHUBS_CONNECTION_STRING is required; see docs/eventhubs.md')

  const runId = `eventhubs-smoke-${randomUUID()}`
  const connectionOptions = {
    bootstrapBrokers: bootstrapServers.split(',').map(broker => broker.trim()),
    tls: { rejectUnauthorized: true },
    tlsServerName: true,
    sasl: { mechanism: 'PLAIN' as const, username: '$ConnectionString', password },
    autocreateTopics: false,
    connectTimeout: 10_000,
    requestTimeout: 60_000,
    timeout: 60_000,
    retries: 2,
    retryDelay: 1000
  }
  const producer = new Producer<string, string, string, string>({
    ...connectionOptions,
    clientId: `${runId}-producer`,
    serializers: stringSerializers,
    compression: 'none'
  })
  t.after(() => producer.close())

  const metadata = await producer.metadata({ topics: [topic], forceUpdate: true })
  const partitions = metadata.topics.get(topic)?.partitions
  ok(partitions, 'the smoke test Event Hub must exist')
  strictEqual(partitions.length, 2, 'the smoke test Event Hub must have two partitions')
  ok(
    partitions.every(partition => partition.leader >= 0),
    'both partitions must have active leaders'
  )

  // Reuse the group only within this run, so retained messages and other runs cannot alter its commits.
  for (const batch of [0, 1, 2]) {
    const records: MessageToProduce<string, string, string, string>[] = [0, 1, 0, 1].map((partition, index) => ({
      topic,
      partition,
      key: `${runId}-${batch}-${index}`,
      value: `batch-${batch}-value-${index}`,
      headers: { runId, batch: String(batch) }
    }))

    // Prepublish the first two batches so a broken COMMITTED resume falling back to LATEST must fail.
    if (batch < 2) {
      await producer.send({ messages: records, acks: ProduceAcks.ALL })
    }

    const consumer = new Consumer<string, string, string, string>({
      ...connectionOptions,
      clientId: `${runId}-consumer-${batch}`,
      groupId: runId,
      groupProtocol: 'classic',
      deserializers: stringDeserializers,
      autocommit: false,
      sessionTimeout: 30_000,
      rebalanceTimeout: 60_000,
      heartbeatInterval: 3000
    })
    t.after(() => consumer.close(true))

    const stream: MessagesStream<string, string, string, string> = await consumer.consume({
      topics: [topic],
      mode: [MessagesStreamModes.EARLIEST, MessagesStreamModes.COMMITTED, MessagesStreamModes.LATEST][batch],
      fallbackMode: MessagesStreamFallbackModes.FAIL,
      maxWaitTime: 1000
    })
    // Abort a stalled fetch when the test deadline expires, allowing cleanup to close the clients.
    addAbortSignal(t.signal, stream)

    // LATEST starts beyond retained records. Wait for an actual fetch before publishing, rather than
    // sleeping or assuming consume() has already initialized offsets and started fetching.
    let publishAfterFetch = Promise.resolve()
    if (batch === 2) {
      publishAfterFetch = once(stream, 'fetch', { signal: t.signal }).then(async () => {
        ok(!stream.destroyed, 'the initial fetch must not destroy the stream')
        for (const partition of [0, 1]) {
          ok(stream.offsetsToFetch.has(`${topic}:${partition}`), 'both partition offsets must be initialized')
        }
        await producer.send({ messages: records, acks: ProduceAcks.ALL })
      })
    }

    const messages: Message<string, string, string, string>[] = []
    const receiveMessages = async () => {
      for await (const message of stream) {
        // The Event Hub persists across executions; ignore records from other runs.
        if (!message.key?.startsWith(`${runId}-`)) {
          continue
        }

        const partitionMessages = messages.filter(received => received.partition === message.partition)
        const expected = records.filter(record => record.partition === message.partition)[partitionMessages.length]
        ok(expected, 'received an unexpected record for this run')
        deepStrictEqual(
          { topic: message.topic, partition: message.partition, key: message.key, value: message.value },
          { topic, partition: expected.partition, key: expected.key, value: expected.value }
        )
        deepStrictEqual(message.headerEntries, Object.entries(expected.headers!))
        strictEqual(typeof message.timestamp, 'bigint')
        ok(message.offset >= 0n)
        if (partitionMessages.length > 0) {
          ok(message.offset > partitionMessages[partitionMessages.length - 1].offset)
        }

        messages.push(message)
        if (messages.length === records.length) {
          break
        }
      }
    }
    // Reading starts the fetch loop; observe both promises immediately so errors from either path fail the test.
    await Promise.all([publishAfterFetch, receiveMessages()])
    await stream.close()
    strictEqual(messages.length, records.length, 'all records must arrive before the stream ends')

    const offsets: CommitOptionsPartition[] = [0, 1].map(partition => {
      const lastMessage = messages.findLast(message => message.partition === partition)!
      return { topic, partition, offset: lastMessage.offset + 1n, leaderEpoch: lastMessage.leaderEpoch }
    })
    await consumer.commit({ offsets })
    const committed: Offsets = await consumer.listCommittedOffsets({ topics: [{ topic, partitions: [0, 1] }] })
    deepStrictEqual(
      committed.get(topic),
      offsets.map(({ offset }) => offset)
    )
    await consumer.close(true)
    t.diagnostic(
      [
        'Initial consumption and commit succeeded',
        'Resume from committed offsets succeeded',
        'Consumption of messages published after the initial fetch succeeded'
      ][batch]
    )
  }
})
