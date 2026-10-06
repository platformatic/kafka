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
  type Offsets
} from '../../src/index.ts'

test('supports the Oracle Streaming Kafka workflow', { timeout: 240_000 }, async t => {
  const bootstrapServers = process.env.ORACLE_STREAMING_BOOTSTRAP_SERVERS?.trim()
  const topic = process.env.ORACLE_STREAMING_TOPIC?.trim()
  const username = process.env.ORACLE_STREAMING_USERNAME?.trim()
  const password = process.env.ORACLE_STREAMING_AUTH_TOKEN?.trim()
  ok(bootstrapServers, 'ORACLE_STREAMING_BOOTSTRAP_SERVERS is required; see docs/regression/oracle-streaming.md')
  ok(topic, 'ORACLE_STREAMING_TOPIC is required; see docs/regression/oracle-streaming.md')
  ok(username, 'ORACLE_STREAMING_USERNAME is required; see docs/regression/oracle-streaming.md')
  ok(password, 'ORACLE_STREAMING_AUTH_TOKEN is required; see docs/regression/oracle-streaming.md')

  const runId = `oracle-streaming-smoke-${randomUUID()}`
  const connectionOptions = {
    bootstrapBrokers: bootstrapServers.split(',').map(broker => broker.trim()),
    tls: { rejectUnauthorized: true },
    tlsServerName: true,
    sasl: { mechanism: 'PLAIN' as const, username, password },
    autocreateTopics: false,
    connectTimeout: 10_000,
    requestTimeout: 30_000,
    timeout: 30_000,
    retries: 2,
    retryDelay: 1000
  }
  // OCI Streaming does not support idempotent production or transactions.
  const producer = new Producer<string, string, string, string>({
    ...connectionOptions,
    clientId: `${runId}-producer`,
    serializers: stringSerializers,
    idempotent: false,
    compression: 'none'
  })
  t.after(() => producer.close())

  const metadata = await producer.metadata({ topics: [topic], forceUpdate: true })
  const partitions = metadata.topics.get(topic)?.partitions
  ok(partitions, 'the smoke test stream must exist')
  strictEqual(partitions.length, 2, 'the smoke test stream must have two partitions')
  ok(
    partitions.every(partition => partition.leader >= 0),
    'both partitions must have active leaders'
  )

  for (const batch of [0, 1, 2]) {
    const records: MessageToProduce<string, string, string, string>[] = [0, 1, 0, 1].map((partition, index) => ({
      topic,
      partition,
      key: `${runId}-${batch}-${index}`,
      value: `batch-${batch}-value-${index}`,
      headers: { runId, batch: String(batch) }
    }))

    // Prepublish the resume batch so an incorrect fallback to LATEST cannot pass.
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
    const stream = await consumer.consume({
      topics: [topic],
      mode: [MessagesStreamModes.EARLIEST, MessagesStreamModes.COMMITTED, MessagesStreamModes.LATEST][batch],
      fallbackMode: MessagesStreamFallbackModes.FAIL,
      maxWaitTime: 1000,
      maxBytes: 1_048_576,
      maxBytesPerPartition: 1_048_576
    })
    addAbortSignal(t.signal, stream)

    let publishAfterFetch = Promise.resolve()
    if (batch === 2) {
      // Observe an actual fetch after LATEST initialization before publishing the final batch.
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
        // A second local execution shares retained records, but never its group ID or message keys.
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
    await Promise.all([publishAfterFetch, receiveMessages()])
    await stream.close()
    strictEqual(messages.length, records.length, 'all records must arrive before the stream ends')

    const offsets: CommitOptionsPartition[] = [0, 1].map(partition => {
      const last = messages.findLast(message => message.partition === partition)!
      return { topic, partition, offset: last.offset + 1n, leaderEpoch: last.leaderEpoch }
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
