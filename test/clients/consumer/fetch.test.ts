import { strictEqual, deepStrictEqual, partialDeepStrictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test, type TestContext } from 'node:test'

import { type FetchResponse } from '../../../src/apis/consumer/fetch-v17.ts'

import {
  Consumer,
  type ConsumerOptions,
  type Deserializers,
  ProduceAcks,
  Producer,
  type ProducerOptions,
  type Serializers,
  stringDeserializers,
  stringSerializers,
  type MessageToProduce,
  sleep,
  type RecordsBatch
} from '../../../src/index.ts'
import { createTopic } from '../../helpers.ts'

const kafkaBootstrapServers = ['localhost:9092']

// Helper functions
function createTestGroupId () {
  return `test-consumer-group-${randomUUID()}`
}

function createProducer<K = string, V = string, HK = string, HV = string> ({
  t,
  options
}: {
  t: TestContext,
  options?: Partial<ProducerOptions<K, V, HK, HV>>
}): Producer<K, V, HK, HV> {
  options ??= {}

  const producer = new Producer({
    clientId: `test-producer-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    serializers: stringSerializers as Serializers<K, V, HK, HV>,
    autocreateTopics: false,
    ...options
  })

  t.after(() => producer.close())

  return producer
}

// This helper creates a consumer and ensure proper termination
function createConsumer<K = string, V = string, HK = string, HV = string> ({
  t,
  groupId,
  options
}: {
  t: TestContext,
  groupId?: string,
  options?: Partial<ConsumerOptions<K, V, HK, HV>>
}): Consumer<K, V, HK, HV> {
  options ??= {}

  const consumer = new Consumer<K, V, HK, HV>({
    clientId: `test-consumer-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    groupId: groupId ?? createTestGroupId(),
    deserializers: stringDeserializers as Deserializers<K, V, HK, HV>,
    timeout: 1000,
    sessionTimeout: 6000,
    rebalanceTimeout: 6000,
    heartbeatInterval: 1000,
    retries: 1,
    ...options
  })

  t.after(async () => {
    await consumer.close(true)
  })

  return consumer
}

// This function produces sample messages to a topic for testing the consumer
async function produceTestMessages ({
  t,
  messages,
  batchSize = 3,
  delay = 0,
  options
}: {
  t: TestContext,
  messages: MessageToProduce<string, string, string, string>[],
  batchSize?: number,
  delay?: number,
  options?: Partial<ProducerOptions<string, string, string, string>>
}): Promise<void> {
  const producer = createProducer({ t, options })

  for (let i = 0; i < messages.length; i += batchSize) {
    await producer.send({
      messages: messages.slice(i, i + batchSize),
      acks: ProduceAcks.LEADER
    })
    await sleep(delay)
  }
}

async function fetchFromOffset ({
  consumer,
  topic,
  fetchOffset = 0n,
  partition = 0
}: {
  consumer: Consumer<string, string, string, string>,
  topic: string,
  fetchOffset: bigint,
  partition?: number
}): Promise<FetchResponse> {
  const metadata = await consumer.metadata({ topics: [topic] })
  const { id: topicId, partitions } = metadata.topics.get(topic)!
  const reqPartition = partitions[partition]
  return consumer.fetch({
    node: reqPartition.leader,
    topics: [
      {
        topicId,
        partitions: [
          {
            partition,
            currentLeaderEpoch: reqPartition.leaderEpoch,
            fetchOffset,
            lastFetchedEpoch: -1,
            partitionMaxBytes: 1048576 // Default max bytes per partition
          }
        ]
      }
    ]
  })
}

test('fetch should retrieve messages', { only: true }, async t => {
  const groupId = createTestGroupId()
  const topic = await createTopic(t, true)
  const count = 9
  const batchSize = 3

  const messages = Array.from({ length: count }, (_, i) => ({
    topic,
    key: `key-${i}`,
    value: `value-${i}`,
    headers: { headerKey: `headerValue-${i}` }
  }))

  // Produce test messages
  await produceTestMessages({
    t,
    messages,
    batchSize,
    delay: 100,
    options: {
      partitioner: () => 0
    }
  })

  const consumer = createConsumer({
    t,
    groupId,
    options: {
      minBytes: 1024 * 1024,
      maxBytes: 1024 * 1024 * 10,
      maxWaitTime: 500
    }
  })
  const fetchResult = await fetchFromOffset({
    consumer,
    topic,
    fetchOffset: 0n,
    partition: 0
  })

  strictEqual(fetchResult.errorCode, 0, 'Should succeed fetching')
  strictEqual(fetchResult.responses.length, 1, 'Should return one topic')
  strictEqual(fetchResult.responses[0].partitions.length, 1, 'Should return one partition')
  const fetchPartition = fetchResult.responses[0].partitions[0]!
  strictEqual(fetchPartition.errorCode, 0, 'Should succeed fetching partition')
  strictEqual(fetchPartition.records?.length, 3, 'Should return all batches')
  for (let batchNo = 0; batchNo < fetchPartition.records.length; ++batchNo) {
    const recordsBatch: RecordsBatch = fetchPartition.records[batchNo]
    partialDeepStrictEqual(recordsBatch, {
      attributes: 0,
      magic: 2,
      firstOffset: BigInt(batchNo * batchSize),
      lastOffsetDelta: batchSize - 1,
      length: 178
    }, 'Should return records batch with correct metadata')
    const records = recordsBatch.records
    strictEqual(records.length, batchSize, 'Should get all messages in batch')

    const fetchMessages = records.map((record, i) => {
      strictEqual(record.offsetDelta, i, 'Should have correct offset delta for each record')
      return {
        topic,
        key: record.key.toString('utf-8'),
        value: record.value.toString('utf-8'),
        headers: Object.fromEntries(record.headers.map(([key, value]) => [key.toString('utf-8'), value.toString('utf-8')]))
      }
    })
    deepStrictEqual(fetchMessages, messages.slice(batchNo * batchSize, batchNo * batchSize + batchSize), 'Should match produced messages in batch')
  }
})
