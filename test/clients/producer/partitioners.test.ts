import { strictEqual } from 'node:assert'
import { test } from 'node:test'
import {
  compatibilityPartitioner,
  murmur2,
  ProduceAcks,
  stringSerializers,
  type ClusterMetadata,
  type Partitioner
} from '../../../src/index.ts'
import { createProducer, createTopic } from '../../helpers.ts'

test('compatibilityPartitioner should match Java/KafkaJs positive-toPositive behavior', () => {
  const message = { topic: 'topic', key: Buffer.from('partition-key') }
  const expected = murmur2(message.key) & 0x7fffffff

  strictEqual(compatibilityPartitioner(message, message.key), expected)
  strictEqual(compatibilityPartitioner(message, message.key) > -1, true)
})

test('compatibilityPartitioner should normalize string keys using UTF-8 bytes', () => {
  const message = { topic: 'topic', key: 'string-key' }

  strictEqual(compatibilityPartitioner(message, Buffer.from(message.key)), murmur2(message.key) & 0x7fffffff)
})

test('custom partitioner should accept metadata context', () => {
  const metadata: ClusterMetadata = {
    id: 'cluster-id',
    brokers: new Map(),
    controllerId: 0,
    topics: new Map([
      [
        'topic',
        {
          id: 'topic-id',
          partitions: [
            { leader: -1, leaderEpoch: 0, replicas: [0], isr: [], offlineReplicas: [0] },
            { leader: 1, leaderEpoch: 0, replicas: [1], isr: [1], offlineReplicas: [] }
          ],
          partitionsCount: 2,
          lastUpdate: Date.now()
        }
      ]
    ]),
    lastUpdate: Date.now()
  }

  const partitioner: Partitioner<string, string, string, string> = (message, _key, context) => {
    const partitions = context.metadata!.topics.get(message.topic)!.partitions

    return partitions.findIndex(partition => partition.leader >= 0)
  }

  strictEqual(partitioner({ topic: 'topic', value: 'payload' }, undefined, { metadata }), 1)
})

test('producer should inject cached metadata into custom partitioner', async t => {
  const producer = createProducer<string, string, string, string>(t, {
    serializers: stringSerializers
  })
  const testTopic = await createTopic(t, true, 2)

  await producer.metadata({ topics: [testTopic], forceUpdate: true })

  const partitions = producer.getSendTopicPartitions({
    messages: [{ topic: testTopic, value: 'payload' }],
    partitioner: (message, _key, context) => {
      const topic = context.metadata!.topics.get(message.topic)!

      strictEqual(topic.partitionsCount, 2)

      return 1
    }
  })

  strictEqual(partitions.get(testTopic)!.has(1), true)
})

test('compatibilityPartitioner should produce expected partition inside produce flow', async t => {
  const producer = createProducer<string, string, string, string>(t, {
    serializers: stringSerializers,
    partitioner: compatibilityPartitioner
  })

  const testTopic = await createTopic(t, true, 4)
  const key = 'compat-topic-key'
  const expectedPartition = compatibilityPartitioner({ topic: testTopic, key }, Buffer.from(key)) % 4

  const result = await producer.send({
    messages: [
      {
        topic: testTopic,
        key,
        value: 'payload'
      }
    ],
    acks: ProduceAcks.LEADER
  })

  strictEqual(result.offsets![0].partition, expectedPartition)
})
