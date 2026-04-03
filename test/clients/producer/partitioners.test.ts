import { strictEqual } from 'node:assert'
import { test } from 'node:test'
import { compatibilityPartitioner, murmur2, ProduceAcks, stringSerializers } from '../../../src/index.ts'
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
