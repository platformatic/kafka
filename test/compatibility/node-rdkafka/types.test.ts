import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  AdminClient,
  HighLevelProducer,
  KafkaConsumer,
  Producer,
  type ConsumerGlobalConfig,
  type ConsumerTopicConfig,
  type ProducerGlobalConfig,
  type ProducerTopicConfig
} from '../../../src/compatibility/node-rdkafka/index.ts'

test('node-rdkafka public overloads compile', () => {
  const verifyTypes = (): void => {
    const producerConfig: ProducerGlobalConfig = {
      'bootstrap.servers': 'localhost:9092',
      'transactional.id': 'transactions'
    }
    const producerTopicConfig: ProducerTopicConfig = { 'compression.codec': 'gzip' }
    const consumerConfig: ConsumerGlobalConfig = {
      'bootstrap.servers': 'localhost:9092',
      'group.id': 'workers'
    }
    const consumerTopicConfig: ConsumerTopicConfig = { 'auto.offset.reset': 'earliest' }

    const producer = new Producer(producerConfig, producerTopicConfig)
    const highLevel = new HighLevelProducer(producerConfig, producerTopicConfig)
    const consumer = new KafkaConsumer(consumerConfig, consumerTopicConfig)
    const admin = AdminClient.create({ 'bootstrap.servers': 'localhost:9092' })
    const expectVoid = (_value: void): void => {}

    producer.initTransactions(() => {})
    producer.initTransactions(100, () => {})
    producer.commitTransaction(() => {})
    producer.abortTransaction(100, () => {})
    producer.sendOffsetsToTransaction([{ topic: 'events', partition: 0, offset: 1 }], consumer, () => {})
    highLevel.setKeySerializer(key => typeof key === 'string' ? key : null)
    highLevel.setValueSerializer((value, callback) => callback(null, Buffer.from(String(value))))
    expectVoid(highLevel.produce('events', null, 'value', 'key', null, () => {}))
    expectVoid(highLevel.produce('events', null, 'value', 'key', null, [{ source: 'test' }], () => {}))
    consumer.committed(100, () => {})
    consumer.committed([{ topic: 'events', partition: 0 }], 100, (_error, offsets) => offsets?.[0].offset.toFixed())
    consumer.offsetsForTimes([{ topic: 'events', partition: 0, offset: 0 }], () => {})
    consumer.on('ready', (_info, metadata) => metadata.topics)
    producer.once('delivery-report', (_error, report) => report.offset)
    admin.createTopic({ topic: 'events', num_partitions: 1, replication_factor: 1 }, () => {})
  }
  assert.equal(typeof verifyTypes, 'function')
})
