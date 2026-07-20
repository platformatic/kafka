import assert from 'node:assert'
import { test } from 'node:test'
import type * as KafkaJS from 'kafkajs'
import type * as Compatibility from '../../../src/compatibility/kafkajs/index.ts'

type Assert<T extends true> = T
type Extends<From, To> = [From] extends [To] ? true : false
type Same<Left, Right> = Extends<Left, Right> extends true ? Extends<Right, Left> : false

type CompatibilityAssertions = [
  Assert<Same<Compatibility.KafkaConfig, KafkaJS.KafkaConfig>>,
  Assert<Same<Compatibility.ProducerConfig, KafkaJS.ProducerConfig>>,
  Assert<Same<Compatibility.ConsumerConfig, KafkaJS.ConsumerConfig>>,
  Assert<Same<Compatibility.AdminConfig, KafkaJS.AdminConfig>>,
  Assert<Same<Compatibility.Message, KafkaJS.Message>>,
  Assert<Same<Compatibility.ProducerRecord, KafkaJS.ProducerRecord>>,
  Assert<Same<Compatibility.ProducerBatch, KafkaJS.ProducerBatch>>,
  Assert<Same<Compatibility.RecordMetadata, KafkaJS.RecordMetadata>>,
  Assert<Same<Compatibility.ConsumerRunConfig, KafkaJS.ConsumerRunConfig>>,
  Assert<Same<Compatibility.EachMessagePayload, KafkaJS.EachMessagePayload>>,
  Assert<Same<Compatibility.EachBatchPayload, KafkaJS.EachBatchPayload>>,
  Assert<Same<Compatibility.Batch, KafkaJS.Batch>>,
  Assert<Same<Compatibility.Cluster, KafkaJS.Cluster>>,
  Assert<Same<Compatibility.Broker, KafkaJS.Broker>>,
  Assert<Same<Compatibility.Transaction, KafkaJS.Transaction>>,
  Assert<Same<Compatibility.RequestEvent, KafkaJS.RequestEvent>>,
  Assert<Same<Compatibility.RequestTimeoutEvent, KafkaJS.RequestTimeoutEvent>>,
  Assert<Same<Compatibility.RequestQueueSizeEvent, KafkaJS.RequestQueueSizeEvent>>,
  Assert<Same<Compatibility.KafkaJSErrorMetadata, KafkaJS.KafkaJSErrorMetadata>>,
  Assert<Same<Compatibility.KafkaJSConnectionErrorMetadata, KafkaJS.KafkaJSConnectionErrorMetadata>>,
  Assert<Same<Compatibility.KafkaJSRequestTimeoutErrorMetadata, KafkaJS.KafkaJSRequestTimeoutErrorMetadata>>,
  Assert<Extends<Compatibility.Kafka, KafkaJS.Kafka>>,
  Assert<Extends<Compatibility.Producer, KafkaJS.Producer>>,
  Assert<Extends<Compatibility.Consumer, KafkaJS.Consumer>>,
  Assert<Extends<Compatibility.Admin, KafkaJS.Admin>>
]

const compatibilityAssertions: CompatibilityAssertions = Array.from(
  { length: 25 },
  () => true
) as CompatibilityAssertions

function exerciseApis (
  kafka: Compatibility.Kafka,
  producer: Compatibility.Producer,
  consumer: Compatibility.Consumer,
  admin: Compatibility.Admin,
  cluster: Compatibility.Cluster,
  broker: Compatibility.Broker
): void {
  const kafkaJSKafka: KafkaJS.Kafka = kafka
  const kafkaJSProducer: KafkaJS.Producer = producer
  const kafkaJSConsumer: KafkaJS.Consumer = consumer
  const kafkaJSAdmin: KafkaJS.Admin = admin
  const configuredProducer: Compatibility.Producer = kafka.producer({ idempotent: true })
  const configuredConsumer: Compatibility.Consumer = kafka.consumer({ groupId: 'workers' })
  const configuredAdmin = kafka.admin({ retry: { retries: 3 } })
  assert.ok([
    configuredProducer,
    configuredConsumer,
    configuredAdmin,
    kafkaJSKafka,
    kafkaJSProducer,
    kafkaJSConsumer,
    kafkaJSAdmin
  ])

  producer.on(producer.events.REQUEST, event => {
    const duration: number = event.payload.duration
    assert.equal(typeof duration, 'number')
  })
  consumer.on(consumer.events.GROUP_JOIN, event => {
    const assignment: Compatibility.IMemberAssignment = event.payload.memberAssignment
    assert.ok(assignment)
  })
  admin.on(admin.events.REQUEST_QUEUE_SIZE, event => {
    const queueSize: number = event.payload.queueSize
    assert.equal(typeof queueSize, 'number')
  })

  producer.sendBatch({
    compression: 1,
    topicMessages: [{ topic: 'events', messages: [{ key: 'id', value: Buffer.from('payload') }] }]
  })
  producer.transaction().then(transaction =>
    transaction.sendOffsets({
      consumerGroupId: 'workers',
      topics: [{ topic: 'events', partitions: [{ partition: 0, offset: '42' }] }]
    }))
  consumer.subscribe({ topics: [/^events-/], fromBeginning: true })
  consumer.run({
    eachBatch: async ({ batch, resolveOffset, commitOffsetsIfNecessary, isStale }) => {
      if (!isStale()) {
        resolveOffset(batch.lastOffset())
        await commitOffsetsIfNecessary()
      }
    }
  })
  admin.alterPartitionReassignments({
    topics: [{ topic: 'events', partitionAssignment: [{ partition: 0, replicas: [1, 2, 3] }] }]
  })
  cluster.fetchTopicsOffset([{ topic: 'events', partitions: [{ partition: 0 }], fromTimestamp: Date.now() }])
  broker.fetch({
    maxWaitTime: 500,
    minBytes: 1,
    topics: [{ topic: 'events', partitions: [{ partition: 0, fetchOffset: '0', maxBytes: 1_048_576 }] }]
  })
  broker.produce({
    acks: -1,
    topicData: [{ topic: 'events', partitions: [{ partition: 0, messages: [{ value: 'payload' }] }] }]
  })
}

test('KafkaJS compatibility types compile', () => {
  assert.equal(typeof exerciseApis, 'function')
  assert.equal(compatibilityAssertions.every(Boolean), true)
})
