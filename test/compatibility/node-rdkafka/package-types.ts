import {
  AdminClient,
  Client,
  CODES,
  HighLevelProducer,
  KafkaConsumer,
  Producer,
  type ConsumerGlobalConfig,
  type ProducerGlobalConfig
} from '@platformatic/kafka/compatibility/node-rdkafka'

const producerConfig: ProducerGlobalConfig = {
  'bootstrap.servers': 'localhost:9092',
  'statistics.interval.ms': 1000,
  'batch.size': 16384,
  'linger.ms': 5
}
const consumerConfig: ConsumerGlobalConfig = {
  'bootstrap.servers': 'localhost:9092',
  'group.id': 'workers',
  'queued.max.messages.kbytes': 1024,
  'client.rack': 'rack-a'
}

const producer = new Producer(producerConfig)
const highLevelProducer = new HighLevelProducer(producerConfig)
const consumer = new KafkaConsumer(consumerConfig, { 'auto.offset.reset': 'earliest' })
const admin = AdminClient.create(producerConfig)

producer.on('delivery-report', (_error, report) => report.offset.toFixed())
consumer.on('ready', (_info, metadata) => metadata.topics.map(topic => topic.name))
consumer.consume(1, (_error, messages) => messages.map(message => message.offset))
highLevelProducer.produce('events', null, 'value', 'key', null, () => {})
admin.createTopic({ topic: 'events', num_partitions: 1, replication_factor: 1 }, () => {})

export { Client, CODES }
