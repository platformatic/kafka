import ConfluentRDKafka, { KafkaJS as ConfluentKafka } from '@confluentinc/kafka-javascript'
import RDKafka from '@platformatic/rdkafka'
import { printResults, Tracker, type Result } from 'cronometro'
import { Kafka as KafkaJS } from 'kafkajs'
import { once } from 'node:events'
import { ProduceAcks, Producer, PromiseWithResolvers, stringSerializers } from '../src/index.ts'
import { brokers, topic } from './utils/definitions.ts'

const iterations = 100000

async function rdkafka (): Promise<Result> {
  const { promise, resolve, reject } = PromiseWithResolvers<Result>()
  const tracker = new Tracker()

  const producer = new RDKafka.Producer(
    {
      'client.id': 'benchmarks',
      'metadata.broker.list': brokers.join(','),
      'linger.ms': 0,
      'max.in.flight': 1,
      dr_cb: true
    },
    {
      acks: 0
    }
  )
  producer.connect()
  producer.setPollInterval(1)
  producer.on('event.error', reject)
  await once(producer, 'ready')

  for (let i = 0; i < iterations; i++) {
    const start = process.hrtime.bigint()

    producer.produce(topic, 0, Buffer.from('222'), '111-' + i, 0, null, [{ a: '123', b: '456' }])
    producer.flush()
    await once(producer, 'delivery-report')

    tracker.track(start)
  }

  resolve(tracker.results)
  producer.disconnect()
  await once(producer, 'disconnected')
  return promise
}

async function confluentRdKafka (): Promise<Result> {
  const { promise, resolve, reject } = PromiseWithResolvers<Result>()
  const tracker = new Tracker()

  const producer = new ConfluentRDKafka.Producer(
    {
      'client.id': 'benchmarks',
      'metadata.broker.list': brokers.join(','),
      'linger.ms': 0,
      'max.in.flight': 1,
      dr_cb: true
    },
    {
      acks: 0
    }
  )
  producer.connect()
  producer.setPollInterval(1)
  producer.on('event.error', reject)
  await once(producer, 'ready')

  for (let i = 0; i < iterations; i++) {
    const start = process.hrtime.bigint()

    producer.produce(topic, 0, Buffer.from('222'), '111-' + i, 0, null, [{ a: '123', b: '456' }])
    producer.flush()
    await once(producer, 'delivery-report')

    tracker.track(start)
  }

  resolve(tracker.results)
  producer.disconnect()
  await once(producer, 'disconnected')
  return promise
}

async function kafkajs (): Promise<Result> {
  const tracker = new Tracker()

  const client = new KafkaJS({
    clientId: 'benchmarks',
    brokers
  })
  const producer = client.producer()
  await producer.connect()

  for (let i = 0; i < iterations; i++) {
    const start = process.hrtime.bigint()

    await producer.send({
      topic,
      messages: [{ key: '111-' + i, value: '222', partition: 0, headers: { a: '123', b: '456' } }],
      acks: 0
    })

    tracker.track(start)
  }

  await producer.disconnect()
  return tracker.results
}

async function confluentKafkaJS (): Promise<Result> {
  const tracker = new Tracker()

  const client = new ConfluentKafka.Kafka()
  const producer = client.producer({
    'client.id': 'benchmarks',
    'metadata.broker.list': brokers.join(','),
    'linger.ms': 0,
    'max.in.flight': 1,
    acks: 0
  })
  await producer.connect()

  for (let i = 0; i < iterations; i++) {
    const start = process.hrtime.bigint()

    await producer.send({
      topic,
      messages: [
        {
          partition: 0,
          key: '111-' + i,
          value: '222',
          headers: { a: '123', b: '456' }
        }
      ]
    })

    tracker.track(start)
  }

  await producer.flush()
  await producer.disconnect()
  return tracker.results
}

async function platformaticKafka (): Promise<Result> {
  const tracker = new Tracker()

  const producer = new Producer({
    clientId: 'benchmarks',
    bootstrapBrokers: brokers,
    serializers: stringSerializers
  })

  for (let i = 0; i < iterations; i++) {
    const start = process.hrtime.bigint()

    await producer.send({
      messages: [
        {
          topic,
          partition: 0,
          key: '111-' + i,
          value: '222',
          headers: { a: '123', b: '456' }
        }
      ],
      acks: ProduceAcks.NO_RESPONSE
    })

    tracker.track(start)
  }

  await producer.close()
  return tracker.results
}

const results = {
  'node-rdkafka': await rdkafka(),
  '@confluentinc/kafka-javascript (node-rdkafka)': await confluentRdKafka(),
  KafkaJS: await kafkajs(),
  '@confluentinc/kafka-javascript (KafkaJS)': await confluentKafkaJS(),
  '@platformatic/kafka': await platformaticKafka()
}

printResults(results, true, true, 'previous')
