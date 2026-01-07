import ConfluentRDKafka, { KafkaJS as ConfluentKafka } from '@confluentinc/kafka-javascript'
import RDKafka from '@platformatic/rdkafka'
import { printResults, Tracker, type Result } from 'cronometro'
import { Kafka as KafkaJS } from 'kafkajs'
import { once } from 'node:events'
import { ProduceAcks, Producer, promiseWithResolvers, stringSerializers } from '../src/index.ts'
import { brokers, topic } from './utils/definitions.ts'

const batchSize = 100
const iterations = batchSize * 1000
const batches = Math.ceil(iterations / batchSize)

async function rdkafka (): Promise<Result> {
  const { promise, resolve, reject } = promiseWithResolvers<Result>()
  const tracker = new Tracker()

  const producer = new RDKafka.Producer(
    {
      'client.id': 'benchmarks',
      'metadata.broker.list': brokers.join(','),
      'linger.ms': batchSize,
      'max.in.flight': batchSize,
      dr_cb: true
    },
    {
      acks: 0
    }
  )

  const pendingDeliveryReports: Record<string, () => void> = {}

  producer.connect()
  producer.setPollInterval(1)
  producer.on('event.error', reject)
  producer.on('delivery-report', (err, report) => {
    if (err) {
      reject(err)
      return
    }

    const key = report.key!.toString()

    if (!pendingDeliveryReports[key]) {
      reject(new Error('Unknown delivery report key ' + key))
      return
    }

    pendingDeliveryReports[key]()
    delete pendingDeliveryReports[key]
  })

  await once(producer, 'ready')

  let last = process.hrtime.bigint()

  for (let i = 0; i < batches; i++) {
    const promises = []

    for (let j = 0; j < batchSize; j++) {
      const index = i * batchSize + j
      const key = '111-' + index

      producer.produce(topic, 0, Buffer.from('222'), key, 0, null, [{ a: '123', b: '456' }])

      const { promise, resolve } = promiseWithResolvers<void>()

      pendingDeliveryReports[key] = resolve
      promises.push(promise)
    }

    producer.flush()
    await Promise.all(promises)
    tracker.track(last)
    last = process.hrtime.bigint()
  }

  resolve(tracker.results)
  producer.disconnect()
  await once(producer, 'disconnected')
  return promise
}

async function confluentRdKafka (): Promise<Result> {
  const { promise, resolve, reject } = promiseWithResolvers<Result>()
  const tracker = new Tracker()

  const producer = new ConfluentRDKafka.Producer(
    {
      'client.id': 'benchmarks',
      'metadata.broker.list': brokers.join(','),
      'linger.ms': batchSize,
      'max.in.flight': batchSize,
      dr_cb: true
    },
    {
      acks: 0
    }
  )

  const pendingDeliveryReports: Record<string, () => void> = {}

  producer.connect()
  producer.setPollInterval(1)
  producer.on('event.error', reject)
  producer.on('delivery-report', (err, report) => {
    if (err) {
      reject(err)
      return
    }

    const key = report.key!.toString()

    if (!pendingDeliveryReports[key]) {
      reject(new Error('Unknown delivery report key ' + key))
      return
    }

    pendingDeliveryReports[key]()
    delete pendingDeliveryReports[key]
  })

  await once(producer, 'ready')

  let last = process.hrtime.bigint()
  for (let i = 0; i < batches; i++) {
    const promises = []

    for (let j = 0; j < batchSize; j++) {
      const index = i * batchSize + j
      const key = '111-' + index

      producer.produce(topic, 0, Buffer.from('222'), key, 0, null, [{ a: '123', b: '456' }])

      const { promise, resolve } = promiseWithResolvers<void>()

      pendingDeliveryReports[key] = resolve
      promises.push(promise)
    }

    producer.flush()
    await Promise.all(promises)
    tracker.track(last)
    last = process.hrtime.bigint()
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
  const producer = client.producer({ maxInFlightRequests: batchSize })
  await producer.connect()

  let last = process.hrtime.bigint()
  for (let i = 0; i < batches; i++) {
    const batch = []

    for (let j = 0; j < batchSize; j++) {
      const index = i * batchSize + j

      batch.push({ topic, partition: 0, key: '111-' + index, value: '222', headers: { a: '123', b: '456' } })
    }

    await producer.send({ topic, messages: batch, acks: 0 })
    tracker.track(last)
    last = process.hrtime.bigint()
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
    'max.in.flight': batchSize,
    acks: 0
  })
  await producer.connect()

  let last = process.hrtime.bigint()
  for (let i = 0; i < batches; i++) {
    const batch = []

    for (let j = 0; j < batchSize; j++) {
      const index = i * batchSize + j

      batch.push({ topic, partition: 0, key: '111-' + index, value: '222', headers: { a: '123', b: '456' } })
    }

    await producer.send({ topic, messages: batch })
    tracker.track(last)
    last = process.hrtime.bigint()
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

  let last = process.hrtime.bigint()
  for (let i = 0; i < batches; i++) {
    const batch = []

    for (let j = 0; j < batchSize; j++) {
      const index = i * batchSize + j

      batch.push({ topic, partition: 0, key: '111-' + index, value: '222', headers: { a: '123', b: '456' } })
    }

    await producer.send({ messages: batch, acks: ProduceAcks.NO_RESPONSE })
    tracker.track(last)
    last = process.hrtime.bigint()
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
