import ConfluentRDKafka, { KafkaJS as ConfluentKafka } from '@confluentinc/kafka-javascript'
import RDKafka from '@platformatic/rdkafka'
import { printResults, Tracker, type Result } from 'cronometro'
import { Kafka as KafkaJS, logLevel } from 'kafkajs'
import { randomUUID } from 'node:crypto'
import { Consumer, MessagesStreamModes, PromiseWithResolvers } from '../src/index.ts'
import { brokers, topic } from './utils/definitions.ts'

const iterations = 100000
const maxBytes = 2048

function rdkafkaEvented (): Promise<Result> {
  const { promise, resolve, reject } = PromiseWithResolvers<Result>()
  const tracker = new Tracker()

  const consumer = new RDKafka.KafkaConsumer(
    {
      'client.id': 'benchmarks',
      'group.id': randomUUID(),
      'metadata.broker.list': brokers.join(','),
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10
    },
    { 'auto.offset.reset': 'earliest' }
  )

  let i = 0
  let last = process.hrtime.bigint()
  consumer.on('data', () => {
    i++
    tracker.track(last)
    last = process.hrtime.bigint()

    if (i === iterations) {
      consumer.removeAllListeners('data')
      consumer.pause([
        {
          topic,
          partition: 0
        },
        {
          topic,
          partition: 1
        },
        {
          topic,
          partition: 2
        }
      ])

      setTimeout(() => {
        consumer.disconnect()
        resolve(tracker.results)
      }, 100)
    }
  })

  consumer.on('ready', () => {
    consumer.subscribe([topic])
    consumer.consume()
  })

  consumer.on('event.error', reject)

  consumer.connect()

  return promise
}

function rdkafkaStream (): Promise<Result> {
  const { promise, resolve, reject } = PromiseWithResolvers<Result>()
  const tracker = new Tracker()

  const stream = RDKafka.KafkaConsumer.createReadStream(
    {
      'client.id': 'benchmarks',
      'group.id': randomUUID(),
      'metadata.broker.list': brokers.join(','),
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10
    },
    { 'auto.offset.reset': 'earliest' },
    { topics: [topic], waitInterval: 0, highWaterMark: 1024, objectMode: true }
  )

  let i = 0
  let last = process.hrtime.bigint()
  stream.on('data', () => {
    i++
    tracker.track(last)
    last = process.hrtime.bigint()

    if (i === iterations) {
      stream.removeAllListeners('data')
      stream.pause()

      stream.destroy()
      resolve(tracker.results)
    }
  })

  stream.on('error', reject)

  return promise
}

function confluentRdkafkaEvented (): Promise<Result> {
  const { promise, resolve, reject } = PromiseWithResolvers<Result>()
  const tracker = new Tracker()

  const consumer = new ConfluentRDKafka.KafkaConsumer(
    {
      'client.id': 'benchmarks',
      'group.id': randomUUID(),
      'metadata.broker.list': brokers.join(','),
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10
    },
    { 'auto.offset.reset': 'earliest' }
  )

  let i = 0
  let last = process.hrtime.bigint()
  consumer.on('data', () => {
    i++
    tracker.track(last)
    last = process.hrtime.bigint()

    if (i === iterations) {
      consumer.removeAllListeners('data')
      consumer.pause([
        {
          topic,
          partition: 0
        },
        {
          topic,
          partition: 1
        },
        {
          topic,
          partition: 2
        }
      ])

      setTimeout(() => {
        consumer.disconnect()
        resolve(tracker.results)
      }, 100)
    }
  })

  consumer.on('ready', () => {
    consumer.subscribe([topic])
    consumer.consume()
  })

  consumer.on('event.error', reject)

  consumer.connect()

  return promise
}

function confluentRdkafkaStream (): Promise<Result> {
  const { promise, resolve, reject } = PromiseWithResolvers<Result>()
  const tracker = new Tracker()

  const stream = ConfluentRDKafka.KafkaConsumer.createReadStream(
    {
      'client.id': 'benchmarks',
      'group.id': randomUUID(),
      'metadata.broker.list': brokers.join(','),
      'enable.auto.commit': false,
      'fetch.min.bytes': 1,
      'fetch.message.max.bytes': maxBytes,
      'fetch.wait.max.ms': 10
    },
    { 'auto.offset.reset': 'earliest' },
    { topics: [topic], waitInterval: 0, highWaterMark: 1024, objectMode: true }
  )

  let i = 0
  let last = process.hrtime.bigint()
  stream.on('data', () => {
    i++
    tracker.track(last)
    last = process.hrtime.bigint()

    if (i === iterations) {
      stream.removeAllListeners('data')
      stream.pause()

      stream.destroy()
      resolve(tracker.results)
    }
  })

  stream.on('error', reject)

  return promise
}

async function kafkajs (): Promise<Result> {
  const { promise, resolve, reject } = PromiseWithResolvers<Result>()
  const tracker = new Tracker()

  const client = new KafkaJS({ clientId: 'benchmarks', brokers, logLevel: logLevel.ERROR })
  const consumer = client.consumer({ groupId: randomUUID(), maxWaitTimeInMs: 10, maxBytes })

  await consumer.connect()
  await consumer.subscribe({ topics: [topic], fromBeginning: true })

  consumer.on('consumer.crash', reject)

  let i = 0
  let last = process.hrtime.bigint()
  await consumer.run({
    autoCommit: false,
    partitionsConsumedConcurrently: 1,
    async eachMessage () {
      i++
      tracker.track(last)
      last = process.hrtime.bigint()

      if (i === iterations) {
        consumer.disconnect()
        resolve(tracker.results)
      }
    }
  })

  return promise
}

async function confluentKafkaJS (): Promise<Result> {
  const { promise, resolve } = PromiseWithResolvers<Result>()
  const tracker = new Tracker()

  const client = new ConfluentKafka.Kafka({
    kafkaJS: {
      clientId: 'benchmarks',
      brokers
    }
  })
  const consumer = client.consumer({
    kafkaJS: {
      groupId: randomUUID(),
      maxWaitTimeInMs: 10,
      maxBytes,
      fromBeginning: true,
      autoCommit: false
    }
  })

  await consumer.connect()
  await consumer.subscribe({ topics: [topic] })

  let i = 0
  let last = process.hrtime.bigint()
  await consumer.run({
    partitionsConsumedConcurrently: 1,
    async eachMessage () {
      i++
      tracker.track(last)
      last = process.hrtime.bigint()

      if (i === iterations) {
        consumer.disconnect()
        resolve(tracker.results)
      }
    }
  })

  return promise
}

async function platformaticKafka (): Promise<Result> {
  const { promise, resolve, reject } = PromiseWithResolvers<Result>()
  const tracker = new Tracker()

  const consumer = new Consumer({
    clientId: 'benchmarks',
    groupId: randomUUID(),
    bootstrapBrokers: brokers,
    minBytes: 1,
    maxBytes,
    maxWaitTime: 10,
    autocommit: false
  })

  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST })

  let i = 0
  let last = process.hrtime.bigint()
  stream.on('data', () => {
    i++
    tracker.track(last)
    last = process.hrtime.bigint()

    if (i === iterations) {
      process.nextTick(() => {
        consumer.close(true, () => {
          resolve(tracker.results)
        })
      })
    }
  })

  stream.on('error', reject)

  return promise
}

const results = {
  'node-rdkafka (evented)': await rdkafkaEvented(),
  'node-rdkafka (stream)': await rdkafkaStream(),
  '@confluentinc/kafka-javascript (node-rdkafka, evented)': await confluentRdkafkaEvented(),
  '@confluentinc/kafka-javascript (node-rdkafka, stream)': await confluentRdkafkaStream(),
  KafkaJS: await kafkajs(),
  '@confluentinc/kafka-javascript (KafkaJS)': await confluentKafkaJS(),
  '@platformatic/kafka': await platformaticKafka()
}

printResults(results, true, true, 'previous')
