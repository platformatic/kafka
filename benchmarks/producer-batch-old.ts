import { printResults, Tracker, type Result } from 'cronometro'
import { Kafka as KafkaJS } from 'kafkajs'
import { ProduceAcks, Producer, stringSerializers } from '../src/index.ts'
import { brokers, topic } from './utils/definitions.ts'

const iterations = 1000_000
const batchSize = 100
const batches = Math.ceil(iterations / batchSize)

async function kafkajs (): Promise<Result> {
  const tracker = new Tracker()
  const client = new KafkaJS({ clientId: 'benchmarks', brokers })
  const producer = client.producer({ maxInFlightRequests: 5 })
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
  kafkajs: await kafkajs(),
  '@platformatic/kafka': await platformaticKafka()
}

printResults(results, true, true, 'previous')
