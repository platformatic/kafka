import { printResults, Tracker, type Result } from 'cronometro'
import { Kafka as KafkaJS } from 'kafkajs'
import { ProduceAcks, Producer, stringSerializers } from '../src/index.ts'
import { brokers, topic } from './utils/definitions.ts'

const iterations = 100_000
const batchSize = 100
const batches = Math.ceil(iterations / batchSize)

async function kafkajs (): Promise<Result> {
  const tracker = new Tracker()
  const client = new KafkaJS({ clientId: 'benchmarks', brokers })
  const producer = client.producer({ maxInFlightRequests: 5 })
  await producer.connect()

  let last = process.hrtime.bigint()

  for (let b = 0; b < batches; b++) {
    const start = b * batchSize
    const end = Math.min(start + batchSize, iterations)

    const messages = new Array(end - start).fill(null).map((_, idx) => {
      const i = start + idx
      return { key: '111-' + i, value: '222', partition: 0, headers: { a: '123', b: '456' } }
    })

    await producer.send({ topic, messages, acks: 0 })
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

  for (let b = 0; b < batches; b++) {
    const start = b * batchSize
    const end = Math.min(start + batchSize, iterations)

    const batch = new Array(end - start).fill(null).map((_, idx) => {
      const i = start + idx
      return {
        topic,
        partition: 0,
        key: '111-' + i,
        value: '222',
        headers: { a: '123', b: '456' }
      }
    })

    await producer.send({ messages: batch, acks: ProduceAcks.NO_RESPONSE })
    tracker.track(last)
    last = process.hrtime.bigint()
  }

  await producer.close()
  return tracker.results
}

const results = {
  'kafkajs (batched)': await kafkajs(),
  '@platformatic/kafka (batched)': await platformaticKafka()
}

printResults(results, true, true, 'previous')
