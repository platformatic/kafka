import RDKafka from '@platformatic/rdkafka'
import { printResults, Tracker, type Result } from 'cronometro'
import { Kafka as KafkaJS } from 'kafkajs'
import { ProduceAcks, Producer, stringSerializers } from '../src/index.ts'
import { brokers, topic } from './utils/definitions.ts'

const iterations = 10000
const batchSize = Math.max(iterations / 100, 1)

function rdkafka (): Promise<Result> {
  const { promise, resolve, reject } = Promise.withResolvers<Result>()
  const tracker = new Tracker()

  const producer = new RDKafka.Producer(
    {
      'client.id': 'benchmarks',
      'metadata.broker.list': brokers.join(','),
      'queue.buffering.max.messages': iterations,
      dr_cb: true
    },
    {
      acks: 0
    }
  )

  producer.on('event.error', reject)

  let i = 0
  let last = process.hrtime.bigint()
  producer.on('delivery-report', () => {
    i++

    if (i % batchSize === 0) {
      tracker.track(last)
      last = process.hrtime.bigint()
    }

    if (i === iterations) {
      producer.disconnect(error => {
        if (error) {
          reject(error)
          return
        }

        resolve(tracker.results)
      })
    }
  })

  producer.connect({}, () => {
    producer.setPollInterval(1)

    for (let j = 0; j < iterations; j++) {
      producer.produce(topic, 0, Buffer.from('222'), '111-' + j, 0, null, [{ a: '123', b: '456' }])
    }
  })

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

  let last = process.hrtime.bigint()
  for (let i = 0; i < iterations; i++) {
    await producer.send({
      topic,
      messages: [{ key: '111-' + i, value: '222', partition: 0, headers: { a: '123', b: '456' } }],
      acks: 0
    })

    if (i % batchSize === 0) {
      tracker.track(last)
      last = process.hrtime.bigint()
    }
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
  for (let i = 0; i < iterations; i++) {
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

    if (i % batchSize === 0) {
      tracker.track(last)
      last = process.hrtime.bigint()
    }
  }

  await producer.close()
  return tracker.results
}

const results = {
  'node-rdkafka': await rdkafka(),
  kafkajs: await kafkajs(),
  '@platformatic/kafka': await platformaticKafka()
}

printResults(results, true, true, 'previous')
