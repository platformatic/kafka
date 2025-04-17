import { randomUUID } from 'node:crypto'
import { ProduceAcks, Producer, stringSerializers } from '../../src/index.ts'
import { brokers, topic } from './definitions.ts'

const producer = new Producer({
  clientId: randomUUID(),
  bootstrapBrokers: brokers,
  serializers: stringSerializers,
  strict: true
})

const max = 1e6

for (let i = 0; i < max; i++) {
  await producer.send({
    messages: [
      {
        topic,
        key: `key-${i}`,
        value: `value-${i}`,
        headers: new Map([[`header-key-${i}`, `header-value-${i}`]]),
        partition: i % brokers.length
      }
    ],
    acks: ProduceAcks.NO_RESPONSE
  })

  if (i % 1000 === 0) {
    console.log(`Produced ${i}/${max} messages.`)
  }
}

await producer.close()
