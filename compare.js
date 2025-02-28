import { Kafka } from 'kafkajs'

const kafka = new Kafka({
  clientId: '123',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()

await producer.connect()
await producer.send({
  topic: 'temp',
  messages: [{ key: '111', value: '222', timestamp: 12345678 }]
})

await producer.disconnect()
