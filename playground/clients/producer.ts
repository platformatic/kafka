import { ProduceAcks, Producer, debugDump, stringSerializers } from '../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../test/helpers.ts'

const producer = new Producer({
  clientId: 'id',
  bootstrapBrokers: kafkaSingleBootstrapServers,
  serializers: stringSerializers,
  strict: true
})

debugDump(
  'produce',
  await Promise.all([
    producer.send({
      messages: [
        {
          topic: 'temp1',
          partition: 0,
          key: 'key1',
          value: 'value1',
          headers: new Map([['headerKey1', 'headerValue1']])
        },
        { topic: 'temp1', partition: 0, key: 'key2', value: 'value2' },
        {
          topic: 'temp1',
          partition: 0,
          key: 'key3',
          value: 'value3',
          headers: new Map([['headerKey3', 'headerValue3']])
        },
        { topic: 'temp1', partition: 0, key: 'key4', value: 'value4' },
        {
          topic: 'temp1',
          partition: 0,
          key: 'key5',
          value: 'value5',
          headers: { headerKey5: 'headerValue5' }
        },
        { topic: 'temp1', partition: 0, key: 'key6', value: 'value6' }
      ],
      acks: ProduceAcks.LEADER
    }),

    producer.send({
      messages: [
        {
          topic: 'temp1',
          partition: 0,
          key: 'key1',
          value: 'value1',
          headers: new Map([['headerKey1', 'headerValue1']])
        },
        { topic: 'temp1', partition: 0, key: 'key2', value: 'value2' },
        {
          topic: 'temp1',
          partition: 0,
          key: 'key3',
          value: 'value3',
          headers: new Map([['headerKey3', 'headerValue3']])
        },
        { topic: 'temp1', partition: 0, key: 'key4', value: 'value4' },
        {
          topic: 'temp1',
          partition: 0,
          key: 'key5',
          value: 'value5',
          headers: { headerKey5: 'headerValue5' }
        },
        { topic: 'temp1', partition: 0, key: 'key6', value: 'value6' }
      ],
      acks: ProduceAcks.LEADER
    }),

    producer.send({
      messages: [
        {
          topic: 'temp1',
          partition: 0,
          key: 'key1',
          value: 'value1',
          headers: new Map([['headerKey1', 'headerValue1']])
        },
        { topic: 'temp1', partition: 0, key: 'key2', value: 'value2' },
        {
          topic: 'temp1',
          partition: 0,
          key: 'key3',
          value: 'value3',
          headers: new Map([['headerKey3', 'headerValue3']])
        },
        { topic: 'temp1', partition: 0, key: 'key4', value: 'value4' },
        {
          topic: 'temp1',
          partition: 0,
          key: 'key5',
          value: 'value5',
          headers: { headerKey5: 'headerValue5' }
        },
        { topic: 'temp1', partition: 0, key: 'key6', value: 'value6' }
      ],
      acks: ProduceAcks.LEADER
    })
  ])
)

await producer.close()
