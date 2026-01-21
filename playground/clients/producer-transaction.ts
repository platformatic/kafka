import { randomUUID } from 'crypto'
import { once } from 'node:events'
import {
  Consumer,
  debugDump,
  FetchIsolationLevels,
  Producer,
  stringDeserializers,
  stringSerializers
} from '../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../test/helpers.ts'

async function consume () {
  const consumer = new Consumer({
    groupId: randomUUID(),
    clientId: 'id',
    bootstrapBrokers: kafkaSingleBootstrapServers,
    strict: true,
    deserializers: stringDeserializers
  })

  const stream = await consumer.consume({
    autocommit: false,
    topics: ['temp1'],
    sessionTimeout: 10000,
    rebalanceTimeout: 10000,
    heartbeatInterval: 500,
    maxWaitTime: 500,
    isolationLevel: FetchIsolationLevels.READ_COMMITTED
  })

  once(process, 'SIGINT').then(() => consumer.close(true))

  stream.on('data', message => {
    console.log('data', message.partition, message.offset, message.key, message.value, message.headers)
  })

  debugDump('consumer started')
}

async function produce () {
  const transactionalId = randomUUID()

  const producer = new Producer({
    clientId: 'id',
    bootstrapBrokers: kafkaSingleBootstrapServers,
    idempotent: true,
    transactionalId,
    autocreateTopics: true,
    serializers: stringSerializers,
    strict: true
  })

  const t1 = await producer.beginTransaction()

  debugDump('transaction started')
  for (let i = 0; i < 3; i++) {
    await t1.send({
      messages: [
        {
          topic: 'temp1',
          key: `key-${i}`,
          value: `commit-${i}`,
          headers: new Map([[`header-key-${i}`, `header-value-${i}`]]),
          partition: i % 2
        }
      ]
    })

    debugDump('send')
  }

  await t1.commit()
  debugDump('transaction committed')

  const t2 = await producer.beginTransaction()
  debugDump('transaction started')

  for (let i = 0; i < 3; i++) {
    await t2.send({
      messages: [
        {
          topic: 'temp1',
          key: `key-${i}`,
          value: `abort-${i}`,
          headers: new Map([[`header-key-${i}`, `header-value-${i}`]]),
          partition: i % 2
        }
      ]
    })

    debugDump('send')
  }

  await t2.abort()
  debugDump('transaction aborted')
  await producer.close()
}

await consume()
await produce()
