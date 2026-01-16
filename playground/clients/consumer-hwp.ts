import { forEach } from 'hwp'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { setTimeout as sleep } from 'node:timers/promises'
import { Consumer, debugDump, stringDeserializers } from '../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../test/helpers.ts'

const consumer = new Consumer({
  groupId: randomUUID(),
  clientId: 'id',
  bootstrapBrokers: kafkaSingleBootstrapServers,
  strict: true,
  deserializers: stringDeserializers
})

const stream = await consumer.consume({
  autocommit: false,
  topics: ['temp1', 'temp2'],
  sessionTimeout: 10000,
  heartbeatInterval: 500,
  maxWaitTime: 500,
  mode: 'earliest'
})

// This is purposely not catched to show the error handling if we remove the force parameter
once(process, 'SIGINT').then(() => consumer.close(true))

debugDump('start')

await forEach(
  stream,
  async message => {
    console.log('data', message.partition, message.offset, message.key, message.value, message.headers)
    await sleep(1000)
    console.log('done', message.partition, message.offset, message.key, message.value, message.headers)
  },
  16
)

debugDump('end')
await consumer.close()
