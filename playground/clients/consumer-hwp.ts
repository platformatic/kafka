import { once } from 'node:events'
import { Consumer, debugDump, stringDeserializers } from '../../src/index.ts'
import { forEach } from 'hwp'
import { setTimeout as sleep } from 'node:timers/promises'

// const consumer = new Consumer({ groupId: 'id7', clientId: 'id', bootstrapBrokers: ['localhost:9092'], strict: true })
const consumer = new Consumer({
  groupId: 'id9',
  clientId: 'id',
  bootstrapBrokers: ['localhost:29092'],
  strict: true,
  deserializers: stringDeserializers,
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

await forEach(stream, async (message, { signal }) => {
  console.log('data', message.partition, message.offset, message.key, message.value, message.headers)
  await sleep(1000)
  console.log('done', message.partition, message.offset, message.key, message.value, message.headers)
}, 16)

debugDump('end')
await consumer.close()
