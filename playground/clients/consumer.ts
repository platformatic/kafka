import { once } from 'node:events'
import { Consumer, debugDump, MessagesStreamModes, stringDeserializers } from '../../src/index.ts'

// const consumer = new Consumer({ groupId: 'id7', clientId: 'id', bootstrapBrokers: ['localhost:9092'], strict: true })
const consumer = new Consumer({
  groupId: 'id9',
  clientId: 'id',
  bootstrapBrokers: ['localhost:29092'],
  strict: true,
  deserializers: stringDeserializers
})

const stream = await consumer.consume({
  autocommit: false,
  topics: ['temp1', 'temp2'],
  sessionTimeout: 10000,
  heartbeatInterval: 500,
  maxWaitTime: 500,
  mode: MessagesStreamModes.EARLIEST
})

// This is purposely not catched to show the error handling if we remove the force parameter
once(process, 'SIGINT').then(() => consumer.close(true))

debugDump('start')

stream.on('data', message => {
  console.log('data', message.partition, message.offset, message.key, message.value, message.headers)
})

for await (const message of stream) {
  console.log('data', message.partition, message.offset, message.key, message.value, message.headers)
}

debugDump('end')
await consumer.close()
