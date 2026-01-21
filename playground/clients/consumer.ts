import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { Consumer, debugDump, FetchIsolationLevels, stringDeserializers } from '../../src/index.ts'
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
  topics: ['temp1'],
  sessionTimeout: 10000,
  rebalanceTimeout: 10000,
  heartbeatInterval: 500,
  maxWaitTime: 500,
  isolationLevel: FetchIsolationLevels.READ_COMMITTED
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
