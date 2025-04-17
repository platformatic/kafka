import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { Consumer, debugDump, stringDeserializers } from '../../src/index.ts'
import { brokers, topic } from './definitions.ts'

const consumer = new Consumer({
  groupId: randomUUID(),
  clientId: randomUUID(),
  bootstrapBrokers: brokers,
  strict: true,
  deserializers: stringDeserializers
})

const stream = await consumer.consume({
  autocommit: false,
  topics: [topic],
  sessionTimeout: 10000,
  heartbeatInterval: 500,
  maxWaitTime: 500
})

once(process, 'SIGINT').then(() => consumer.close(true))

debugDump('--- Consuming messages ---')

for await (const message of stream) {
  debugDump('messages', message)
}

debugDump('--- Consuming stopped ---')

await consumer.close()
