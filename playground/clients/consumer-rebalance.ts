import { once } from 'node:events'
import { Consumer, debugDump, stringDeserializers } from '../../src/index.ts'

const consumer1 = new Consumer({
  groupId: 'id9',
  clientId: 'id',
  bootstrapBrokers: ['localhost:29092'],
  strict: true,
  deserializers: stringDeserializers
})

const consumer2 = new Consumer({
  groupId: 'id9',
  clientId: 'id',
  bootstrapBrokers: ['localhost:29092'],
  strict: true,
  deserializers: stringDeserializers
})

consumer1.topics.trackAll('temp1')
await consumer1.joinGroup({ sessionTimeout: 10000, heartbeatInterval: 500, rebalanceTimeout: 2000 })
debugDump({ id: 1, memberId: consumer1.memberId, assignments: consumer1.assignments })

consumer1.topics.trackAll('temp2')
await consumer1.joinGroup({ sessionTimeout: 10000, heartbeatInterval: 500, rebalanceTimeout: 2000 })
debugDump({ id: 1, memberId: consumer1.memberId, assignments: consumer1.assignments })

consumer2.topics.trackAll('temp1', 'temp2')
await consumer2.joinGroup({ sessionTimeout: 10000, heartbeatInterval: 500, rebalanceTimeout: 2000 })
debugDump({ id: 1, memberId: consumer1.memberId, assignments: consumer1.assignments })
debugDump({ id: 2, memberId: consumer2.memberId, assignments: consumer2.assignments })

await consumer1.close()

await once(consumer2, 'consumer:group:join')
debugDump({ id: 2, memberId: consumer2.memberId, assignments: consumer2.assignments })

await consumer2.close()
