import { Admin } from '../../src/clients/admin/index.ts'
import { Consumer, debugDump } from '../../src/index.ts'

const consumer1 = new Consumer({
  groupId: 'id1',
  clientId: 'id',
  bootstrapBrokers: ['localhost:9092'],
  strict: true
})

const consumer2 = new Consumer({
  groupId: 'id2',
  clientId: 'id',
  bootstrapBrokers: ['localhost:9092'],
  strict: true
})

consumer1.topics.track('temp1')
await consumer1.joinGroup({ sessionTimeout: 10000, heartbeatInterval: 500, rebalanceTimeout: 15000 })
await consumer2.joinGroup({ sessionTimeout: 10000, heartbeatInterval: 500, rebalanceTimeout: 15000 })

const admin = new Admin({ clientId: 'id', bootstrapBrokers: ['localhost:9092'], strict: true })

debugDump('listGroups', await admin.listGroups())
debugDump('describeGroups', await admin.describeGroups({ groups: ['id1', 'id2'] }))
debugDump('describeGroups', await admin.describeGroups({ groups: ['id1'] }))
debugDump('describeGroups', await admin.describeGroups({ groups: ['id2'] }))

await consumer2.close()

await admin.deleteGroups({ groups: ['id2'] })

await consumer1.close()

await admin.close()
