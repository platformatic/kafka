import { Admin } from '../../src/clients/admin/index.ts'
import { debugDump, sleep } from '../../src/index.ts'
import { brokers, topic } from './definitions.ts'

const admin = new Admin({ clientId: 'id', bootstrapBrokers: brokers, strict: true })

try {
  await admin.deleteTopics({ topics: [topic] })
} catch (e) {
  // Noop
}

await admin.createTopics({ topics: [topic], partitions: brokers.length, replicas: 1 })
await sleep(1000)
debugDump('topic', await admin.metadata({ topics: [topic] }))

await admin.close()
