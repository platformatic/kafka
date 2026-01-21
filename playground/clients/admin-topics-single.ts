import { Admin } from '../../src/clients/admin/index.ts'
import { debugDump, sleep } from '../../src/index.ts'

const retries = 0
const admin = new Admin({ clientId: 'id', bootstrapBrokers: ['localhost:9092'], retries, strict: true })
const metadataDelay = retries === 0 ? 500 : 0

try {
  await admin.deleteTopics({ topics: ['temp1', 'temp2'] })
} catch (e) {
  // Noop
}

await admin.createTopics({ topics: ['temp1', 'temp2'], partitions: 3, replicas: 1 })
await sleep(metadataDelay)
debugDump('metadata', await admin.metadata({ topics: ['temp1', 'temp2'] }))

await admin.close()
