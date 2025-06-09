import { Base, debugDump } from '../../src/index.ts'

const client = new Base({ clientId: 'id', bootstrapBrokers: ['localhost:19092'], retries: 0, strict: true })
debugDump('metadata', await client.listApis())

await client.close()
