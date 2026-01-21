import { Base, debugDump } from '../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../test/helpers.ts'

const client = new Base({ clientId: 'id', bootstrapBrokers: kafkaSingleBootstrapServers, retries: 0, strict: true })
debugDump('metadata', await client.listApis())

await client.close()
