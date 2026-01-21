import { Base } from '../../src/index.ts'
import { kafkaSaslBootstrapServers } from '../../test/helpers.ts'

async function main () {
  const client = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslBootstrapServers,
    sasl: {
      mechanism: 'SCRAM-SHA-256',
      username: 'admin',
      password: 'admin'
    },
    retries: 0
  })

  console.log(await client.metadata({ topics: [] }))

  await client.close()
}

await main()
