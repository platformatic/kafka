import { Base } from '../../src/index.ts'
import { kafkaSingleBootstrapServers } from '../../test/helpers.ts'

async function main () {
  const client = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSingleBootstrapServers,
    tls: {
      rejectUnauthorized: false
    },
    retries: 0
  })

  console.log(await client.metadata({ topics: [] }))

  await client.close()
}

await main()
