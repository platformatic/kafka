import { Base } from '../../src/index.ts'

async function main () {
  const client = new Base({
    clientId: 'clientId',
    groupId: 'groupId',
    bootstrapBrokers: ['localhost:9098'],
    tls: {
      rejectUnauthorized: false
    },
    retries: 0
  })

  console.log(await client.metadata({ topics: [] }))

  await client.close()
}

await main()
