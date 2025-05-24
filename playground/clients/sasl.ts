import { Consumer } from '../../src/index.ts'

async function main () {
  const consumer = new Consumer({
    clientId: 'clientId',
    groupId: 'groupId',
    bootstrapBrokers: ['localhost:3012'],
    sasl: {
      mechanism: 'SCRAM-SHA-256',
      username: 'admin',
      password: 'admin'
    },
    retries: 0
  })

  console.log(await consumer.metadata({ topics: [] }))
}

await main()
