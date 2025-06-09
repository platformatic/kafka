import { readFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { Base } from '../../src/index.ts'

async function main () {
  const client = new Base({
    clientId: 'clientId',
    groupId: 'groupId',
    bootstrapBrokers: ['localhost:9098'],
    tls: {
      rejectUnauthorized: false,
      cert: await readFile(resolve(import.meta.dirname, '../../ssl/client.pem')),
      key: await readFile(resolve(import.meta.dirname, '../../ssl/client.key'))
    },
    retries: 0
  })

  console.log(await client.metadata({ topics: [] }))

  await client.close()
}

await main()
