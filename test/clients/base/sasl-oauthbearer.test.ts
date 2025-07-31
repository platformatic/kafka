import { createSigner } from 'fast-jwt'
import { deepStrictEqual, rejects } from 'node:assert'
import { test } from 'node:test'
import { Base } from '../../../src/index.ts'

test('should not connect to SASL protected broker by default', async t => {
  const base = new Base({ clientId: 'clientId', bootstrapBrokers: ['localhost:9096'], strict: true, retries: false })
  t.after(() => base.close())

  await rejects(() => base.metadata({ topics: [] }))
})

test('should connect to SASL protected broker using SASL/OAUTHBEARER', async t => {
  const signSync = createSigner({
    algorithm: 'none',
    iss: 'kafka',
    aud: ['users'],
    sub: 'admin',
    expiresIn: '2h'
  })
  const token = signSync({ scope: 'test' })

  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:9096'],
    strict: true,
    retries: 0,
    sasl: { mechanism: 'OAUTHBEARER', token }
  })

  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })

  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9092 })
})
