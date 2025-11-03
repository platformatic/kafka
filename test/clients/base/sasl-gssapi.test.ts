import { deepStrictEqual, rejects } from 'node:assert'
import { resolve } from 'node:path'
import { test } from 'node:test'
import { Base, debugDump } from '../../../src/index.ts'

test.skip('should not connect to SASL protected broker by default', async t => {
  const base = new Base({ clientId: 'clientId', bootstrapBrokers: ['localhost:9003'], strict: true, retries: false })
  t.after(() => base.close())

  await rejects(() => base.metadata({ topics: [] }))
})

test('should connect to SASL protected broker using SASL/GSSAPI using username and password', async t => {
  process.env.KRB5_CONFIG = resolve(import.meta.dirname, '../../fixtures/krb5.conf')

  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:9003'],
    strict: true,
    retries: 0,
    sasl: { mechanism: 'GSSAPI', username: 'admin-password@EXAMPLE.COM', password: 'admin' }
  })

  t.after(() => base.close())

  try {
    const metadata = await base.metadata({ topics: [] })
    deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9003 })
    debugDump(metadata)
  } catch (e) {
    debugDump(e)
    throw e
  }
})
