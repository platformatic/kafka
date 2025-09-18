import { deepStrictEqual, rejects } from 'node:assert'
import { resolve } from 'node:path'
import { test } from 'node:test'
import { Base, debugDump } from '../../../src/index.ts'

test.skip('should not connect to SASL protected broker by default', async t => {
  const base = new Base({ clientId: 'clientId', bootstrapBrokers: ['localhost:9097'], strict: true, retries: false })
  t.after(() => base.close())

  await rejects(() => base.metadata({ topics: [] }))
})

test('should connect to SASL protected broker using SASL/GSSAPI using username and password', async t => {
  process.env.KRB5_CONFIG = resolve(import.meta.dirname, '../../fixtures/krb5.conf')

  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:9097'],
    strict: true,
    retries: 0,
    sasl: { mechanism: 'GSSAPI', username: 'admin-keytab@EXAMPLE.COM' }
  })

  t.after(() => base.close())

  try {
    const metadata = await base.metadata({ topics: [] })
  } catch (e) {
    debugDump(e)
    throw e
  }

  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9097 })
})
