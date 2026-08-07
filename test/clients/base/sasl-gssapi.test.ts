import { deepStrictEqual, ok, rejects } from 'node:assert'
import { test } from 'node:test'
import { Base, MultipleErrors, NetworkError, UserError } from '../../../src/index.ts'
import { createAuthenticator, type KerberosCredentials } from '../../fixtures/kerberos-authenticator.ts'

const kerberosCredentials: KerberosCredentials = { username: 'admin-password@EXAMPLE.COM', password: 'admin' }
const kerberosConnectTimeout = 30000

test('should not connect to SASL protected broker by default', async t => {
  const base = new Base({ clientId: 'clientId', bootstrapBrokers: ['localhost:9003'], strict: true, retries: false })
  t.after(() => base.close())

  await rejects(() => base.metadata({ topics: [] }))
})

test('should fail connecttion to SASL protected broker using SASL/GSSAPI when no custom authenticator is provided', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:9003'],
    strict: true,
    retries: 0,
    sasl: { mechanism: 'GSSAPI', username: 'admin-password@EXAMPLE.COM', password: 'admin' }
  })

  t.after(() => base.close())

  try {
    await base.metadata({ topics: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    ok(error instanceof MultipleErrors)
    ok(error.errors[0] instanceof NetworkError)
    ok(error.errors[0].cause instanceof UserError)
    deepStrictEqual(error.errors[0].cause.message, 'No custom SASL/GSSAPI authenticator provided.')
  }
})

test('should connect to SASL protected broker using SASL/GSSAPI using a custom authenticator', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:9003'],
    strict: true,
    retries: 0,
    /*
      The GSSAPI handshake talks to the KDC before the connection is considered ready, so it is
      bounded by connectTimeout. The default 5s is not enough on a loaded CI machine.
    */
    connectTimeout: kerberosConnectTimeout,
    sasl: {
      mechanism: 'GSSAPI',
      username: kerberosCredentials.username,
      password: kerberosCredentials.password,
      authenticate: await createAuthenticator(
        'broker@broker-sasl-kerberos',
        'EXAMPLE.COM',
        'localhost:8000',
        kerberosCredentials
      )
    }
  })

  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })
  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9003, rack: null })
})
