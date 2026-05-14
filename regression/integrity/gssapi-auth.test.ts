import { deepStrictEqual } from 'node:assert'
import { test } from 'node:test'
import { Base, SASLMechanisms } from '../../src/index.ts'
import { createAuthenticator } from '../../test/fixtures/kerberos-authenticator.ts'
import { regressionSaslKerberosBootstrapServers } from '../helpers/index.ts'

test('regression: GSSAPI auth lane connects with a custom Kerberos authenticator', { timeout: 30_000 }, async t => {
  // Keep GSSAPI in the dedicated regression folder because it exercises a very
  // different auth path from PLAIN/SCRAM/OAUTHBEARER and has historically hung.
  const authenticate = await createAuthenticator('broker@broker-sasl-kerberos', 'EXAMPLE.COM', 'localhost:8000')
  const base = new Base({
    clientId: 'regression-gssapi-auth',
    bootstrapBrokers: regressionSaslKerberosBootstrapServers,
    strict: true,
    retries: 0,
    sasl: {
      mechanism: SASLMechanisms.GSSAPI,
      username: 'admin-password@EXAMPLE.COM',
      password: 'admin',
      authenticate
    }
  })
  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })
  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9003, rack: null })
})
