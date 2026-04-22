import { createSigner } from 'fast-jwt'
import { deepStrictEqual, strictEqual } from 'node:assert'
import { once } from 'node:events'
import { before, test } from 'node:test'
import { Base, parseBroker, SASLMechanisms, sleep } from '../../src/index.ts'
import {
  createScramUsers,
  regressionSaslBootstrapServers
} from '../helpers/index.ts'

const saslBroker = parseBroker(regressionSaslBootstrapServers[0]!)

// Provision SCRAM credentials once so the auth regression lane covers both token
// based reauthentication and username/password SASL mechanisms.
before(() => createScramUsers())

test('regression #226: OAUTHBEARER async token refresh does not deadlock reauthentication', { timeout: 20_000 }, async t => {
  let refreshes = 0
  const base = new Base({
    clientId: 'regression-oauth-reauth',
    bootstrapBrokers: regressionSaslBootstrapServers,
    strict: true,
    retries: 0,
    sasl: {
      mechanism: SASLMechanisms.OAUTHBEARER,
      async token () {
        refreshes++
        await sleep(50)
        return createSigner({ algorithm: 'none', iss: 'kafka', aud: ['users'], sub: 'admin', expiresIn: '3s' })({
          scope: 'test'
        })
      }
    }
  })
  t.after(() => base.close())

  // The first metadata request authenticates the connection. The short-lived JWT
  // then forces the broker-driven reauthentication path to call the async provider.
  await base.metadata({ topics: [] })
  const reauthenticated = once(base, 'client:broker:sasl:authentication:extended')
  await sleep(6000)
  await reauthenticated
  const metadata = await base.metadata({ topics: [], forceUpdate: true })

  // At least two token calls proves the refresh path completed and the connection
  // stayed usable for follow-up requests.
  strictEqual(refreshes >= 2, true)
  deepStrictEqual(metadata.brokers.get(1), { ...saslBroker, rack: null })
})

test('regression: SCRAM auth lane remains usable after credentials are provisioned', { timeout: 20_000 }, async t => {
  const base = new Base({
    clientId: 'regression-scram-auth',
    bootstrapBrokers: regressionSaslBootstrapServers,
    strict: true,
    retries: 0,
    sasl: { mechanism: SASLMechanisms.SCRAM_SHA_256, username: 'admin', password: 'admin' }
  })
  t.after(() => base.close())

  // Keep a cheap SCRAM smoke test in the regression folder so auth failures are
  // visible even when the broader SASL suite is not selected.
  const metadata = await base.metadata({ topics: [] })
  deepStrictEqual(metadata.brokers.get(1), { ...saslBroker, rack: null })
})
