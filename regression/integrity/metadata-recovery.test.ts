import { deepStrictEqual, strictEqual } from 'node:assert'
import { test } from 'node:test'
import { Base, parseBroker } from '../../src/index.ts'
import { regressionBootstrapServers } from '../helpers/index.ts'

test('regression #232: metadata refresh survives a failed bootstrap broker when a discovered broker is healthy', async t => {
  // The first broker is intentionally unreachable. The client must continue to a
  // reachable bootstrap broker and keep the discovered broker set usable.
  const base = new Base({
    clientId: 'regression-metadata-recovery',
    bootstrapBrokers: ['localhost:65535', regressionBootstrapServers[0]!],
    retries: 2,
    retryDelay: 100
  })
  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [], forceUpdate: true })

  // A successful metadata response from broker 1 proves the refresh did not stay
  // pinned to the dead bootstrap address.
  strictEqual(metadata.brokers.size > 0, true)
  deepStrictEqual(metadata.brokers.get(1), { ...parseBroker(regressionBootstrapServers[0]!), rack: null })
})
