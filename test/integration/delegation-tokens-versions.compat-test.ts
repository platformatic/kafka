import { ok, strictEqual } from 'node:assert'
import test, { type TestContext } from 'node:test'
import * as apis from '../../src/apis/index.ts'
import { type Connection, kGetBootstrapConnection, SASLMechanisms } from '../../src/index.ts'
import { kafkaSaslBootstrapServers } from '../helpers.ts'
import { createAdmin, forEachVersion, implementedVersions, usableVersions, waitFor } from './helpers.ts'

// Delegation tokens have no Admin wrapper, so these drive the codecs directly. They only work on a
// broker configured with delegation.token.secret.key and only over an authenticated connection,
// which is why they run against the SASL broker rather than the PLAINTEXT ones.

function createSaslAdmin (t: any) {
  return createAdmin(t, {
    bootstrapBrokers: kafkaSaslBootstrapServers,
    sasl: { mechanism: SASLMechanisms.PLAIN, username: 'admin', password: 'admin' }
  })
}

function connectionOf (admin: any): Promise<Connection> {
  return new Promise((resolve, reject) => {
    admin[kGetBootstrapConnection]((error: Error | null, connection: Connection) => {
      error ? reject(error) : resolve(connection)
    })
  })
}

function invoke<ResultType> (api: any, connection: Connection, ...args: unknown[]): Promise<ResultType> {
  return new Promise((resolve, reject) => {
    api(connection, ...args, (error: Error | null, result: ResultType) => {
      error ? reject(error) : resolve(result)
    })
  })
}

function codec (name: string, version: number) {
  const key = (name.slice(0, 1).toLowerCase() + name.slice(1) + 'V' + version) as keyof typeof apis
  return (apis[key] as unknown as { api: any }).api
}

async function issueToken (probe: any, connection: Connection) {
  const newest = (await usableVersions(probe, 'CreateDelegationToken')).at(-1)!
  return invoke<any>(codec('CreateDelegationToken', newest), connection, null, null, [], 86400000n)
}

/**
 * Delegation tokens only work when the broker has a secret key, which docker-compose.yml cannot set
 * unconditionally: KRaft only supports them from Apache Kafka 3.6 (KIP-900), and on 3.5 the broker
 * refuses to start. Skip rather than fail when the feature is off, so the suite stays meaningful on
 * every broker in the matrix.
 */
async function delegationTokensEnabled (t: TestContext, probe: any): Promise<boolean> {
  // Kafka 3.5 does not implement the APIs under KRaft at all and reports them as UNSUPPORTED,
  // which is a different case from a broker which has them but no secret key configured.
  if (!(await usableVersions(probe, 'CreateDelegationToken')).length) {
    t.diagnostic('This broker does not support the delegation token APIs at all, skipping.')
    return false
  }

  try {
    await issueToken(probe, await connectionOf(probe))
    return true
  } catch (error) {
    t.diagnostic(
      'Delegation tokens are not enabled on this broker, skipping. Enable them with ' +
        '-f docker-compose.delegation-tokens.yml on Confluent 7.6.0 or later. ' +
        `(${(error as Error).message})`
    )
    return false
  }
}

test('CreateDelegationToken issues a token at every version', async t => {
  const probe = createSaslAdmin(t)

  if (!(await delegationTokensEnabled(t, probe))) {
    return
  }

  await forEachVersion(t, probe, 'CreateDelegationToken', async version => {
    const admin = createSaslAdmin(t)
    const connection = await connectionOf(admin)

    // v0 and v1 take no owner principal; v2 and v3 accept one but null means "the caller".
    const response = await invoke<any>(
      codec('CreateDelegationToken', version),
      connection,
      null,
      null,
      [],
      86400000n
    )

    ok(response.tokenId.length > 0, `CreateDelegationToken v${version} returned no token id`)
    ok(response.hmac.length > 0, `CreateDelegationToken v${version} returned no HMAC`)
    strictEqual(response.errorCode, 0, `CreateDelegationToken v${version} reported an error`)
  })
})

test('DescribeDelegationToken lists tokens at every version', async t => {
  const probe = createSaslAdmin(t)

  if (!(await delegationTokensEnabled(t, probe))) {
    return
  }

  await forEachVersion(t, probe, 'DescribeDelegationToken', async version => {
    const admin = createSaslAdmin(t)
    const connection = await connectionOf(admin)
    const created = await issueToken(probe, connection)

    // A new token reaches the brokers through the metadata log, so it is not immediately visible.
    await waitFor(
      async () => {
        const described = await invoke<any>(codec('DescribeDelegationToken', version), connection, null)

        if (!described.tokens.some((token: any) => token.tokenId === created.tokenId)) {
          throw new Error(`DescribeDelegationToken v${version} has not seen the token yet`)
        }

        return true
      },
      { interval: 200, timeout: 15000 }
    )
  })
})

test('RenewDelegationToken and ExpireDelegationToken act on a token at every version', async t => {
  const probe = createSaslAdmin(t)

  if (!(await delegationTokensEnabled(t, probe))) {
    return
  }

  for (const name of ['RenewDelegationToken', 'ExpireDelegationToken']) {
    await forEachVersion(t, probe, name, async version => {
      const admin = createSaslAdmin(t)
      const connection = await connectionOf(admin)

      const created = await issueToken(probe, connection)

      // Same propagation delay as above: the token is not renewable until the brokers have it.
      const response = await waitFor(
        () => invoke<any>(codec(name, version), connection, created.hmac, 3600000n),
        { interval: 200, timeout: 15000 }
      )

      strictEqual(response.errorCode, 0, `${name} v${version} reported an error`)
      ok(typeof response.expiryTimestampMs === 'bigint', `${name} v${version} returned no expiry timestamp`)
    })
  }
})

test('The delegation token codecs which no broker accepts are accounted for', t => {
  // v0 of each delegation token API is implemented but every supported broker advertises a minimum
  // of v1, so nothing here can reach it. This test exists so the gap is visible rather than silent.
  for (const name of [
    'CreateDelegationToken',
    'RenewDelegationToken',
    'ExpireDelegationToken',
    'DescribeDelegationToken'
  ]) {
    ok(implementedVersions(name).includes(0), `${name} v0 is expected to exist`)
  }

  t.diagnostic('Delegation token v0 codecs are covered by protocol tests only: brokers require v1+.')
})
