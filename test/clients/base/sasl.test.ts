import { deepStrictEqual, ok, rejects } from 'node:assert'
import { once } from 'node:events'
import { before, test } from 'node:test'
import {
  allowedSASLMechanisms,
  AuthenticationError,
  Base,
  MultipleErrors,
  NetworkError,
  parseBroker,
  sleep
} from '../../../src/index.ts'
import { createScramUsers } from '../../fixtures/create-users.ts'
import { kafkaSaslBootstrapServers } from '../../helpers.ts'

// Create passwords as Confluent Kafka images don't support it via environment
const saslBroker = parseBroker(kafkaSaslBootstrapServers[0])
before(() => createScramUsers(saslBroker))

test('UNAUTHENTICATED - should not connect to SASL protected broker by default', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslBootstrapServers,
    strict: true,
    retries: false
  })
  t.after(() => base.close())

  await rejects(() => base.metadata({ topics: [] }))
})

for (const mechanism of allowedSASLMechanisms) {
  if (mechanism === 'OAUTHBEARER') {
    // GSSAPI requires a properly configured Kerberos environment
    // which is out of scope for these tests
    continue
  }

  test(`${mechanism} - should connect to SASL protected broker`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: kafkaSaslBootstrapServers,
      strict: true,
      retries: 0,
      sasl: { mechanism, username: 'admin', password: 'admin' }
    })

    t.after(() => base.close())

    const metadata = await base.metadata({ topics: [] })

    deepStrictEqual(metadata.brokers.get(1), saslBroker)
  })

  test(`${mechanism} - should handle authentication errors`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: kafkaSaslBootstrapServers,
      retries: 0,
      sasl: { mechanism, username: 'admin', password: 'invalid' }
    })

    t.after(() => base.close())

    try {
      await base.metadata({ topics: [] })
      throw new Error('Expected error not thrown')
    } catch (error) {
      ok(error instanceof MultipleErrors)
      deepStrictEqual(error.errors[0].cause.message, 'SASL authentication failed.')
    }
  })

  test(`${mechanism} - should accept a function as credential provider`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: kafkaSaslBootstrapServers,
      strict: true,
      retries: 0,
      sasl: {
        mechanism,
        username () {
          return 'admin'
        },
        password: 'admin'
      }
    })

    t.after(() => base.close())

    const metadata = await base.metadata({ topics: [] })

    deepStrictEqual(metadata.brokers.get(1), saslBroker)
  })

  test(`${mechanism} - should accept an async function as credential provider`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: kafkaSaslBootstrapServers,
      strict: true,
      retries: 0,
      sasl: {
        mechanism,
        username: 'admin',
        async password () {
          await sleep(1000)
          return 'admin'
        }
      }
    })

    t.after(() => base.close())

    const metadata = await base.metadata({ topics: [] })

    deepStrictEqual(metadata.brokers.get(1), saslBroker)
  })

  test(`${mechanism} - should handle sync credential provider errors`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: kafkaSaslBootstrapServers,
      strict: true,
      retries: 0,
      sasl: {
        mechanism,
        username () {
          throw new Error('Kaboom!')
        }
      }
    })

    t.after(() => base.close())

    try {
      await base.metadata({ topics: [] })
      throw new Error('Expected error not thrown')
    } catch (error) {
      deepStrictEqual(error.message, 'Cannot connect to any broker.')

      const networkError = error.errors[0]
      deepStrictEqual(networkError instanceof NetworkError, true)
      deepStrictEqual(networkError.message, `Connection to ${kafkaSaslBootstrapServers[0]} failed.`)

      const authenticationError = networkError.cause
      deepStrictEqual(authenticationError instanceof AuthenticationError, true)
      deepStrictEqual(authenticationError.message, `The SASL/${mechanism} username provider threw an error.`)
      deepStrictEqual(authenticationError.cause.message, 'Kaboom!')
    }
  })

  test(`${mechanism} - should handle async credential provider errors`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: kafkaSaslBootstrapServers,
      strict: true,
      retries: 0,
      sasl: {
        mechanism,
        username: 'admin',
        async password () {
          throw new Error('Kaboom!')
        }
      }
    })

    t.after(() => base.close())

    try {
      await base.metadata({ topics: [] })
      throw new Error('Expected error not thrown')
    } catch (error) {
      deepStrictEqual(error.message, 'Cannot connect to any broker.')

      const networkError = error.errors[0]
      deepStrictEqual(networkError instanceof NetworkError, true)
      deepStrictEqual(networkError.message, `Connection to ${kafkaSaslBootstrapServers[0]} failed.`)

      const authenticationError = networkError.cause
      deepStrictEqual(authenticationError instanceof AuthenticationError, true)
      deepStrictEqual(authenticationError.message, `The SASL/${mechanism} password provider threw an error.`)
      deepStrictEqual(authenticationError.cause.message, 'Kaboom!')
    }
  })

  test(`${mechanism} - should automatically refresh expired tokens when the server provides a session_lifetime`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: kafkaSaslBootstrapServers,
      strict: true,
      retries: 0,
      sasl: { mechanism, username: 'admin', password: 'admin' }
    })

    t.after(() => base.close())

    await base.metadata({ topics: [] })

    // Wait for the token to expire, and for the re-authentication to happen
    await Promise.all([sleep(6000), once(base, 'client:broker:sasl:authentication:extended')])

    await base.metadata({ topics: [], forceUpdate: true })
  })
}
