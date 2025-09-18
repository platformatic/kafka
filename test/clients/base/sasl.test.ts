import { deepStrictEqual, ok, rejects } from 'node:assert'
import { once } from 'node:events'
import { test } from 'node:test'
import {
  AuthenticationError,
  Base,
  MultipleErrors,
  NetworkError,
  sleep,
  type SASLMechanism
} from '../../../src/index.ts'
import { isKafka } from '../../helpers.ts'

test('should not connect to SASL protected broker by default', async t => {
  const base = new Base({ clientId: 'clientId', bootstrapBrokers: ['localhost:9095'], strict: true, retries: false })
  t.after(() => base.close())

  await rejects(() => base.metadata({ topics: [] }))
})

for (const mechanism of ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512']) {
  test(
    `${mechanism} - should connect to SASL protected broker`,
    // Disable SCRAM-SHA for Kafka 3.5.0 due to known issues in the image bitnami/kafka:3.5.0
    { skip: isKafka('3.5.0') },
    async t => {
      const base = new Base({
        clientId: 'clientId',
        bootstrapBrokers: ['localhost:9095'],
        strict: true,
        retries: 0,
        sasl: { mechanism: mechanism as SASLMechanism, username: 'admin', password: 'admin' }
      })

      t.after(() => base.close())

      const metadata = await base.metadata({ topics: [] })

      deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9095 })
    }
  )

  test(`${mechanism} - should handle authentication errors`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: ['localhost:9095'],
      retries: 0,
      sasl: { mechanism: mechanism as SASLMechanism, username: 'admin', password: 'invalid' }
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
      bootstrapBrokers: ['localhost:9095'],
      strict: true,
      retries: 0,
      sasl: {
        mechanism: mechanism as SASLMechanism,
        username () {
          return 'admin'
        },
        password: 'admin'
      }
    })

    t.after(() => base.close())

    const metadata = await base.metadata({ topics: [] })

    deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9095 })
  })

  test(`${mechanism} - should accept an async function as credential provider`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: ['localhost:9095'],
      strict: true,
      retries: 0,
      sasl: {
        mechanism: mechanism as SASLMechanism,
        username: 'admin',
        async password () {
          await sleep(1000)
          return 'admin'
        }
      }
    })

    t.after(() => base.close())

    const metadata = await base.metadata({ topics: [] })

    deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9095 })
  })

  test(`${mechanism} - should handle sync credential provider errors`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: ['localhost:9095'],
      strict: true,
      retries: 0,
      sasl: {
        mechanism: mechanism as SASLMechanism,
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
      deepStrictEqual(networkError.message, 'Connection to localhost:9095 failed.')

      const authenticationError = networkError.cause
      deepStrictEqual(authenticationError instanceof AuthenticationError, true)
      deepStrictEqual(authenticationError.message, `The SASL/${mechanism} username provider threw an error.`)
      deepStrictEqual(authenticationError.cause.message, 'Kaboom!')
    }
  })

  test(`${mechanism} - should handle async credential provider errors`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: ['localhost:9095'],
      strict: true,
      retries: 0,
      sasl: {
        mechanism: mechanism as SASLMechanism,
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
      deepStrictEqual(networkError.message, 'Connection to localhost:9095 failed.')

      const authenticationError = networkError.cause
      deepStrictEqual(authenticationError instanceof AuthenticationError, true)
      deepStrictEqual(authenticationError.message, `The SASL/${mechanism} password provider threw an error.`)
      deepStrictEqual(authenticationError.cause.message, 'Kaboom!')
    }
  })

  test(`${mechanism} - should automatically refresh expired tokens when the server provides a session_lifetime`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: ['localhost:9095'],
      strict: true,
      retries: 0,
      sasl: { mechanism: mechanism as SASLMechanism, username: 'admin', password: 'admin' }
    })

    t.after(() => base.close())

    await base.metadata({ topics: [] })

    // Wait for the token to expire, and for the re-authentication to happen
    await Promise.all([sleep(6000), once(base, 'client:broker:sasl:authentication:extended')])

    await base.metadata({ topics: [], forceUpdate: true })
  })
}
