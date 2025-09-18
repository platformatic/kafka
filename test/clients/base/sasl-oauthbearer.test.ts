import { createSigner } from 'fast-jwt'
import { deepStrictEqual, rejects } from 'node:assert'
import { once } from 'node:events'
import { test } from 'node:test'
import { AuthenticationError, Base, NetworkError, sleep } from '../../../src/index.ts'

test('should not connect to SASL protected broker by default', async t => {
  const base = new Base({ clientId: 'clientId', bootstrapBrokers: ['localhost:9095'], strict: true, retries: false })
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
    bootstrapBrokers: ['localhost:9095'],
    strict: true,
    retries: 0,
    sasl: { mechanism: 'OAUTHBEARER', token }
  })

  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })

  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9095 })
})

// The 'should handle authentication errors' is not possible here as the Kafka unsecured validator accepts any token

test('should accept a function as credential provider', async t => {
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
    bootstrapBrokers: ['localhost:9095'],
    strict: true,
    retries: 0,
    sasl: {
      mechanism: 'OAUTHBEARER',
      token () {
        return token
      }
    }
  })

  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })

  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9095 })
})

test('should accept an async function as credential provider', async t => {
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
    bootstrapBrokers: ['localhost:9095'],
    strict: true,
    retries: 0,
    sasl: {
      mechanism: 'OAUTHBEARER',
      async token () {
        await sleep(1000)
        return token
      }
    }
  })

  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })

  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9095 })
})

test('should handle sync credential provider errors', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:9095'],
    strict: true,
    retries: 0,
    sasl: {
      mechanism: 'OAUTHBEARER',
      token () {
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
    deepStrictEqual(authenticationError.message, 'The SASL/OAUTHBEARER token provider threw an error.')
    deepStrictEqual(authenticationError.cause.message, 'Kaboom!')
  }
})

test('should handle async credential provider errors', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:9095'],
    strict: true,
    retries: 0,
    sasl: {
      mechanism: 'OAUTHBEARER',
      async token () {
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
    deepStrictEqual(authenticationError.message, 'The SASL/OAUTHBEARER token provider threw an error.')
    deepStrictEqual(authenticationError.cause.message, 'Kaboom!')
  }
})

test('should automatically refresh expired tokens when the server provides a session_lifetime', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:9095'],
    strict: true,
    retries: 0,
    sasl: {
      mechanism: 'OAUTHBEARER',
      token () {
        const signSync = createSigner({
          algorithm: 'none',
          iss: 'kafka',
          aud: ['users'],
          sub: 'admin',
          expiresIn: '3s'
        })

        return signSync({ scope: 'test' })
      }
    }
  })

  t.after(() => base.close())

  await base.metadata({ topics: [] })

  // Wait for the token to expire, and for the re-authentication to happen
  await Promise.all([sleep(6000), once(base, 'client:broker:sasl:authentication:extended')])

  await base.metadata({ topics: [], forceUpdate: true })
})
