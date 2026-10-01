import { deepStrictEqual, ok, rejects } from 'node:assert'
import { once } from 'node:events'
import { before, test } from 'node:test'
import {
  allowedSASLMechanisms,
  AuthenticationError,
  Base,
  findErrorBy,
  Connection,
  metadataV12,
  MultipleErrors,
  NetworkError,
  parseBroker,
  SASLMechanisms,
  saslPlain,
  sleep,
  UserError
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
  // These are tested in their own file
  if (mechanism === 'OAUTHBEARER' || mechanism === 'GSSAPI') {
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

    deepStrictEqual(metadata.brokers.get(1), { ...saslBroker, rack: null })
  })

  test(`${mechanism} - should handle authentication errors`, async t => {
    const base = new Base({
      clientId: 'clientId',
      bootstrapBrokers: kafkaSaslBootstrapServers,
      retries: 2,
      retryDelay: 0,
      sasl: { mechanism, username: 'admin', password: 'invalid' }
    })

    t.after(() => base.close())
    let metadataRetries = 0
    base.on('client:performWithRetry:retry', operationId => {
      if (operationId === 'metadata') {
        metadataRetries++
      }
    })

    try {
      await base.metadata({ topics: [] })
      throw new Error('Expected error not thrown')
    } catch (error) {
      ok(error instanceof MultipleErrors)
      deepStrictEqual(error.errors[0].cause.message, 'SASL authentication failed.')
      deepStrictEqual(findErrorBy(error, 'apiId', 'SASL_AUTHENTICATION_FAILED')?.canRetry, false)
      deepStrictEqual(metadataRetries, 0)
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

    deepStrictEqual(metadata.brokers.get(1), { ...saslBroker, rack: null })
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

    deepStrictEqual(metadata.brokers.get(1), { ...saslBroker, rack: null })
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

test('reauthFraction schedules reauthentication while idle', async t => {
  const connection = new Connection('test-client', {
    sasl: { mechanism: SASLMechanisms.PLAIN, username: 'admin', password: 'admin', reauthFraction: 0.05 }
  })
  t.after(() => connection.close())

  await connection.connect(saslBroker.host, saslBroker.port)
  await once(connection, 'sasl:authentication:extended')
})

test('reauthFraction defaults to 80% for a direct connection', async t => {
  const connection = new Connection('test-client', {
    sasl: { mechanism: SASLMechanisms.PLAIN, username: 'admin', password: 'admin', lazyReauthentication: true }
  })
  t.after(() => connection.close())

  await connection.connect(saslBroker.host, saslBroker.port)
  let extended = 0
  connection.on('sasl:authentication:extended', () => extended++)

  await sleep(100)
  await metadataV12.api.async(connection, [])
  deepStrictEqual(extended, 0)
})

test('reauthLeadTime schedules the timer earlier than reauthFraction', async t => {
  const connection = new Connection('test-client', {
    sasl: {
      mechanism: SASLMechanisms.PLAIN,
      username: 'admin',
      password: 'admin',
      reauthLeadTime: 4500
    }
  })
  t.after(() => connection.close())

  await connection.connect(saslBroker.host, saslBroker.port)
  const startedAt = Date.now()
  await once(connection, 'sasl:authentication:extended')
  ok(Date.now() - startedAt < 3000)
})

for (const reauthLeadTime of [5000, 10000]) {
  test(`reauthLeadTime ${reauthLeadTime} falls back to reauthFraction`, async t => {
    const connection = new Connection('test-client', {
      sasl: {
        mechanism: SASLMechanisms.PLAIN,
        username: 'admin',
        password: 'admin',
        reauthFraction: 0.1,
        reauthLeadTime
      }
    })
    t.after(() => connection.close())

    await connection.connect(saslBroker.host, saslBroker.port)
    let extended = 0
    connection.on('sasl:authentication:extended', () => extended++)

    await sleep(100)
    deepStrictEqual(extended, 0)

    await once(connection, 'sasl:authentication:extended')
    deepStrictEqual(extended, 1)
  })
}

test('a zero session lifetime disables the reauthentication timer', async t => {
  const connection = new Connection('test-client', {
    sasl: {
      mechanism: SASLMechanisms.PLAIN,
      username: 'admin',
      password: 'admin',
      reauthFraction: 0.05,
      reauthLeadTime: 10000,
      lazyReauthentication: true,
      authenticate (_mechanism, connection, authenticate, username, password, _token, callback) {
        // Simulate a broker that disables reauthentication while still completing the SASL exchange.
        saslPlain.authenticate(authenticate, connection, username!, password!, (error, response) => {
          callback(error, response && { ...response, sessionLifetimeMs: 0n })
        })
      }
    }
  })
  t.after(() => connection.close())

  await connection.connect(saslBroker.host, saslBroker.port)
  let extended = 0
  connection.on('sasl:authentication:extended', () => extended++)

  await sleep(400)
  await metadataV12.api.async(connection, [])
  deepStrictEqual(extended, 0)
})

test('lazyReauthentication leaves idle connections alone and shares one reauthentication', async t => {
  const connection = new Connection('test-client', {
    sasl: {
      mechanism: SASLMechanisms.PLAIN,
      username: 'admin',
      password: 'admin',
      reauthFraction: 0.05,
      lazyReauthentication: true
    }
  })
  t.after(() => connection.close())

  await connection.connect(saslBroker.host, saslBroker.port)
  let extended = 0
  connection.on('sasl:authentication:extended', () => extended++)

  await sleep(400)
  deepStrictEqual(extended, 0)

  await Promise.all([metadataV12.api.async(connection, []), metadataV12.api.async(connection, [])])
  deepStrictEqual(extended, 1)
})

test('lazyReauthentication fails waiting requests if reauthentication fails', async t => {
  let password = 'admin'
  const connection = new Connection('test-client', {
    sasl: {
      mechanism: SASLMechanisms.PLAIN,
      username: 'admin',
      password: () => password,
      reauthFraction: 0.05,
      lazyReauthentication: true
    }
  })
  t.after(() => connection.close())

  await connection.connect(saslBroker.host, saslBroker.port)
  password = 'invalid'
  await sleep(400)

  const results = await Promise.allSettled([
    metadataV12.api.async(connection, []),
    metadataV12.api.async(connection, [])
  ])
  for (const result of results) {
    deepStrictEqual(result.status, 'rejected')
    ok((result as PromiseRejectedResult).reason instanceof NetworkError)
  }
})

test('should show proper error when SASL failed due to attempted TLS to a non TLS broker', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslBootstrapServers,
    strict: true,
    retries: false,
    sasl: { mechanism: SASLMechanisms.PLAIN, username: 'admin', password: 'admin' },
    tls: {
      rejectUnauthorized: false
    }
  })
  t.after(() => base.close())

  try {
    await base.metadata({ topics: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    ok(error instanceof MultipleErrors)
    ok(error.errors[0] instanceof NetworkError)

    const cause = error.errors[0].cause
    ok(cause instanceof UserError)
    deepStrictEqual(cause.message, 'TLS handshake failed. Please verify the broker supports TLS.')
  }
})
