import { deepStrictEqual, ok, rejects } from 'node:assert'
import { once } from 'node:events'
import { resolve } from 'node:path'
import { test } from 'node:test'
import { AuthenticationError, Base, MultipleErrors, NetworkError, parseBroker, sleep } from '../../../src/index.ts'
import { kafkaSaslKerberosBootstrapServers } from '../../helpers.ts'

const saslBroker = parseBroker(kafkaSaslKerberosBootstrapServers[0])

test('should not connect to SASL protected broker by default', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslKerberosBootstrapServers,
    strict: true,
    retries: false
  })
  t.after(() => base.close())

  await rejects(() => base.metadata({ topics: [] }))
})

test('should connect to SASL protected broker using SASL/GSSAPI using username and password', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslKerberosBootstrapServers,
    strict: true,
    retries: 0,
    sasl: {
      mechanism: 'GSSAPI',
      username: 'admin-password@EXAMPLE.COM',
      password: 'admin'
    }
  })

  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })

  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9003 })
})

test('should connect to SASL protected broker using SASL/GSSAPI using keytab', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslKerberosBootstrapServers,
    strict: true,
    retries: 0,
    sasl: {
      mechanism: 'GSSAPI',
      username: 'admin-keytab@EXAMPLE.COM',
      keytab: resolve(import.meta.dirname, '../../../tmp/kerberos/admin.keytab')
    }
  })

  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })

  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9003 })
})

test('should handle authentication errors', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslKerberosBootstrapServers,
    retries: 0,
    sasl: {
      mechanism: 'GSSAPI',
      username: 'admin-password@EXAMPLE.COM',
      password: 'admin123'
    }
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

test('should accept a function as credential provider', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslKerberosBootstrapServers,
    retries: 0,
    sasl: {
      mechanism: 'GSSAPI',
      username () {
        return 'admin-password@EXAMPLE.COM'
      },
      password: 'admin'
    }
  })

  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })

  deepStrictEqual(metadata.brokers.get(1), saslBroker)
})

test('should accept an async function as credential provider', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslKerberosBootstrapServers,
    retries: 0,
    sasl: {
      mechanism: 'GSSAPI',
      username: 'admin-password@EXAMPLE.COM',
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

test('should handle sync credential provider errors', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslKerberosBootstrapServers,
    retries: 0,
    sasl: {
      mechanism: 'GSSAPI',
      username () {
        throw new Error('Kaboom!')
      },
      password: 'admin'
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
    deepStrictEqual(networkError.message, `Connection to ${kafkaSaslKerberosBootstrapServers[0]} failed.`)

    const authenticationError = networkError.cause
    deepStrictEqual(authenticationError instanceof AuthenticationError, true)
    deepStrictEqual(authenticationError.message, 'The SASL/GSSAPI username provider threw an error.')
    deepStrictEqual(authenticationError.cause.message, 'Kaboom!')
  }
})

test('should handle async credential provider errors', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslKerberosBootstrapServers,
    strict: true,
    retries: 0,
    sasl: {
      mechanism: 'GSSAPI',
      username: 'admin-password@EXAMPLE.COM',
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
    deepStrictEqual(networkError.message, `Connection to ${kafkaSaslKerberosBootstrapServers[0]} failed.`)

    const authenticationError = networkError.cause
    deepStrictEqual(authenticationError instanceof AuthenticationError, true)
    deepStrictEqual(authenticationError.message, 'The SASL/GSSAPI password provider threw an error.')
    deepStrictEqual(authenticationError.cause.message, 'Kaboom!')
  }
})

test('should automatically refresh expired tokens when the server provides a session_lifetime', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: kafkaSaslKerberosBootstrapServers,
    strict: true,
    retries: 0,
    sasl: {
      mechanism: 'GSSAPI',
      username: 'admin-password@EXAMPLE.COM',
      password: 'admin'
    }
  })

  t.after(() => base.close())

  await base.metadata({ topics: [] })

  // Wait for the token to expire, and for the re-authentication to happen
  await Promise.all([sleep(6000), once(base, 'client:broker:sasl:authentication:extended')])

  await base.metadata({ topics: [], forceUpdate: true })
})
