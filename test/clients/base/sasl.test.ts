import { deepStrictEqual, ok, rejects } from 'node:assert'
import { randomBytes } from 'node:crypto'
import { test } from 'node:test'
import { alterUserScramCredentialsV0, Base, Connection, MultipleErrors, ScramMechanisms } from '../../../src/index.ts'
import { hi, ScramAlgorithms } from '../../../src/protocol/sasl/scram-sha.ts'

test('should not connect to SASL protected broker by default', async t => {
  const base = new Base({ clientId: 'clientId', bootstrapBrokers: ['localhost:3012'], strict: true, retries: false })
  t.after(() => base.close())

  await rejects(() => base.metadata({ topics: [] }))
})

test('should connect to SASL protected broker using SASL/PLAIN', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:3012'],
    strict: true,
    retries: 0,
    sasl: { mechanism: 'PLAIN', username: 'admin', password: 'admin' }
  })

  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })

  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9092 })
})

test('should connect to SASL protected broker using SASL/SCRAM-SHA-256', async t => {
  // To use SCRAM-SHA-256, we need to create the user via the alterUserScramCredentialsV0 API
  {
    const connection = new Connection('clientId', {
      sasl: { mechanism: 'PLAIN', username: 'admin', password: 'admin' }
    })

    const iterations = ScramAlgorithms['SHA-256'].minIterations
    const salt = randomBytes(10)
    const saltedPassword = hi(ScramAlgorithms['SHA-256'], 'admin', salt, iterations)
    await connection.connect('localhost', 3012)

    await alterUserScramCredentialsV0.api.async(
      connection,
      [],
      [
        {
          name: 'admin',
          mechanism: ScramMechanisms.SCRAM_SHA_256,
          iterations: ScramAlgorithms['SHA-256'].minIterations,
          salt,
          saltedPassword
        }
      ]
    )

    await connection.close()
  }

  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:3012'],
    strict: true,
    retries: 0,
    sasl: { mechanism: 'SCRAM-SHA-256', username: 'admin', password: 'admin' }
  })

  t.after(() => base.close())

  const metadata = await base.metadata({ topics: [] })

  deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9092 })
})

test('should handle authentication errors', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:3012'],
    retries: 0,
    sasl: { mechanism: 'PLAIN', username: 'admin', password: 'invalid' }
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
