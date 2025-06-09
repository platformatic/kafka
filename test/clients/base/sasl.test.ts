import { deepStrictEqual, ok, rejects } from 'node:assert'
import { test } from 'node:test'
import { Base, MultipleErrors, SASLMechanisms } from '../../../src/index.ts'
import { isKafka } from '../../helpers.ts'

test('should not connect to SASL protected broker by default', async t => {
  const base = new Base({ clientId: 'clientId', bootstrapBrokers: ['localhost:9095'], strict: true, retries: false })
  t.after(() => base.close())

  await rejects(() => base.metadata({ topics: [] }))
})

for (const mechanism of SASLMechanisms) {
  test(
    `should connect to SASL protected broker using SASL/${mechanism}`,
    // Disable SCRAM-SHA for Kafka 3.5.0 due to known issues in the image bitnami/kafka:3.5.0
    { skip: isKafka('3.5.0') },
    async t => {
      const base = new Base({
        clientId: 'clientId',
        bootstrapBrokers: ['localhost:9095'],
        strict: true,
        retries: 0,
        sasl: { mechanism, username: 'admin', password: 'admin' }
      })

      t.after(() => base.close())

      const metadata = await base.metadata({ topics: [] })

      deepStrictEqual(metadata.brokers.get(1), { host: 'localhost', port: 9092 })
    }
  )
}

test('should handle authentication errors', async t => {
  const base = new Base({
    clientId: 'clientId',
    bootstrapBrokers: ['localhost:9095'],
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
