import { deepStrictEqual, ok, rejects, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { kConnections } from '../../../src/clients/base/base.ts'
import {
  addPartitionsToTxnV5,
  type ClientDiagnosticEvent,
  endTxnV4,
  MultipleErrors,
  ProduceAcks,
  producerTransactionsChannel,
  ProtocolError,
  UnsupportedApiError,
  UserError
} from '../../../src/index.ts'
import {
  createProducer,
  createTopic,
  createTracingChannelVerifier,
  mockAPI,
  mockConnectionPoolGet,
  mockConnectionPoolGetFirstAvailable,
  mockedErrorMessage,
  mockedOperationId,
  mockMetadata,
  mockUnavailableAPI
} from '../../helpers.ts'

test('beginTransaction should initialize a transaction', async t => {
  // Create a producer with idempotent=true
  const transactionalId = randomUUID()
  const producer = createProducer(t, {
    idempotent: true,
    strict: true,
    transactionalId
  })

  const verifyTracingChannel = createTracingChannelVerifier(
    producerTransactionsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: producer,
          operation: 'begin',
          operationId: mockedOperationId
        })
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'begin'
  )

  // Initialize and check if properly configured for idempotency
  await producer.beginTransaction()

  // Verify producer info structure
  strictEqual(typeof producer.producerId, 'bigint')
  strictEqual(typeof producer.producerEpoch, 'number')
  strictEqual(typeof producer.transactionalId, 'string')
  strictEqual(typeof producer.coordinatorId, 'number')
  ok(producer.producerId! >= 0n, 'Producer ID should be a positive bigint')
  ok(producer.producerEpoch! >= 0, 'Producer epoch should be a non-negative number')
  strictEqual(producer.transactionalId, transactionalId)

  verifyTracingChannel()

  await producer.beginTransaction() // This should be a no-op
})

test('beginTransaction should fail when the producer is not idempotent', async t => {
  const producer = createProducer(t, {
    strict: true
  })

  await rejects(
    async () => {
      await producer.beginTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      strictEqual(error.message, 'Cannot begin a transaction on a non-idempotent producer.')
      return true
    }
  )
})

test('beginTransaction should fail when the producer has no transactionalId', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    strict: true
  })

  await rejects(
    async () => {
      await producer.beginTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      strictEqual(error.message, 'transactionalId must be set when creating a producer to begin a transaction.')
      return true
    }
  )
})

test('beginTransaction should handle unavailable API errors', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    transactionalId: randomUUID(),
    strict: true
  })

  mockUnavailableAPI(producer, 'FindCoordinator')

  // Attempt to initialize idempotent producer - should fail with connection error
  await rejects(
    async () => {
      await producer.beginTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof UnsupportedApiError, true)
      strictEqual(error.message.includes('Unsupported API FindCoordinator.'), true)
      return true
    }
  )
})

test('beginTransaction should handle errors from findCoordinator', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    transactionalId: randomUUID(),
    strict: true
  })

  mockConnectionPoolGetFirstAvailable(producer[kConnections])

  // Attempt to initialize idempotent producer - should fail with connection error
  await rejects(
    async () => {
      await producer.beginTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('beginTransaction should handle errors from initIdempotentProducer', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    transactionalId: randomUUID(),
    strict: true
  })

  mockConnectionPoolGetFirstAvailable(producer[kConnections], 3)

  // Attempt to initialize idempotent producer - should fail with connection error
  await rejects(
    async () => {
      await producer.beginTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('commitTransaction should commit a transaction', async t => {
  // Create a producer with idempotent=true
  const transactionalId = randomUUID()
  const producer = createProducer(t, {
    idempotent: true,
    strict: true,
    transactionalId
  })
  const testTopic = await createTopic(t, true)

  const verifyTracingChannel = createTracingChannelVerifier(
    producerTransactionsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: producer,
          operation: 'commit',
          operationId: mockedOperationId
        })
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'commit'
  )

  // Create a complete transaction
  await producer.beginTransaction()
  await producer.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value'),
        partition: 0
      }
    ]
  })

  await producer.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value'),
        partition: 0
      }
    ]
  })

  await producer.commitTransaction()

  strictEqual(typeof producer.transactionalId, 'undefined')
  verifyTracingChannel()
})

test('commitTransaction should fail when the producer is not idempotent', async t => {
  const producer = createProducer(t, {
    strict: true
  })

  await rejects(
    async () => {
      await producer.commitTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      strictEqual(error.message, 'Cannot commit a transaction of a non-idempotent producer.')
      return true
    }
  )
})

test('commitTransaction should fail if no transaction is active', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })

  await rejects(
    async () => {
      await producer.commitTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      strictEqual(error.message, 'No active transaction to commit.')
      return true
    }
  )
})

test('commitTransaction should handle errors from ConnectionPool.get', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    transactionalId: randomUUID(),
    strict: true
  })
  const testTopic = await createTopic(t, true)

  // Create a complete transaction
  await producer.beginTransaction()
  await producer.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value'),
        partition: 0
      }
    ]
  })

  mockConnectionPoolGet(producer[kConnections])

  // Attempt to initialize idempotent producer - should fail with connection error
  await rejects(
    async () => {
      await producer.commitTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('commitTransaction should handle unavailable API errors', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    transactionalId: randomUUID(),
    strict: true
  })
  const testTopic = await createTopic(t, true)

  // Create a complete transaction
  await producer.beginTransaction()
  await producer.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value'),
        partition: 0
      }
    ]
  })

  mockUnavailableAPI(producer, 'EndTxn')

  await rejects(
    async () => {
      await producer.commitTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof UnsupportedApiError, true)
      strictEqual(error.message.includes('Unsupported API EndTxn.'), true)
      return true
    }
  )
})

test('commitTransaction should handle errors from the API', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    transactionalId: randomUUID(),
    strict: true
  })
  const testTopic = await createTopic(t, true)

  // Create a complete transaction
  await producer.beginTransaction()
  await producer.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value'),
        partition: 0
      }
    ]
  })

  mockAPI(producer[kConnections], endTxnV4.api.key)

  await rejects(
    async () => {
      await producer.commitTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes(mockedErrorMessage), true)
      return true
    }
  )

  strictEqual(typeof producer.transactionalId, 'string')
})

test('commitTransaction should handle fencing errors from the API', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    transactionalId: randomUUID(),
    strict: true
  })
  const testTopic = await createTopic(t, true)

  // Create a complete transaction
  await producer.beginTransaction()
  await producer.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value'),
        partition: 0
      }
    ]
  })

  mockAPI(producer[kConnections], endTxnV4.api.key, new ProtocolError('INVALID_PRODUCER_EPOCH'))

  await rejects(
    async () => {
      await producer.commitTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof ProtocolError, true)
      return true
    }
  )

  strictEqual(typeof producer.transactionalId, 'undefined')
})

test('abortTransaction should abort a transaction', async t => {
  // Create a producer with idempotent=true
  const transactionalId = randomUUID()
  const producer = createProducer(t, {
    idempotent: true,
    strict: true,
    transactionalId
  })
  const testTopic = await createTopic(t, true)

  const verifyTracingChannel = createTracingChannelVerifier(
    producerTransactionsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: producer,
          operation: 'abort',
          operationId: mockedOperationId
        })
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'abort'
  )

  // Create a complete transaction
  await producer.beginTransaction()
  await producer.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value')
      }
    ]
  })

  await producer.abortTransaction()

  strictEqual(typeof producer.transactionalId, 'undefined')
  verifyTracingChannel()
})

test('abortTransaction should fail when the producer is not idempotent', async t => {
  const producer = createProducer(t, {
    strict: true
  })

  await rejects(
    async () => {
      await producer.abortTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      strictEqual(error.message, 'Cannot abort a transaction of a non-idempotent producer.')
      return true
    }
  )
})

test('abortTransaction should fail if no transaction is active', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })

  await rejects(
    async () => {
      await producer.abortTransaction()
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      strictEqual(error.message, 'No active transaction to abort.')
      return true
    }
  )
})

// Other internal behaviors of abortTransaction are covered in the commitTransaction test since they share
// the same low level code.

test('send should handle errors from Base.metadata during a transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t)

  await producer.beginTransaction()

  mockMetadata(producer)

  // Attempt to initialize idempotent producer - should fail with connection error
  await rejects(
    async () => {
      await producer.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('send should handle unavailable API errors during transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t)

  await producer.beginTransaction()

  mockUnavailableAPI(producer, 'AddPartitionsToTxn')

  // Attempt to initialize idempotent producer - should fail with connection error
  await rejects(
    async () => {
      await producer.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    (error: any) => {
      strictEqual(error instanceof UnsupportedApiError, true)
      strictEqual(error.message.includes('Unsupported API AddPartitionsToTxn.'), true)
      return true
    }
  )
})

test('send should handle errors from ConnectionPool.get during transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t)

  await producer.beginTransaction()

  mockConnectionPoolGet(producer[kConnections], 3)

  // Attempt to initialize idempotent producer - should fail with connection error
  await rejects(
    async () => {
      await producer.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('send should handle errors from the transaction API', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t)

  await producer.beginTransaction()

  mockAPI(producer[kConnections], addPartitionsToTxnV5.api.key)

  // Attempt to initialize idempotent producer - should fail with connection error
  await rejects(
    async () => {
      await producer.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes(mockedErrorMessage), true)
      return true
    }
  )
})
