import { deepStrictEqual, ok, rejects, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { test } from 'node:test'
import {
  type FindCoordinatorRequest,
  type FindCoordinatorResponse
} from '../../../src/apis/metadata/find-coordinator-v6.ts'
import { kConnections, kGetApi, kOptions } from '../../../src/clients/base/base.ts'
import { Transaction } from '../../../src/clients/producer/transaction.ts'
import {
  addOffsetsToTxnV4,
  addPartitionsToTxnV5,
  type API,
  type Callback,
  type ClientDiagnosticEvent,
  type Connection,
  endTxnV4,
  type Message,
  MessagesStreamModes,
  MultipleErrors,
  ProduceAcks,
  producerTransactionsChannel,
  ProtocolError,
  txnOffsetCommitV4,
  UnsupportedApiError,
  UserError
} from '../../../src/index.ts'
import { kInstance } from '../../../src/symbols.ts'
import {
  createConsumer,
  createProducer,
  createTopic,
  createTracingChannelVerifier,
  mockAPI,
  mockConnectionPoolGet,
  mockConnectionPoolGetFirstAvailable,
  mockedErrorMessage,
  mockedOperationId,
  mockMetadata,
  mockMethod,
  mockUnavailableAPI
} from '../../helpers.ts'

function mockTransactionCoordinator (producer: any, coordinatorId: number, endError?: Error): void {
  mockConnectionPoolGetFirstAvailable(producer[kConnections], 1, null, {} as any)
  mockConnectionPoolGet(
    producer[kConnections],
    () => true,
    null,
    undefined,
    (_original, _broker, callback) => {
      callback(null, {})
      return true
    }
  )

  mockMetadata(
    producer,
    () => true,
    null,
    undefined,
    (_original, _options, callback) => {
      callback(null, {
        id: 'test-cluster',
        brokers: new Map([[coordinatorId, { host: 'localhost', port: 9092 }]]),
        topics: new Map(),
        lastUpdate: Date.now(),
        controllerId: coordinatorId
      })
      return true
    }
  )

  mockMethod(
    producer,
    kGetApi,
    () => true,
    null,
    undefined,
    (_original, name, callback: Callback<API<FindCoordinatorRequest, FindCoordinatorResponse>>) => {
      if (name === 'FindCoordinator') {
        callback(null, ((
          _connection: Connection,
          _keyType: number,
          keys: string[],
          apiCallback: Callback<FindCoordinatorResponse>
        ) => {
          apiCallback(null, {
            throttleTimeMs: 0,
            coordinators: [
              { key: keys[0], nodeId: coordinatorId, host: 'localhost', port: 9092, errorCode: 0, errorMessage: null }
            ]
          })
        }) as any)
        return true
      }

      if (name === 'InitProducerId') {
        callback(null, ((...args: any[]) =>
          args.at(-1)(null, { throttleTimeMs: 0, errorCode: 0, producerId: 1n, producerEpoch: 0 })) as any)
        return true
      }

      if (endError) {
        callback(null, ((...args: any[]) => args.at(-1)(endError)) as any)
      } else {
        callback(new Error(`Unexpected API ${name}`))
      }

      return true
    }
  )
}

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
  const transaction = await producer.beginTransaction()

  // Verify producer info structure
  strictEqual(typeof producer.producerId, 'bigint')
  strictEqual(typeof producer.producerEpoch, 'number')
  strictEqual(transaction.id, transactionalId)
  strictEqual(typeof producer.coordinatorId, 'number')
  ok(producer.producerId! >= 0n, 'Producer ID should be a positive bigint')
  ok(producer.producerEpoch! >= 0, 'Producer epoch should be a non-negative number')

  verifyTracingChannel()

  await rejects(
    async () => {
      await producer.beginTransaction()
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'There is already an active transaction.')
      return true
    }
  )
})

test('transaction commit should invalidate cached coordinator on coordinator protocol errors', async t => {
  const transactionalId = randomUUID()
  const producer = createProducer(t, {
    idempotent: true,
    strict: true,
    transactionalId
  })
  producer[kOptions].retries = 0
  mockTransactionCoordinator(producer, 1, new ProtocolError('NOT_COORDINATOR'))

  const transaction = await producer.beginTransaction()
  const coordinatorId = producer.coordinatorId

  strictEqual(typeof coordinatorId, 'number')

  await rejects(
    async () => {
      await transaction.commit()
    },
    error => {
      strictEqual((error as Error).message, 'This is not the correct coordinator.')
      return true
    }
  )

  strictEqual(producer.coordinatorId, undefined)
})

test('beginTransaction should validate options in strict mode', async t => {
  const producer = createProducer(t, { strict: true })

  // Test with invalid producerId type
  await rejects(
    async () => {
      await producer.beginTransaction({
        // @ts-expect-error - Intentionally passing invalid option
        producerId: 'not-a-bigint'
      })
    },
    error => {
      strictEqual(error instanceof UserError, true)
      ok((error as Error).message.includes('producerId'), 'Error should mention producerId')
      return true
    }
  )
})

test('beginTransaction should fail when the producer is not idempotent', async t => {
  const producer = createProducer(t, {
    strict: true
  })

  await rejects(
    async () => {
      await producer.beginTransaction()
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'Cannot begin a transaction on a non-idempotent producer.')
      return true
    }
  )
})

test('beginTransaction should handle unavailable API errors', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    strict: true
  })

  mockUnavailableAPI(producer, 'FindCoordinator')

  await rejects(
    async () => {
      await producer.beginTransaction()
    },
    error => {
      strictEqual(error instanceof UnsupportedApiError, true)
      strictEqual((error as Error).message.includes('Unsupported API FindCoordinator.'), true)
      return true
    }
  )
})

test('beginTransaction should handle errors from findCoordinator', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    strict: true
  })

  mockConnectionPoolGetFirstAvailable(producer[kConnections])

  await rejects(
    async () => {
      await producer.beginTransaction()
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('beginTransaction should handle errors from initIdempotentProducer', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    strict: true
  })

  mockConnectionPoolGetFirstAvailable(producer[kConnections], 3)

  await rejects(
    async () => {
      await producer.beginTransaction()
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('commit should commit a transaction', async t => {
  // Create a producer with idempotent=true
  const transactionalId = randomUUID()
  const producer = createProducer(t, {
    idempotent: true,
    strict: true,
    transactionalId,
    retries: 0
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
          operationId: mockedOperationId,
          transaction: context.transaction
        })
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'commit'
  )

  // Create a complete transaction
  const transaction = await producer.beginTransaction()
  await transaction.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value'),
        partition: 0
      }
    ]
  })

  await transaction.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value'),
        partition: 0
      }
    ]
  })

  await transaction.commit()

  ok(!producer.transaction)
  ok(transaction.completed)
  verifyTracingChannel()
})

test('commit should fail if committed twice', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t, true)

  const transaction = await producer.beginTransaction()
  await transaction.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value'),
        partition: 0
      }
    ]
  })
  await transaction.commit()

  await rejects(
    async () => {
      await transaction.commit()
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'Cannot commit an already completed transaction.')
      return true
    }
  )
})

test('commit should reject requests from another transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })

  await producer.beginTransaction()
  const t2 = new Transaction(producer) // We can't use beginTransaction again as it would throw

  await rejects(
    async () => {
      await t2.commit()
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'The producer is in use by another transaction.')
      return true
    }
  )
})

test('commit should handle errors from ConnectionPool.get', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    strict: true
  })
  const testTopic = await createTopic(t, true)

  // Create a complete transaction
  const transaction = await producer.beginTransaction()
  await transaction.send({
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

  await rejects(
    async () => {
      await transaction.commit()
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('commit should handle unavailable API errors', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    strict: true
  })
  const testTopic = await createTopic(t, true)

  // Create a complete transaction
  const transaction = await producer.beginTransaction()
  await transaction.send({
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
      await transaction.commit()
    },
    error => {
      strictEqual(error instanceof UnsupportedApiError, true)
      strictEqual((error as Error).message.includes('Unsupported API EndTxn.'), true)
      return true
    }
  )
})

test('commit should handle errors from the API', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    strict: true
  })
  const testTopic = await createTopic(t, true)

  // Create a complete transaction
  const transaction = await producer.beginTransaction()
  await transaction.send({
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
      await transaction.commit()
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
      return true
    }
  )

  ok(producer.transaction)
})

test('commit should handle fencing errors from the API', async t => {
  const producer = createProducer(t, {
    idempotent: true,
    strict: true
  })
  const testTopic = await createTopic(t, true)

  // Create a complete transaction
  const transaction = await producer.beginTransaction()
  await transaction.send({
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
      await transaction.commit()
    },
    error => {
      strictEqual(error instanceof ProtocolError, true)
      return true
    }
  )

  ok(!producer.transaction)
})

test('abort should abort a transaction', async t => {
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
          operationId: mockedOperationId,
          transaction: context.transaction
        })
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'abort'
  )

  // Create a complete transaction
  const transaction = await producer.beginTransaction()
  await transaction.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value')
      }
    ]
  })

  await transaction.abort()

  ok(!producer.transaction)
  ok(transaction.completed)
  verifyTracingChannel()
})

test('abort should fail if aborted twice', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t, true)

  const transaction = await producer.beginTransaction()
  await transaction.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value')
      }
    ]
  })

  await transaction.abort()

  await rejects(
    async () => {
      await transaction.abort()
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'Cannot abort an already completed transaction.')
      return true
    }
  )
})

test('abort should reject requests from another transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })

  await producer.beginTransaction()
  const t2 = new Transaction(producer) // We can't use beginTransaction again as it would throw

  await rejects(
    async () => {
      await t2.abort()
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'The producer is in use by another transaction.')
      return true
    }
  )
})

// Other internal behaviors of abort are covered in the commit test since they share the same low level code.

test('cancel should reject requests from another transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })

  await producer.beginTransaction()
  const t2 = new Transaction(producer) // We can't use beginTransaction again as it would throw

  await rejects(
    async () => {
      await t2.cancel()
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'The producer is in use by another transaction.')
      return true
    }
  )
})

test('send should reject out-of transaction requests', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t)

  const transaction = await producer.beginTransaction()

  await rejects(
    async () => {
      await producer.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    error => {
      strictEqual(error instanceof UserError, true)
      deepStrictEqual((error as Error).message, 'The producer is in use by a transaction.')
      return true
    }
  )

  await transaction.cancel()

  await producer.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
    idempotent: true,
    acks: ProduceAcks.ALL
  })
})

test('send should reject requests from another transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t)

  const t1 = await producer.beginTransaction()
  const t2 = new Transaction(producer) // We can't use beginTransaction again as it would throw

  await rejects(
    async () => {
      await t2.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    error => {
      strictEqual(error instanceof UserError, true)
      deepStrictEqual((error as Error).message, 'The producer is in use by another transaction.')
      return true
    }
  )

  await t1.cancel()

  await t2.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
    idempotent: true,
    acks: ProduceAcks.ALL
  })
})

test('send should reject requests from a completed transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t)

  const transaction = await producer.beginTransaction()

  await transaction.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
    idempotent: true,
    acks: ProduceAcks.ALL
  })

  await transaction.commit()

  await rejects(
    async () => {
      await transaction.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    error => {
      strictEqual(error instanceof UserError, true)
      deepStrictEqual((error as Error).message, 'Cannot produce to an already completed transaction.')
      return true
    }
  )
})

test('send should handle errors from Base.metadata during a transaction (produce)', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t)

  const transaction = await producer.beginTransaction()

  mockMetadata(producer)

  await rejects(
    async () => {
      await transaction.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
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

  const transaction = await producer.beginTransaction()

  mockUnavailableAPI(producer, 'AddPartitionsToTxn')

  await rejects(
    async () => {
      await transaction.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    error => {
      strictEqual(error instanceof UnsupportedApiError, true)
      strictEqual((error as Error).message.includes('Unsupported API AddPartitionsToTxn.'), true)
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
  const testTopic = await createTopic(t, true)

  const transaction = await producer.beginTransaction()

  mockConnectionPoolGet(producer[kConnections], 2)

  await rejects(
    async () => {
      await transaction.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
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

  const transaction = await producer.beginTransaction()

  mockAPI(producer[kConnections], addPartitionsToTxnV5.api.key)

  await rejects(
    async () => {
      await transaction.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('addPartitions should reject requests from another transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t)

  const t1 = await producer.beginTransaction()
  const t2 = new Transaction(producer) // We can't use beginTransaction again as it would throw
  const metadata = await producer.metadata({ topics: [testTopic] })

  await rejects(
    async () => {
      await t2.addPartitions(metadata, [
        {
          topic: testTopic,
          value: Buffer.from('idempotent-message')
        }
      ])
    },
    error => {
      strictEqual(error instanceof UserError, true)
      deepStrictEqual((error as Error).message, 'The producer is in use by another transaction.')
      return true
    }
  )

  await t1.cancel()

  await t2.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
    idempotent: true,
    acks: ProduceAcks.ALL
  })
})

test('addPartitions should reject requests from a completed transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t)

  const transaction = await producer.beginTransaction()

  await transaction.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message'), partition: 0 }]
  })

  await transaction.commit()

  const metadata = await producer.metadata({ topics: [testTopic] })

  metadata.topics.get(testTopic)!.partitions.push(metadata.topics.get(testTopic)!.partitions[0])
  metadata.topics.get(testTopic)!.partitionsCount = 2

  await rejects(
    async () => {
      await transaction.addPartitions(metadata, [
        {
          topic: testTopic,
          value: Buffer.from('idempotent-message'),
          partition: 1
        }
      ])
    },
    error => {
      strictEqual(error instanceof UserError, true)
      deepStrictEqual((error as Error).message, 'Cannot add partitions to an already completed transaction.')
      return true
    }
  )
})

test('addConsumer should support multiple stream for the same group', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })

  const consumer = await createConsumer(t)
  await consumer.joinGroup()
  const transaction = await producer.beginTransaction()

  await transaction.addConsumer(consumer)
  await transaction.addConsumer(consumer)
})

test('addConsumer should reject requests from another transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t, true)

  await producer.send({ messages: [{ topic: testTopic, key: Buffer.from('key'), value: Buffer.from('value') }] })

  const consumer = await createConsumer(t, { autocommit: false })
  const transaction = await producer.beginTransaction()
  await transaction.addConsumer(consumer)

  const t2 = new Transaction(producer) // We can't use beginTransaction again as it would throw
  await rejects(
    async () => {
      await t2.addConsumer(consumer)
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'The producer is in use by another transaction.')
      return true
    }
  )
})

test('addConsumer should reject requests from a completed transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t, true)

  await producer.send({ messages: [{ topic: testTopic, key: Buffer.from('key'), value: Buffer.from('value') }] })

  const consumer = await createConsumer(t, { autocommit: false })

  const transaction = await producer.beginTransaction()

  await transaction.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message'), partition: 0 }]
  })

  await transaction.commit()

  await rejects(
    async () => {
      await transaction.addConsumer(consumer)
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'Cannot add a consumer to an already completed transaction.')
      return true
    }
  )
})

test('addConsumer should handle unavailable API errors', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })

  const consumer = await createConsumer(t)
  await consumer.joinGroup()
  const transaction = await producer.beginTransaction()

  mockUnavailableAPI(producer, 'AddOffsetsToTxn')

  await rejects(
    async () => {
      await transaction.addConsumer(consumer)
    },
    error => {
      strictEqual(error instanceof UnsupportedApiError, true)
      strictEqual((error as Error).message.includes('Unsupported API AddOffsetsToTxn.'), true)
      return true
    }
  )
})

test('addConsumer should handle errors from the API', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })

  const consumer = await createConsumer(t)
  await consumer.joinGroup()
  const transaction = await producer.beginTransaction()

  mockAPI(producer[kConnections], addOffsetsToTxnV4.api.key)

  await rejects(
    async () => {
      await transaction.addConsumer(consumer)
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('addConsumer should handle errors from ConnectionPool.get', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })

  const consumer = await createConsumer(t)
  await consumer.joinGroup()
  const transaction = await producer.beginTransaction()

  mockConnectionPoolGet(producer[kConnections], 1)

  await rejects(
    async () => {
      await transaction.addConsumer(consumer)
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('addOffset should fail when the message consumer group is not linked', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })

  const transaction = await producer.beginTransaction()

  await rejects(
    async () => {
      await transaction.addOffset({} as unknown as Message<string, string, string, string>)
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual(
        (error as Error).message,
        'Cannot add an offset to a transaction when the consumer group of the message has not been added to the transaction.'
      )
      return true
    }
  )
})

test('addOffset should reject requests from another transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t, true)

  await producer.send({ messages: [{ topic: testTopic, key: Buffer.from('key'), value: Buffer.from('value') }] })

  const consumer = await createConsumer(t, { autocommit: false })
  const stream = await consumer.consume({ topics: [testTopic], mode: MessagesStreamModes.EARLIEST })
  const [message] = await once(stream, 'data')
  const transaction = await producer.beginTransaction()
  await transaction.addConsumer(consumer)

  const t2 = new Transaction(producer) // We can't use beginTransaction again as it would throw
  const originalId = t2[kInstance]
  t2[kInstance] = transaction[kInstance] // Temporarily set same instance to pass the check
  await t2.addConsumer(consumer)
  t2[kInstance] = originalId

  await rejects(
    async () => {
      await t2.addOffset(message)
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'The producer is in use by another transaction.')
      return true
    }
  )
})

test('addOffset should reject requests from another transaction', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t, true)

  await producer.send({ messages: [{ topic: testTopic, key: Buffer.from('key'), value: Buffer.from('value') }] })

  const consumer = await createConsumer(t, { autocommit: false })
  const stream = await consumer.consume({ topics: [testTopic], mode: MessagesStreamModes.EARLIEST })
  const [message] = await once(stream, 'data')

  const transaction = await producer.beginTransaction()

  await transaction.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message'), partition: 0 }]
  })

  await transaction.commit()

  await rejects(
    async () => {
      await transaction.addOffset(message)
    },
    error => {
      strictEqual(error instanceof UserError, true)
      strictEqual((error as Error).message, 'Cannot add an offset to an already completed transaction.')
      return true
    }
  )
})

test('addOffset should handle errors from Base.metadata', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t, true)

  await producer.send({ messages: [{ topic: testTopic, key: Buffer.from('key'), value: Buffer.from('value') }] })

  const consumer = await createConsumer(t, { autocommit: false })
  const stream = await consumer.consume({ topics: [testTopic], mode: MessagesStreamModes.EARLIEST })
  const [message] = await once(stream, 'data')
  const transaction = await producer.beginTransaction()
  await transaction.addConsumer(consumer)

  mockMetadata(producer)

  await rejects(
    async () => {
      await transaction.addOffset(message)
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('addOffset should handle errors from ConnectionPool.get', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t, true)

  await producer.send({ messages: [{ topic: testTopic, key: Buffer.from('key'), value: Buffer.from('value') }] })

  const consumer = await createConsumer(t, { autocommit: false })
  const stream = await consumer.consume({ topics: [testTopic], mode: MessagesStreamModes.EARLIEST })
  const [message] = await once(stream, 'data')
  const transaction = await producer.beginTransaction()
  await transaction.addConsumer(consumer)

  mockConnectionPoolGet(producer[kConnections], 1)

  await rejects(
    async () => {
      await transaction.addOffset(message)
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('addOffset should handle unavailable API errors', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t, true)

  await producer.send({ messages: [{ topic: testTopic, key: Buffer.from('key'), value: Buffer.from('value') }] })

  const consumer = await createConsumer(t, { autocommit: false })
  const stream = await consumer.consume({ topics: [testTopic], mode: MessagesStreamModes.EARLIEST })
  const [message] = await once(stream, 'data')
  const transaction = await producer.beginTransaction()
  await transaction.addConsumer(consumer)

  mockUnavailableAPI(producer, 'TxnOffsetCommit')

  await rejects(
    async () => {
      await transaction.addOffset(message)
    },
    error => {
      strictEqual(error instanceof UnsupportedApiError, true)
      strictEqual((error as Error).message.includes('Unsupported API TxnOffsetCommit.'), true)
      return true
    }
  )
})

test('addOffset should handle errors from the API', async t => {
  const producer = createProducer(t, {
    strict: true,
    idempotent: true,
    transactionalId: randomUUID()
  })
  const testTopic = await createTopic(t, true)

  await producer.send({ messages: [{ topic: testTopic, key: Buffer.from('key'), value: Buffer.from('value') }] })

  const consumer = await createConsumer(t, { autocommit: false })
  const stream = await consumer.consume({ topics: [testTopic], mode: MessagesStreamModes.EARLIEST })
  const [message] = await once(stream, 'data')
  const transaction = await producer.beginTransaction()
  await transaction.addConsumer(consumer)

  mockAPI(producer[kConnections], txnOffsetCommitV4.api.key)

  await rejects(
    async () => {
      await transaction.addOffset(message)
    },
    error => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual((error as Error).message.includes(mockedErrorMessage), true)
      return true
    }
  )
})
