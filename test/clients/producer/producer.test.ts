import { deepStrictEqual, ok, rejects, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test, type TestContext } from 'node:test'
import { kConnections, kMetadata } from '../../../src/clients/base/base.ts'
import {
  type Broker,
  type Callback,
  type CallbackWithPromise,
  type Connection,
  MultipleErrors,
  NetworkError,
  ProduceAcks,
  Producer,
  type ProducerOptions,
  produceV11,
  ProtocolError,
  type ResponseParser,
  stringSerializer,
  stringSerializers,
  UserError,
  type Writer
} from '../../../src/index.ts'

const kafkaBootstrapServers = ['localhost:29092']

// Helper function to create a unique test client
function createTestProducer<K = Buffer, V = Buffer, HK = Buffer, HV = Buffer> (
  t: TestContext,
  overrideOptions: Partial<ProducerOptions<K, V, HK, HV>> = {}
) {
  const options: ProducerOptions<K, V, HK, HV> = {
    clientId: `test-client-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    autocreateTopics: true,
    ...overrideOptions
  }

  const client = new Producer<K, V, HK, HV>(options)
  t.after(() => client.close())

  return client
}

// Helper function to generate a unique topic name
function getTestTopicName () {
  return `test-topic-${randomUUID()}`
}

test('constructor should initialize properly', t => {
  const client = createTestProducer(t)

  strictEqual(client instanceof Producer, true)
  strictEqual(client.closed, false)

  client.close()
})

test('constructor should validate options in strict mode', t => {
  // Test with an invalid acks value
  try {
    createTestProducer(t, {
      strict: true,
      acks: 123 // Not a valid ProduceAcks enum value
    })
    throw new Error('Should have thrown for invalid acks value')
  } catch (error: any) {
    ok(error.message.includes('acks'), 'Error message should mention acks')
    ok(error.message.includes('/options/acks should be one of'), 'Error should indicate invalid enum value')
  }

  // Test with invalid compression
  try {
    createTestProducer(t, {
      strict: true,
      // @ts-expect-error - Intentionally passing invalid option
      compression: 'invalid-compression' // Not a valid compression algorithm
    })
    throw new Error('Should have thrown for invalid compression')
  } catch (error: any) {
    ok(error.message.includes('/options/compression should be one of'), 'Error message should mention compression')
  }

  // Test with invalid serializers type
  try {
    createTestProducer(t, {
      strict: true,
      // @ts-expect-error - Intentionally passing invalid option
      serializers: 'not-an-object'
    })
    throw new Error('Should have thrown for invalid serializers')
  } catch (error: any) {
    ok(error.message.includes('serializers'), 'Error message should mention serializers')
  }

  // Valid options should work without throwing
  const client = createTestProducer(t, {
    strict: true,
    acks: ProduceAcks.LEADER,
    compression: 'none',
    serializers: stringSerializers
  })
  strictEqual(client instanceof Producer, true)
  client.close()
})

test('should support both promise and callback API', (t, done) => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  // Use callback API
  client.send(
    {
      messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
      acks: ProduceAcks.LEADER
    },
    (err, result) => {
      strictEqual(err, null)
      ok(Array.isArray(result.offsets), 'Should have offsets array')
      strictEqual(result.offsets?.length, 1)
      strictEqual(result.offsets?.[0].topic, testTopic)
      strictEqual(typeof result.offsets?.[0].offset, 'bigint')

      // Clean up and close
      client
        .close()
        .then(() => done())
        .catch(done)
    }
  )
})

test('all operations should fail when client is closed', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  // Close the client first
  await client.close()

  // Attempt to send a message on a closed client
  await rejects(
    async () => {
      await client.send({
        messages: [{ topic: testTopic, value: Buffer.from('test-message') }]
      })
    },
    (error: any) => {
      strictEqual(error instanceof NetworkError, true)
      strictEqual(error.message, 'Client is closed.')
      return true
    }
  )

  // Attempt to initialize idempotent client on closed client
  await rejects(
    async () => {
      await client.initIdempotentProducer({})
    },
    (error: any) => {
      strictEqual(error instanceof NetworkError, true)
      strictEqual(error.message, 'Client is closed.')
      return true
    }
  )
})

test('initIdempotentProducer should set idempotent options correctly', async t => {
  // Create a client with idempotent=true
  const client = createTestProducer(t, {
    idempotent: true,
    strict: true
  })

  // Initialize and check if properly configured for idempotency
  const clientInfo = await client.initIdempotentProducer({})

  // Verify client info structure
  strictEqual(typeof clientInfo.producerId, 'bigint')
  strictEqual(typeof clientInfo.producerEpoch, 'number')
  ok(clientInfo.producerId >= 0n, 'Producer ID should be a positive bigint')
  ok(clientInfo.producerEpoch >= 0, 'Producer epoch should be a non-negative number')
})

test('initIdempotentProducer should validate options in strict mode', async t => {
  const producer = createTestProducer(t, { strict: true })

  // Test with invalid producerId type
  await rejects(
    async () => {
      await producer.initIdempotentProducer({
        // @ts-expect-error - Intentionally passing invalid option
        producerId: 'not-a-bigint'
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('producerId'), 'Error should mention producerId')
      return true
    }
  )

  // Test with invalid producerEpoch type
  await rejects(
    async () => {
      await producer.initIdempotentProducer({
        // @ts-expect-error - Intentionally passing invalid option
        producerEpoch: 'not-a-number'
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('producerEpoch'), 'Error should mention producerEpoch')
      return true
    }
  )

  // Test with invalid acks value
  await rejects(
    async () => {
      await producer.initIdempotentProducer({
        acks: 123 // Not a valid ProduceAcks enum value
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('acks'), 'Error should mention acks')
      return true
    }
  )

  // Test with invalid compression value
  await rejects(
    async () => {
      await producer.initIdempotentProducer({
        // @ts-expect-error - Intentionally passing invalid option
        compression: 'invalid-compression'
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('compression'), 'Error should mention compression')
      return true
    }
  )

  // Test with invalid partitioner type
  await rejects(
    async () => {
      await producer.initIdempotentProducer({
        // @ts-expect-error - Intentionally passing invalid option
        partitioner: 'not-a-function'
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('partitioner'), 'Error should mention partitioner')
      return true
    }
  )

  // Test with invalid additional property
  await rejects(
    async () => {
      await producer.initIdempotentProducer({
        // @ts-expect-error - Intentionally passing invalid options
        invalidProperty: true
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('additional properties'), 'Error should mention invalid property')
      return true
    }
  )

  // Valid options should succeed
  const producerInfo = await producer.initIdempotentProducer({
    acks: ProduceAcks.ALL,
    compression: 'none',
    repeatOnStaleMetadata: true
  })

  // Verify producer info structure
  strictEqual(typeof producerInfo.producerId, 'bigint')
  strictEqual(typeof producerInfo.producerEpoch, 'number')
  ok(producerInfo.producerId > 0n, 'Producer ID should be a positive bigint')
  ok(producerInfo.producerEpoch >= 0, 'Producer epoch should be a non-negative number')
})

test('initIdempotentProducer should handle errors from getFirstAvailable', async t => {
  const client = createTestProducer(t)

  client[kConnections].getFirstAvailable = (_brokers: any, callback: any) => {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined)
  }

  // Attempt to initialize idempotent client - should fail with connection error
  await rejects(
    async () => {
      await client.initIdempotentProducer({})
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes('Cannot connect to any broker.'), true)
      return true
    }
  )
})

test('initIdempotentProducer should handle errors from the API', async t => {
  const client = createTestProducer(t)

  client[kConnections].getFirstAvailable = (_brokers: any, callback: any) => {
    // Successfully return a connection, but the API call will fail
    const mockConnection = {
      send: (
        _apiKey: number,
        _apiVersion: number,
        _payload: any,
        _responseParser: any,
        _hasRequestHeaderTaggedFields: boolean,
        _hasResponseHeaderTaggedFields: boolean,
        apiCallback: any
      ) => {
        const connectionError = new MultipleErrors('API execution failed.', [
          new Error('Failed to execute initProducerId')
        ])
        apiCallback(connectionError, undefined)
      }
    }
    callback(null, mockConnection)
  }

  // Attempt to initialize idempotent client - should fail with API error
  await rejects(
    async () => {
      await client.initIdempotentProducer({})
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes('API execution failed.'), true)
      return true
    }
  )
})

test('send should return ProduceResult with offsets', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  // Produce a message
  const result = await client.send({
    messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
    acks: ProduceAcks.LEADER
  })

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.length, 1)
  strictEqual(result.offsets?.[0].topic, testTopic)
  strictEqual(typeof result.offsets?.[0].offset, 'bigint')
  strictEqual(typeof result.offsets?.[0].partition, 'number')
  ok(result.offsets?.[0].offset >= 0n, 'Offset should be a non-negative bigint')
})

test('send should support messages with keys', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  // Produce a message with a key
  const result = await client.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value')
      }
    ],
    acks: ProduceAcks.LEADER
  })

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.length, 1)
  strictEqual(result.offsets?.[0].topic, testTopic)
  strictEqual(typeof result.offsets?.[0].offset, 'bigint')
})

test('send should support messages with headers', async t => {
  const testTopic = getTestTopicName()

  // Produce a message with headers using Map
  const client1 = createTestProducer(t)
  const result1 = await client1.send({
    messages: [
      {
        topic: testTopic,
        value: Buffer.from('message-with-map-headers'),
        headers: new Map([
          [Buffer.from('header-key-1'), Buffer.from('header-value-1')],
          [Buffer.from('header-key-2'), Buffer.from('header-value-2')]
        ])
      }
    ],
    acks: ProduceAcks.LEADER
  })

  // Verify result structure
  ok(Array.isArray(result1.offsets), 'Should have offsets array')
  strictEqual(result1.offsets?.length, 1)

  // Produce a message with headers using object
  const client2 = createTestProducer(t, {
    serializers: { headerKey: stringSerializer }
  })

  const result2 = await client2.send({
    messages: [
      {
        topic: testTopic,
        value: Buffer.from('message-with-object-headers'),
        headers: {
          'header-key-1': Buffer.from('header-value-1'),
          'header-key-2': Buffer.from('header-value-2')
        }
      }
    ],
    acks: ProduceAcks.LEADER
  })

  // Verify result structure
  ok(Array.isArray(result2.offsets), 'Should have offsets array')
  strictEqual(result2.offsets?.length, 1)
})

test('send should support no response', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  // Produce a message
  const result = await client.send({
    messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
    acks: ProduceAcks.NO_RESPONSE
  })

  deepStrictEqual(result, { unwritableNodes: [] })
})

test('send should support no response with backpressure handling', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  const original = client[kConnections].get.bind(client[kConnections])
  client[kConnections].get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    original(broker, (error: Error | null, connection: Connection) => {
      if (error) {
        callback(error, undefined as unknown as Connection)
        return
      }

      const originalSend = connection.send.bind(connection)

      connection.send = function <ReturnType>(
        apiKey: number,
        apiVersion: number,
        payload: () => Writer,
        responseParser: ResponseParser<ReturnType>,
        hasRequestHeaderTaggedFields: boolean,
        hasResponseHeaderTaggedFields: boolean,
        callback: Callback<ReturnType>
      ) {
        if (apiKey === produceV11.api.key) {
          callback(null, false as ReturnType)
          return
        }

        originalSend(
          apiKey,
          apiVersion,
          payload,
          responseParser,
          hasRequestHeaderTaggedFields,
          hasResponseHeaderTaggedFields,
          callback
        )
      } as typeof originalSend

      callback(null, connection)
    })
  } as typeof original

  // Produce a message
  const result = await client.send({
    messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
    acks: ProduceAcks.NO_RESPONSE
  })

  strictEqual(result.unwritableNodes!.length, 1)
})

test('send should support string serialization with provided serializers', async t => {
  const client = createTestProducer<string, string, string, string>(t, { serializers: stringSerializers })

  const testTopic = getTestTopicName()

  // Produce a message with string key, value and headers
  const result = await client.send({
    messages: [
      {
        topic: testTopic,
        key: 'key1',
        value: 'value1',
        headers: {
          headerKey1: 'headerValue1',
          headerKey2: 'headerValue2'
        }
      }
    ],
    acks: ProduceAcks.LEADER
  })

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.length, 1)
  strictEqual(result.offsets?.[0].topic, testTopic)
  strictEqual(typeof result.offsets?.[0].offset, 'bigint')
})

test('send should support specifying a partition', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  // Produce a message to a specific partition
  const result = await client.send({
    messages: [{ topic: testTopic, partition: 0, value: Buffer.from('test-message') }],
    acks: ProduceAcks.LEADER
  })

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.length, 1)
  strictEqual(result.offsets?.[0].topic, testTopic)
  strictEqual(result.offsets?.[0].partition, 0, 'Should be sent to partition 0')
})

test('send should handle custom partitioning function', async t => {
  // Create a client with a custom partitioner
  const client = createTestProducer(t, {
    // Always use partition 0 regardless of input
    partitioner: () => 0
  })
  const testTopic = getTestTopicName()

  // Send a message - should use our partitioner
  const result = await client.send({
    messages: [{ topic: testTopic, value: Buffer.from('test-message') }]
  })

  // Verify the message went to partition 0
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.[0].partition, 0, 'Should be assigned to partition 0')
})

test('send should support sending to multiple topics', async t => {
  const client = createTestProducer(t)
  const testTopic1 = getTestTopicName()
  const testTopic2 = getTestTopicName()

  // Send to multiple topics in one request
  const result = await client.send({
    messages: [
      { topic: testTopic1, value: Buffer.from('topic1-message') },
      { topic: testTopic2, value: Buffer.from('topic2-message') }
    ]
  })

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')

  strictEqual(result.offsets?.length, 2)

  // Find offset for each topic
  const topic1Offset = result.offsets?.find(o => o.topic === testTopic1)
  const topic2Offset = result.offsets?.find(o => o.topic === testTopic2)

  ok(topic1Offset, `Should have offset for topic ${testTopic1}`)
  ok(topic2Offset, `Should have offset for topic ${testTopic2}`)
})

test('send should initialize idempotent client', async t => {
  const client = createTestProducer(t)

  // Initialize the idempotent client explicitly
  const clientInfo = await client.initIdempotentProducer({})

  // Verify client info structure
  strictEqual(typeof clientInfo.producerId, 'bigint')
  strictEqual(typeof clientInfo.producerEpoch, 'number')
  ok(clientInfo.producerId > 0n, 'Producer ID should be a positive bigint')
  ok(clientInfo.producerEpoch >= 0, 'Producer epoch should be a non-negative number')

  // Send with idempotent client
  const testTopic = getTestTopicName()
  const result = await client.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
    idempotent: true,
    acks: ProduceAcks.ALL
  })

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.length, 1)
})

test('send should auto-initialize idempotent client if needed', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  // Send with idempotent=true without initializing first
  const result = await client.send({
    messages: [{ topic: testTopic, value: Buffer.from('auto-init-idempotent-message') }],
    idempotent: true,
    acks: ProduceAcks.ALL
  })

  // Verify client info structure
  ok(client.producerId! > 0n, 'Producer ID should be a positive bigint')
  ok(client.producerEpoch! >= 0, 'Producer epoch should be a non-negative number')

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.length, 1)
})

test('send should validate options in strict mode', async t => {
  const client = createTestProducer(t, { strict: true })
  const testTopic = getTestTopicName()

  // Test with missing required field (messages)
  await rejects(
    async () => {
      // @ts-expect-error - Intentionally passing invalid options
      await client.send({})
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('messages'), 'Error should mention missing messages')
      return true
    }
  )

  // Test with invalid messages type
  await rejects(
    async () => {
      // @ts-expect-error - Intentionally passing invalid options
      await client.send({ messages: 'not-an-array' })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('messages'), 'Error should mention invalid messages type')
      return true
    }
  )

  // Test with invalid acks value
  await rejects(
    async () => {
      await client.send({
        messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
        acks: 123 // Not a valid ProduceAcks enum value
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('acks'), 'Error should mention invalid acks')
      return true
    }
  )

  // Test with invalid message (missing topic)
  await rejects(
    async () => {
      await client.send({
        // @ts-expect-error - Intentionally passing invalid options
        messages: [{ value: Buffer.from('test-message') }]
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('topic'), 'Error should mention missing topic')
      return true
    }
  )

  // Test with invalid additional property
  await rejects(
    async () => {
      await client.send({
        messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
        // @ts-expect-error - Intentionally passing invalid options
        invalidProperty: true
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      ok(error.message.includes('additional properties'), 'Error should mention invalid property')
      return true
    }
  )
})

test('send should reject conflicting idempotent client options', async t => {
  const client = createTestProducer(t, { idempotent: true })
  const testTopic = getTestTopicName()

  // Try to send with custom clientId (should fail)
  await rejects(
    async () => {
      await client.send({
        messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
        idempotent: true,
        producerId: 123n
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      strictEqual(error.message, 'Cannot specify producerId or producerEpoch when using idempotent producer.')
      return true
    }
  )

  // Try to send with custom clientEpoch (should fail)
  await rejects(
    async () => {
      await client.send({
        messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
        idempotent: true,
        producerEpoch: 1
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      strictEqual(error.message, 'Cannot specify producerId or producerEpoch when using idempotent producer.')
      return true
    }
  )

  // Try to send with wrong acks value (should fail)
  await rejects(
    async () => {
      await client.send({
        messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
        idempotent: true,
        acks: ProduceAcks.LEADER
      })
    },
    (error: any) => {
      strictEqual(error instanceof UserError, true)
      strictEqual(error.message, 'Idempotent producer requires acks to be ALL (-1).')
      return true
    }
  )

  // Should succeed with correct idempotent client settings
  const result = await client.send({
    messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
    idempotent: true
  })

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.length, 1)
})

test('send should handle errors from initIdempotentProducer', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  // This will result in errors inside initIdempotentProducer
  client[kConnections].getFirstAvailable = (_brokers: any, callback: any) => {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined)
  }

  // Attempt to initialize idempotent client - should fail with connection error
  await rejects(
    async () => {
      await client.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes('Cannot connect to any broker.'), true)
      return true
    }
  )
})

test('send should handle errors from Base.metadata', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  // Override the [kMetadata] method to simulate a connection error
  client[kMetadata] = (_options: any, callback: CallbackWithPromise<any>) => {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined)
  }

  // Attempt to initialize idempotent client - should fail with connection error
  await rejects(
    async () => {
      await client.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes('Cannot connect to any broker.'), true)
      return true
    }
  )
})

test('send should handle errors from Base.metadata in internal calls', async t => {
  const client = createTestProducer(t, {})
  const testTopic = getTestTopicName()

  // Save the original [kMetadata] method to restore it later
  const original = client[kMetadata].bind(client)

  // Override the [kMetadata] method to simulate a connection error
  let firstCall = true
  client[kMetadata] = (options: any, callback: CallbackWithPromise<any>) => {
    if (firstCall) {
      firstCall = false
      original(options, callback)
      return
    }

    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined)
  }

  // Attempt to initialize idempotent client - should fail with connection error
  await rejects(
    async () => {
      await client.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes('Producing messages failed.'), true)
      return true
    }
  )
})

test('send should handle errors from ConnectionPool.get', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  // Save original method
  const original = client[kConnections].get.bind(client[kConnections])

  let getCalls = 0
  client[kConnections].get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    getCalls++

    if (getCalls < 4) {
      original(broker, callback)
      return
    }

    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined as unknown as Connection)
  } as typeof original

  // Attempt to initialize idempotent client - should fail with connection error
  await rejects(
    async () => {
      await client.send({
        messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
        idempotent: true,
        acks: ProduceAcks.ALL
      })
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes('Producing messages failed.'), true)
      return true
    }
  )
})

test('send should repeat the operation in case of stale metadata', async t => {
  const client = createTestProducer(t)
  const testTopic = getTestTopicName()

  let firstCall = true
  const original = client[kConnections].get.bind(client[kConnections])
  client[kConnections].get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    original(broker, (error: Error | null, connection: Connection) => {
      if (error) {
        callback(error, undefined as unknown as Connection)
        return
      }

      const originalSend = connection.send.bind(connection)

      connection.send = function <ReturnType>(
        apiKey: number,
        apiVersion: number,
        payload: () => Writer,
        responseParser: ResponseParser<ReturnType>,
        hasRequestHeaderTaggedFields: boolean,
        hasResponseHeaderTaggedFields: boolean,
        callback: Callback<ReturnType>
      ) {
        if (apiKey === produceV11.api.key && firstCall) {
          firstCall = false
          callback(new ProtocolError('UNKNOWN_TOPIC_OR_PARTITION', { topic: 'test-topic' }), undefined as ReturnType)
          return
        }

        originalSend(
          apiKey,
          apiVersion,
          payload,
          responseParser,
          hasRequestHeaderTaggedFields,
          hasResponseHeaderTaggedFields,
          callback
        )
      } as typeof originalSend

      callback(null, connection)
    })
  } as typeof original

  await client.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
    idempotent: true,
    acks: ProduceAcks.ALL
  })
})
