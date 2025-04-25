import { deepStrictEqual, ok, rejects, strictEqual } from 'node:assert'
import { test } from 'node:test'
import * as Prometheus from 'prom-client'
import { kConnections } from '../../../src/clients/base/base.ts'
import {
  initProducerIdV5,
  MultipleErrors,
  NetworkError,
  ProduceAcks,
  Producer,
  produceV11,
  ProtocolError,
  stringSerializer,
  stringSerializers,
  UserError
} from '../../../src/index.ts'
import {
  createProducer,
  createTopic,
  mockAPI,
  mockConnectionPoolGet,
  mockConnectionPoolGetFirstAvailable,
  mockedErrorMessage,
  mockMetadata,
  mockMethod
} from '../../helpers.ts'

test('constructor should initialize properly', t => {
  const producer = createProducer(t)

  strictEqual(producer instanceof Producer, true)
  strictEqual(producer.closed, false)
})

test('constructor should validate options in strict mode', t => {
  // Test with an invalid acks value
  try {
    createProducer(t, {
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
    createProducer(t, {
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
    createProducer(t, {
      strict: true,
      // @ts-expect-error - Intentionally passing invalid option
      serializers: 'not-an-object'
    })
    throw new Error('Should have thrown for invalid serializers')
  } catch (error: any) {
    ok(error.message.includes('serializers'), 'Error message should mention serializers')
  }

  // Valid options should work without throwing
  const producer = createProducer(t, {
    strict: true,
    acks: ProduceAcks.LEADER,
    compression: 'none',
    serializers: stringSerializers
  })
  strictEqual(producer instanceof Producer, true)
  producer.close()
})

test('close should properly clean up resources and set closed state', t => {
  return new Promise<void>((resolve, reject) => {
    const producer = createProducer(t)

    // Verify initial state
    strictEqual(producer.closed, false)

    // Close the producer
    producer.close(err => {
      if (err) {
        reject(err)
        return
      }

      // Verify closed state
      strictEqual(producer.closed, true)
      resolve()
    })
  })
})

test('close should handle errors from Base.close', async t => {
  const producer = createProducer(t)

  // Mock the super.close method to fail
  mockMethod(producer[kConnections], 'close')

  // Attempt to close with the mocked error
  try {
    await producer.close()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('should support both promise and callback API', async t => {
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

  await new Promise((resolve, reject) => {
    // Use callback API
    producer.send(
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
        producer.close().then(resolve).catch(reject)
      }
    )
  })
})

test('all operations should fail when producer is closed', async t => {
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

  // Close the producer first
  await producer.close()

  // Attempt to send a message on a closed producer
  await rejects(
    async () => {
      await producer.send({
        messages: [{ topic: testTopic, value: Buffer.from('test-message') }]
      })
    },
    (error: any) => {
      strictEqual(error instanceof NetworkError, true)
      strictEqual(error.message, 'Client is closed.')
      return true
    }
  )

  // Attempt to initialize idempotent producer on closed producer
  await rejects(
    async () => {
      await producer.initIdempotentProducer({})
    },
    (error: any) => {
      strictEqual(error instanceof NetworkError, true)
      strictEqual(error.message, 'Client is closed.')
      return true
    }
  )
})

test('initIdempotentProducer should set idempotent options correctly', async t => {
  // Create a producer with idempotent=true
  const producer = createProducer(t, {
    idempotent: true,
    strict: true
  })

  // Initialize and check if properly configured for idempotency
  const clientInfo = await producer.initIdempotentProducer({})

  // Verify producer info structure
  strictEqual(typeof clientInfo.producerId, 'bigint')
  strictEqual(typeof clientInfo.producerEpoch, 'number')
  ok(clientInfo.producerId >= 0n, 'Producer ID should be a positive bigint')
  ok(clientInfo.producerEpoch >= 0, 'Producer epoch should be a non-negative number')
})

test('initIdempotentProducer should validate options in strict mode', async t => {
  const producer = createProducer(t, { strict: true })

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
  const producer = createProducer(t)

  mockConnectionPoolGetFirstAvailable(producer[kConnections])

  // Attempt to initialize idempotent producer - should fail with connection error
  await rejects(
    async () => {
      await producer.initIdempotentProducer({})
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('initIdempotentProducer should handle errors from the API', async t => {
  const producer = createProducer(t)

  mockAPI(producer[kConnections], initProducerIdV5.api.key)

  // Attempt to initialize idempotent producer - should fail with API error
  await rejects(
    async () => {
      await producer.initIdempotentProducer({})
    },
    (error: any) => {
      strictEqual(error instanceof MultipleErrors, true)
      strictEqual(error.message.includes(mockedErrorMessage), true)
      return true
    }
  )
})

test('send should return ProduceResult with offsets', async t => {
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

  // Produce a message
  const result = await producer.send({
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
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

  // Produce a message with a key
  const result = await producer.send({
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
  const testTopic = await createTopic(t)

  // Produce a message with headers using Map
  const client1 = createProducer(t)
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
  const client2 = createProducer(t, {
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
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

  // Produce a message
  const result = await producer.send({
    messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
    acks: ProduceAcks.NO_RESPONSE
  })

  deepStrictEqual(result, { unwritableNodes: [] })
})

test('send should support no response with backpressure handling', async t => {
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

  mockAPI(producer[kConnections], produceV11.api.key, null, false)

  // Produce a message
  const result = await producer.send({
    messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
    acks: ProduceAcks.NO_RESPONSE
  })

  strictEqual(result.unwritableNodes!.length, 1)
})

test('send should support string serialization with provided serializers', async t => {
  const producer = createProducer<string, string, string, string>(t, { serializers: stringSerializers })

  const testTopic = await createTopic(t)

  // Produce a message with string key, value and headers
  const result = await producer.send({
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
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

  // Produce a message to a specific partition
  const result = await producer.send({
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
  // Create a producer with a custom partitioner
  const producer = createProducer(t, {
    // Always use partition 0 regardless of input
    partitioner: () => 0
  })
  const testTopic = await createTopic(t)

  // Send a message - should use our partitioner
  const result = await producer.send({
    messages: [{ topic: testTopic, value: Buffer.from('test-message') }]
  })

  // Verify the message went to partition 0
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.[0].partition, 0, 'Should be assigned to partition 0')
})

test('send should support sending to multiple topics', async t => {
  const producer = createProducer(t)
  const testTopic1 = await createTopic(t)
  const testTopic2 = await createTopic(t)

  // Send to multiple topics in one request
  const result = await producer.send({
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

test('send should initialize idempotent producer', async t => {
  const producer = createProducer(t)

  // Initialize the idempotent producer explicitly
  const clientInfo = await producer.initIdempotentProducer({})

  // Verify producer info structure
  strictEqual(typeof clientInfo.producerId, 'bigint')
  strictEqual(typeof clientInfo.producerEpoch, 'number')
  ok(clientInfo.producerId > 0n, 'Producer ID should be a positive bigint')
  ok(clientInfo.producerEpoch >= 0, 'Producer epoch should be a non-negative number')

  // Send with idempotent producer
  const testTopic = await createTopic(t)
  const result = await producer.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
    idempotent: true,
    acks: ProduceAcks.ALL
  })

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.length, 1)
})

test('send should auto-initialize idempotent producer if needed', async t => {
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

  // Send with idempotent=true without initializing first
  const result = await producer.send({
    messages: [{ topic: testTopic, value: Buffer.from('auto-init-idempotent-message') }],
    idempotent: true,
    acks: ProduceAcks.ALL
  })

  // Verify producer info structure
  ok(producer.producerId! > 0n, 'Producer ID should be a positive bigint')
  ok(producer.producerEpoch! >= 0, 'Producer epoch should be a non-negative number')

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.length, 1)
})

test('send should validate options in strict mode', async t => {
  const producer = createProducer(t, { strict: true })
  const testTopic = await createTopic(t)

  // Test with missing required field (messages)
  await rejects(
    async () => {
      // @ts-expect-error - Intentionally passing invalid options
      await producer.send({})
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
      await producer.send({ messages: 'not-an-array' })
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
      await producer.send({
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
      await producer.send({
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
      await producer.send({
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

test('send should reject conflicting idempotent producer options', async t => {
  const producer = createProducer(t, { idempotent: true })
  const testTopic = await createTopic(t)

  // Try to send with custom clientId (should fail)
  await rejects(
    async () => {
      await producer.send({
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
      await producer.send({
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
      await producer.send({
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

  // Should succeed with correct idempotent producer settings
  const result = await producer.send({
    messages: [{ topic: testTopic, value: Buffer.from('test-message') }],
    idempotent: true
  })

  // Verify result structure
  ok(Array.isArray(result.offsets), 'Should have offsets array')
  strictEqual(result.offsets?.length, 1)
})

test('send should handle errors from initIdempotentProducer', async t => {
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

  mockConnectionPoolGetFirstAvailable(producer[kConnections])

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

test('send should handle errors from Base.metadata', async t => {
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

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

test('send should handle errors from Base.metadata in internal calls', async t => {
  const producer = createProducer(t, {})
  const testTopic = await createTopic(t)

  mockMetadata(producer, 2)

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
      strictEqual(error.message.includes('Producing messages failed.'), true)
      return true
    }
  )
})

test('send should handle errors from ConnectionPool.get', async t => {
  const producer = createProducer(t)
  const testTopic = await createTopic(t)

  mockConnectionPoolGet(producer[kConnections], 4)

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
      strictEqual(error.message.includes('Producing messages failed.'), true)
      return true
    }
  )
})

test('send should repeat the operation in case of stale metadata', async t => {
  const producer = createProducer(t)
  const testTopic = await createTopic(t, true)

  mockAPI(
    producer[kConnections],
    produceV11.api.key,
    new ProtocolError('UNKNOWN_TOPIC_OR_PARTITION', { topic: testTopic })
  )

  await producer.send({
    messages: [{ topic: testTopic, value: Buffer.from('idempotent-message') }],
    idempotent: true,
    acks: ProduceAcks.ALL
  })
})

test('metrics should track the number of active consumers', async t => {
  const registry = new Prometheus.Registry()

  const producer1 = await createProducer(t, { metrics: { registry, client: Prometheus } })

  {
    const metrics = await registry.getMetricsAsJSON()
    const activeConsumers = metrics.find(m => m.name === 'kafka_producers')!

    deepStrictEqual(activeConsumers, {
      aggregator: 'sum',
      help: 'Number of active Kafka producers',
      name: 'kafka_producers',
      type: 'gauge',
      values: [
        {
          labels: {},
          value: 1
        }
      ]
    })
  }

  const producer2 = await createProducer(t, { metrics: { registry, client: Prometheus } })

  {
    const metrics = await registry.getMetricsAsJSON()
    const activeConsumers = metrics.find(m => m.name === 'kafka_producers')!
    deepStrictEqual(activeConsumers.values[0].value, 2)
  }

  await producer2.close()

  {
    const metrics = await registry.getMetricsAsJSON()
    const activeConsumers = metrics.find(m => m.name === 'kafka_producers')!
    deepStrictEqual(activeConsumers.values[0].value, 1)
  }

  await producer1.close()

  {
    const metrics = await registry.getMetricsAsJSON()
    const activeConsumers = metrics.find(m => m.name === 'kafka_producers')!
    deepStrictEqual(activeConsumers.values[0].value, 0)
  }
})

test('metrics should track the number of produced messages', async t => {
  const registry = new Prometheus.Registry()
  const producer1 = createProducer(t, { metrics: { registry, client: Prometheus } })
  const producer2 = createProducer(t, { metrics: { registry, client: Prometheus } })
  const testTopic = await createTopic(t)

  await producer1.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value')
      },
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value')
      },
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value')
      }
    ],
    acks: ProduceAcks.LEADER
  })

  {
    const metrics = await registry.getMetricsAsJSON()
    const producedMessages = metrics.find(m => m.name === 'kafka_produced_messages')!

    deepStrictEqual(producedMessages, {
      aggregator: 'sum',
      help: 'Number of produced Kafka messages',
      name: 'kafka_produced_messages',
      type: 'counter',
      values: [
        {
          labels: {},
          value: 3
        }
      ]
    })
  }

  await producer2.send({
    messages: [
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value')
      },
      {
        topic: testTopic,
        key: Buffer.from('message-key'),
        value: Buffer.from('message-value')
      }
    ],
    acks: ProduceAcks.LEADER
  })

  {
    const metrics = await registry.getMetricsAsJSON()
    const producedMessages = metrics.find(m => m.name === 'kafka_produced_messages')!

    deepStrictEqual(producedMessages.values[0].value, 5)
  }
})
