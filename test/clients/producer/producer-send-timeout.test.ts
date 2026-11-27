import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { test, type TestContext } from 'node:test'
import { kGetApi, kGetBootstrapConnection, kGetConnection, kMetadata } from '../../../src/clients/base/base.ts'
import type { ClusterMetadata } from '../../../src/clients/base/types.ts'
import {
  type ClientDiagnosticEvent,
  type Connection,
  NetworkError,
  ProduceAcks,
  Producer,
  type ProducerOptions,
  producerSendsChannel,
  ProtocolError,
  TimeoutError,
  UserError
} from '../../../src/index.ts'
import { mockMethod } from '../../helpers.ts'

type ConnectionCallback = (error: Error | null, connection?: Connection) => void
type ResponseCallback = (error: Error | null, response?: unknown) => void

function respondWithOffset (callback: ResponseCallback): void {
  callback(null, {
    throttleTimeMs: 0,
    responses: [{ name: 'test', partitionResponses: [{ index: 0, errorCode: 0, baseOffset: 10n }] }]
  })
}

// A producer whose metadata, connections and Produce API are mocked, so that each test decides when they answer
interface Harness {
  producer: Producer
  connect: (callback: ConnectionCallback) => void
  respond: (callback: ResponseCallback) => void
  writes: number
  producerIds: number
  // Called each time the Produce API has been obtained, once the request was written or not
  onApi: () => void
}

function createMetadata (): ClusterMetadata {
  return {
    id: 'cluster',
    brokers: new Map([[1, { host: '127.0.0.1', port: 9092, rack: null }]]),
    controllerId: 1,
    lastUpdate: Date.now(),
    topics: new Map([
      [
        'test',
        {
          id: 'test',
          partitions: [{ leader: 1, leaderEpoch: 0, replicas: [1], isr: [1], offlineReplicas: [] }],
          partitionsCount: 1,
          lastUpdate: Date.now()
        }
      ]
    ])
  }
}

function createHarness (t: TestContext, options: Partial<ProducerOptions<Buffer, Buffer, Buffer, Buffer>> = {}): Harness {
  const producer = new Producer({
    clientId: 'send-timeout',
    bootstrapBrokers: ['127.0.0.1:9092'],
    strict: true,
    retries: 100,
    retryDelay: 10,
    ...options
  })

  t.after(() => producer.close(true))

  const harness: Harness = {
    producer,
    connect: () => {},
    respond: respondWithOffset,
    writes: 0,
    producerIds: 0,
    onApi: () => {}
  }

  mockMethod(producer, kMetadata, () => true, null, undefined, (_, ...args) => {
    args.at(-1)(null, createMetadata())
    return true
  })

  mockMethod(producer, kGetConnection, () => true, null, undefined, (_, ...args) => {
    harness.connect(args.at(-1))
    return true
  })

  mockMethod(producer, kGetBootstrapConnection, () => true, null, undefined, (_, callback) => {
    callback(null, {} as Connection)
    return true
  })

  mockMethod(producer, kGetApi, () => true, null, undefined, (_, name, callback) => {
    if (name === 'InitProducerId') {
      harness.producerIds++

      const initProducerId = (...apiArgs: unknown[]) => {
        ;(apiArgs.at(-1) as (error: Error | null, response: unknown) => void)(null, {
          producerId: 1n,
          producerEpoch: 0
        })
      }

      callback(null, Object.assign(initProducerId, { version: 0 }))
      return true
    }

    callback(null, (...apiArgs: unknown[]) => {
      harness.writes++
      harness.respond(apiArgs.at(-1) as ResponseCallback)
    })

    harness.onApi()
    return true
  })

  return harness
}

function message (value: string) {
  return { topic: 'test', partition: 0, value: Buffer.from(value) }
}

function isTimeout (error: unknown): boolean {
  return error instanceof TimeoutError
}

// Connection attempts wait until release() is called
function holdConnections (harness: Harness): () => void {
  const held: ConnectionCallback[] = []

  harness.connect = callback => {
    held.push(callback)
  }

  return () => {
    harness.connect = callback => callback(null, {} as Connection)

    for (const callback of held.splice(0)) {
      callback(null, {} as Connection)
    }
  }
}

test('send() should fail with a TimeoutError once the sendTimeout of the producer has passed', async t => {
  const harness = createHarness(t, { sendTimeout: 50 })
  holdConnections(harness)

  await rejects(harness.producer.send({ messages: [message('a')] }), isTimeout)
})

test('send() should use its own sendTimeout over the one of the producer', async t => {
  const harness = createHarness(t, { sendTimeout: 60_000 })
  holdConnections(harness)

  await rejects(harness.producer.send({ messages: [message('a')], sendTimeout: 50 }), isTimeout)
})

test('send() should resolve as usual when the messages are produced in time', async t => {
  const harness = createHarness(t)
  harness.connect = callback => callback(null, {} as Connection)

  const result = await harness.producer.send({ messages: [message('a')], sendTimeout: 10_000 })

  deepStrictEqual(result.offsets, [{ topic: 'test', partition: 0, offset: 10n }])
})

test('send() should not retry once it has timed out', async t => {
  let produceRetries = 0

  const harness = createHarness(t, {
    retryDelay: (_client, operationId) => {
      if (operationId === 'produce') {
        produceRetries++
      }

      return 10
    }
  })

  const held: ConnectionCallback[] = []
  harness.connect = callback => {
    held.push(callback)
  }

  await rejects(harness.producer.send({ messages: [message('a')], sendTimeout: 50 }), isTimeout)

  // The attempt which was in progress fails with a retriable error, and whether to retry is decided at once
  held[0](new NetworkError('Connection refused.'))

  strictEqual(produceRetries, 0)
})

test('send() should not start an attempt once it has timed out', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  t.after(() => t.mock.timers.reset())

  const harness = createHarness(t, { retryDelay: 100 })
  let connections = 0

  harness.connect = callback => {
    connections++
    callback(new NetworkError('Connection refused.'))
  }

  const sent = harness.producer.send({ messages: [message('a')], sendTimeout: 50 })

  // The send times out while waiting to retry, then the retry is due
  t.mock.timers.tick(50)
  await rejects(sent, isTimeout)
  t.mock.timers.tick(50)

  strictEqual(connections, 1)
})

test('send() should not write a Produce request once it has timed out', async t => {
  const harness = createHarness(t)
  const release = holdConnections(harness)
  const { promise: apiObtained, resolve } = Promise.withResolvers<void>()
  harness.onApi = resolve

  await rejects(harness.producer.send({ messages: [message('a')], sendTimeout: 50 }), isTimeout)

  release()
  await apiObtained

  strictEqual(harness.writes, 0)
})

test('send() with linger should only fail the calls whose sendTimeout has passed', async t => {
  const harness = createHarness(t, { linger: 5 })
  let shortTimedOut = false

  // The request fails until the call with the short sendTimeout has failed: it must keep retrying for the other one
  harness.connect = callback => {
    if (shortTimedOut) {
      callback(null, {} as Connection)
    } else {
      callback(new NetworkError('Connection refused.'))
    }
  }

  const short = harness.producer.send({ messages: [message('a')], sendTimeout: 50 })
  const unbounded = harness.producer.send({ messages: [message('b')] })

  await rejects(short, isTimeout)
  shortTimedOut = true

  const result = await unbounded

  // The messages of both calls were in the same request, the one of the second call comes after the first one
  deepStrictEqual(result.offsets, [{ topic: 'test', partition: 0, offset: 11n }])
  strictEqual(harness.writes, 1)
})

test('send() with linger should not write a Produce request once all the calls have timed out', async t => {
  const harness = createHarness(t, { linger: 5 })
  const release = holdConnections(harness)
  const { promise: apiObtained, resolve } = Promise.withResolvers<void>()
  harness.onApi = resolve

  const first = harness.producer.send({ messages: [message('a')], sendTimeout: 30 })
  const second = harness.producer.send({ messages: [message('b')], sendTimeout: 60 })

  await rejects(first, isTimeout)
  await rejects(second, isTimeout)

  release()
  await apiObtained

  strictEqual(harness.writes, 0)
})

test('send() with orderedSends should not write a queued request whose calls have timed out', async t => {
  const harness = createHarness(t, { orderedSends: true })
  const release = holdConnections(harness)
  let apis = 0

  harness.onApi = () => {
    apis++
  }

  // The second call waits for the first one, which writes to the same partition
  const first = harness.producer.send({ messages: [message('a')] })
  const second = harness.producer.send({ messages: [message('b')], sendTimeout: 50 })

  await rejects(second, isTimeout)

  release()

  // The request of the second call is abandoned once the first one completes, so the Produce API is obtained once
  deepStrictEqual((await first).offsets, [{ topic: 'test', partition: 0, offset: 10n }])

  strictEqual(apis, 1)
  strictEqual(harness.writes, 1)
})

test('send() with an idempotent producer should keep it when a queued request is abandoned', async t => {
  const harness = createHarness(t, { idempotent: true })
  const release = holdConnections(harness)

  const first = harness.producer.send({ messages: [message('a')] })
  const second = harness.producer.send({ messages: [message('b')], sendTimeout: 50 })

  await rejects(second, isTimeout)

  release()
  await first

  // Had the abandoned request reserved sequences, the producer would have been reset and initialized again
  await harness.producer.send({ messages: [message('c')] })

  strictEqual(harness.producerIds, 1)
  strictEqual(harness.writes, 2)
})

test('send() with an idempotent producer should not recover it for a request whose calls have timed out', async t => {
  const harness = createHarness(t, { idempotent: true })
  harness.connect = callback => callback(null, {} as Connection)

  const responses: ResponseCallback[] = []
  harness.respond = callback => {
    responses.push(callback)
  }

  await rejects(harness.producer.send({ messages: [message('a')], sendTimeout: 50 }), isTimeout)

  // The broker answers once the send has timed out, saying that it lost the state of the producer
  harness.respond = respondWithOffset
  responses[0](new ProtocolError('UNKNOWN_PRODUCER_ID'))

  // The producer is reset and initialized again by the next send, rather than first recovered for nobody
  await harness.producer.send({ messages: [message('b')] })

  strictEqual(harness.producerIds, 2)
})

test('send() with orderedSends should complete a long queue of calls which have timed out', async t => {
  const harness = createHarness(t, { orderedSends: true })
  const release = holdConnections(harness)

  const first = harness.producer.send({ messages: [message('a')] })
  const queued: Promise<unknown>[] = []

  // Calls with different settings cannot share a request, so each one waits for the previous one
  for (let i = 0; i < 5000; i++) {
    queued.push(
      harness.producer.send({
        messages: [message('b')],
        acks: i % 2 === 0 ? ProduceAcks.ALL : ProduceAcks.LEADER,
        sendTimeout: 50
      })
    )
  }

  for (const sent of queued) {
    await rejects(sent, isTimeout)
  }

  release()

  deepStrictEqual((await first).offsets, [{ topic: 'test', partition: 0, offset: 10n }])
  strictEqual(harness.writes, 1)
})

test('beginTransaction should reject a sendTimeout', async t => {
  const harness = createHarness(t, { idempotent: true, transactionalId: 'send-timeout' })

  await rejects(harness.producer.beginTransaction({ sendTimeout: 1000 }), (error: Error) => {
    ok(error instanceof UserError)
    strictEqual(error.message, 'Cannot use sendTimeout within a transaction.')
    return true
  })
})

test('send() should time out while a beforeSerialization hook has not completed', async t => {
  const harness = createHarness(t, { beforeSerialization: () => new Promise<void>(() => {}) })

  await rejects(harness.producer.send({ messages: [message('a')], sendTimeout: 50 }), isTimeout)
  strictEqual(harness.writes, 0)
})

test('send() should not fire its timer once it has completed', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  t.after(() => t.mock.timers.reset())

  const harness = createHarness(t)
  harness.connect = callback => callback(null, {} as Connection)

  let calls = 0
  const { promise: completed, resolve } = Promise.withResolvers<void>()

  harness.producer.send({ messages: [message('a')], sendTimeout: 1000 }, error => {
    calls++
    strictEqual(error, null)
    resolve()
  })

  await completed
  t.mock.timers.tick(1000)

  strictEqual(calls, 1)
})

test('send() should report its timeout to the diagnostics channel', async t => {
  const harness = createHarness(t)
  holdConnections(harness)

  const errors: unknown[] = []

  function onError (context: ClientDiagnosticEvent): void {
    errors.push((context as ClientDiagnosticEvent & { error: unknown }).error)
  }

  producerSendsChannel.error.subscribe(onError as (message: unknown) => void)
  t.after(() => producerSendsChannel.error.unsubscribe(onError as (message: unknown) => void))

  await rejects(harness.producer.send({ messages: [message('a')], sendTimeout: 50 }), isTimeout)

  strictEqual(errors.length, 1)
  ok(isTimeout(errors[0]))
})

test('sendTimeout should be a positive number', async t => {
  throws(
    () =>
      new Producer({
        clientId: 'send-timeout',
        bootstrapBrokers: ['127.0.0.1:9092'],
        strict: true,
        sendTimeout: 0
      }),
    (error: Error) => {
      ok(error instanceof UserError)
      ok(error.message.includes('sendTimeout'), error.message)
      return true
    }
  )

  const harness = createHarness(t)

  for (const sendTimeout of [-1, 2 ** 31]) {
    await rejects(harness.producer.send({ messages: [message('a')], sendTimeout }), (error: Error) => {
      ok(error instanceof UserError)
      ok(error.message.includes('sendTimeout'), error.message)
      return true
    })
  }
})
