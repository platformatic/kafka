import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { test, type TestContext } from 'node:test'
import { kGetApi, kGetConnection, kMetadata } from '../../../src/clients/base/base.ts'
import type { ClusterMetadata } from '../../../src/clients/base/types.ts'
import {
  type Connection,
  NetworkError,
  Producer,
  type ProducerOptions,
  TimeoutError,
  UserError
} from '../../../src/index.ts'
import { mockMethod } from '../../helpers.ts'

type ConnectionCallback = (error: Error | null, connection?: Connection) => void

// A producer whose metadata, connections and Produce API are mocked, so that each test decides when they answer
interface Harness {
  producer: Producer
  connect: (callback: ConnectionCallback) => void
  writes: number
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

  const harness: Harness = { producer, connect: () => {}, writes: 0, onApi: () => {} }

  mockMethod(producer, kMetadata, () => true, null, undefined, (_, ...args) => {
    args.at(-1)(null, createMetadata())
    return true
  })

  mockMethod(producer, kGetConnection, () => true, null, undefined, (_, ...args) => {
    harness.connect(args.at(-1))
    return true
  })

  mockMethod(producer, kGetApi, () => true, null, undefined, (_, ...args) => {
    args.at(-1)(null, (...apiArgs: unknown[]) => {
      harness.writes++
      ;(apiArgs.at(-1) as (error: Error | null, response: unknown) => void)(null, {
        throttleTimeMs: 0,
        responses: [{ name: 'test', partitionResponses: [{ index: 0, errorCode: 0, baseOffset: 10n }] }]
      })
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
  let timedOut = false
  const { promise: retriesAfterTimeout, resolve } = Promise.withResolvers<number>()

  const harness = createHarness(t, {
    retryDelay: (_client, operationId) => {
      if (operationId === 'produce') {
        produceRetries++
      }

      return 10
    }
  })

  // Each attempt fails with a retriable error. The first one after the timeout reports whether another was scheduled.
  harness.connect = callback => {
    const before = produceRetries
    callback(new NetworkError('Connection refused.'))

    if (timedOut) {
      resolve(produceRetries - before)
    }
  }

  await rejects(harness.producer.send({ messages: [message('a')], sendTimeout: 50 }), isTimeout)
  timedOut = true

  strictEqual(await retriesAfterTimeout, 0)
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
  const { promise: secondApiObtained, resolve } = Promise.withResolvers<void>()

  harness.onApi = () => {
    if (++apis === 2) {
      resolve()
    }
  }

  // The second call waits for the first one, which writes to the same partition
  const first = harness.producer.send({ messages: [message('a')] })
  const second = harness.producer.send({ messages: [message('b')], sendTimeout: 50 })

  await rejects(second, isTimeout)

  release()

  deepStrictEqual((await first).offsets, [{ topic: 'test', partition: 0, offset: 10n }])
  await secondApiObtained

  strictEqual(harness.writes, 1)
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

  await rejects(harness.producer.send({ messages: [message('a')], sendTimeout: -1 }), (error: Error) => {
    ok(error instanceof UserError)
    ok(error.message.includes('sendTimeout'), error.message)
    return true
  })
})
