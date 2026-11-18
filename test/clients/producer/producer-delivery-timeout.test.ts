import { ok, strictEqual, throws } from 'node:assert'
import { type AddressInfo, createServer, type Socket } from 'node:net'
import { test, type TestContext } from 'node:test'
import { kGetApi, kGetConnection, kMetadata } from '../../../src/clients/base/base.ts'
import type { ClusterMetadata } from '../../../src/clients/base/types.ts'
import { type Connection, Producer, TimeoutError, UserError } from '../../../src/index.ts'
import { mockMethod } from '../../helpers.ts'

// A broker which accepts connections and never answers, so every request runs to its requestTimeout
async function createSilentBroker (t: TestContext): Promise<string> {
  const server = createServer()
  const sockets: Socket[] = []

  server.on('connection', socket => {
    socket.on('error', () => {})
    sockets.push(socket)
  })

  t.after((_, cb) => {
    for (const socket of sockets) {
      socket.destroy()
    }

    server.close(cb)
  })

  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  return `127.0.0.1:${(server.address() as AddressInfo).port}`
}

function findTimeout (error: unknown): TimeoutError | undefined {
  if (error instanceof TimeoutError) {
    return error
  }

  for (const inner of (error as { errors?: unknown[] }).errors ?? []) {
    const found = findTimeout(inner)

    if (found) {
      return found
    }
  }

  return undefined
}

async function sendFailure (producer: Producer<string, string, string, string>, deliveryTimeout?: number) {
  const started = Date.now()

  try {
    await producer.send({
      messages: [{ topic: 'test', key: 'key', value: 'value' }],
      ...(typeof deliveryTimeout === 'number' ? { deliveryTimeout } : {})
    })
  } catch (error) {
    return { error, elapsed: Date.now() - started }
  }

  throw new Error('The send should have failed.')
}

const silentBrokerOptions = {
  clientId: 'delivery-timeout',
  retries: 100,
  retryDelay: 100,
  requestTimeout: 500,
  connectTimeout: 500,
  serializers: {
    key: (value?: string) => (value ? Buffer.from(value) : undefined),
    value: (value?: string) => (value ? Buffer.from(value) : undefined),
    headerKey: (value?: string) => (value ? Buffer.from(value) : undefined),
    headerValue: (value?: string) => (value ? Buffer.from(value) : undefined)
  }
}

test('send() should fail once deliveryTimeout has passed, whatever the retries', { timeout: 10_000 }, async t => {
  const producer = new Producer<string, string, string, string>({
    ...silentBrokerOptions,
    bootstrapBrokers: [await createSilentBroker(t)],
    deliveryTimeout: 1000
  })
  t.after(() => producer.close(true))

  const { error, elapsed } = await sendFailure(producer)

  ok(findTimeout(error), 'the failure should carry a TimeoutError')
  // A request already sent when the deadline passes still runs to its requestTimeout
  ok(elapsed < 1000 + 500 + 500, `took ${elapsed}ms`)
})

test('send() should accept deliveryTimeout per call', { timeout: 10_000 }, async t => {
  const producer = new Producer<string, string, string, string>({
    ...silentBrokerOptions,
    bootstrapBrokers: [await createSilentBroker(t)]
  })
  t.after(() => producer.close(true))

  const { error, elapsed } = await sendFailure(producer, 1000)

  ok(findTimeout(error), 'the failure should carry a TimeoutError')
  ok(elapsed < 1000 + 500 + 500, `took ${elapsed}ms`)
})

test('send() should not write a Produce request once deliveryTimeout has passed', { timeout: 10_000 }, async t => {
  const producer = new Producer<string, string, string, string>({
    ...silentBrokerOptions,
    bootstrapBrokers: ['127.0.0.1:9092'],
    deliveryTimeout: 100
  })
  t.after(() => producer.close(true))

  const metadata: ClusterMetadata = {
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

  let produceRequests = 0

  mockMethod(producer, kMetadata, () => true, null, undefined, (_, ...args) => {
    args.at(-1)(null, metadata)
    return true
  })

  // The leader connection is ready only after the deadline
  mockMethod(producer, kGetConnection, () => true, null, undefined, (_, ...args) => {
    setTimeout(() => args.at(-1)(null, {} as Connection), 300)
    return true
  })

  mockMethod(producer, kGetApi, () => true, null, undefined, (_, ...args) => {
    args.at(-1)(null, (...apiArgs: unknown[]) => {
      produceRequests++
      ;(apiArgs.at(-1) as (error: Error | null, response: unknown) => void)(null, { responses: [] })
    })
    return true
  })

  const { error } = await sendFailure(producer)

  ok(findTimeout(error), 'the failure should carry a TimeoutError')
  strictEqual(produceRequests, 0)
})

test('constructor should reject a deliveryTimeout which is not positive', () => {
  throws(
    () =>
      new Producer({
        clientId: 'delivery-timeout',
        bootstrapBrokers: ['127.0.0.1:9092'],
        strict: true,
        deliveryTimeout: 0
      }),
    (error: Error) => {
      ok(error instanceof UserError)
      ok(error.message.includes('deliveryTimeout'), error.message)
      return true
    }
  )
})
