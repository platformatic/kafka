import { deepStrictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import {
  type ChannelListener,
  subscribe,
  tracingChannel,
  type TracingChannelSubscribers,
  unsubscribe
} from 'node:diagnostics_channel'
import { type TestContext } from 'node:test'
import { kMetadata } from '../src/clients/base/base.ts'
import {
  Admin,
  type AdminOptions,
  Base,
  type BaseOptions,
  type Broker,
  type Callback,
  type CallbackWithPromise,
  type Connection,
  type ConnectionPool,
  Consumer,
  type ConsumerOptions,
  MultipleErrors,
  Producer,
  type ProducerOptions,
  type ResponseParser,
  type Writer
} from '../src/index.ts'

export const kafkaBootstrapServers = ['localhost:9092']
export const mockedErrorMessage = 'Cannot connect to any broker.'
export const mockedOperationId = -1n

export function createBase (t: TestContext, overrideOptions: Partial<BaseOptions> = {}) {
  const options: BaseOptions = {
    clientId: `test-client-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    ...overrideOptions
  }

  const client = new Base(options)
  t.after(() => client.close())

  return client
}

export function createAdmin (t: TestContext, overrideOptions = {}) {
  const options: AdminOptions = {
    clientId: `test-admin-admin-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    ...overrideOptions
  }

  const admin = new Admin(options)
  t.after(() => admin.close())

  return admin
}

export function createProducer<K = Buffer, V = Buffer, HK = Buffer, HV = Buffer> (
  t: TestContext,
  overrideOptions: Partial<ProducerOptions<K, V, HK, HV>> = {}
) {
  const options: ProducerOptions<K, V, HK, HV> = {
    clientId: `test-producer-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    autocreateTopics: true,
    ...overrideOptions
  }

  const producer = new Producer<K, V, HK, HV>(options)
  t.after(() => producer.close())

  return producer
}

export function createConsumer<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> (
  t: TestContext,
  overrideOptions: Partial<ConsumerOptions<Key, Value, HeaderKey, HeaderValue>> = {}
) {
  const options: ConsumerOptions<Key, Value, HeaderKey, HeaderValue> = {
    clientId: `test-consumer-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    groupId: createGroupId(),
    timeout: 1000,
    sessionTimeout: 6000,
    rebalanceTimeout: 6000,
    heartbeatInterval: 1000,
    retries: 1,
    ...overrideOptions
  }

  const consumer = new Consumer<Key, Value, HeaderKey, HeaderValue>(options)
  t.after(() => consumer.close(true))

  return consumer
}

export function createGroupId () {
  return `test-consumer-group-${randomUUID()}`
}

export async function createTopic (t: TestContext, create: boolean = false, partitions: number = 1) {
  const topic = `test-topic-${randomUUID()}`

  if (create) {
    const admin = createAdmin(t)
    await admin.createTopics({ topics: [topic], partitions })
  }

  return topic
}

export function mockMethod (
  target: any,
  method: string | symbol,
  callToMock: number | ((current: number) => boolean) = 1,
  errorToMock?: Error | null,
  returnValue?: unknown,
  fn?: (original: (...args: any[]) => void, ...args: any[]) => boolean | void
) {
  if (typeof errorToMock === 'undefined') {
    errorToMock = new MultipleErrors(mockedErrorMessage, [new Error(mockedErrorMessage + ' (internal)')])
  }

  const original = target[method].bind(target)
  let calls = 0

  target[method] = function (...args: any[]) {
    calls++

    const shouldMock = typeof callToMock === 'function' ? callToMock(calls) : calls === callToMock
    if (shouldMock) {
      if (fn) {
        const shouldKeepMock = fn(original, ...args)

        if (!shouldKeepMock) {
          target[method] = original
        }
      } else {
        target[method] = original

        const cb = args.at(-1) as Function
        cb(errorToMock, returnValue as unknown as Connection)
      }

      return
    }

    original(...args)
  }
}

export function mockConnectionPoolGet (
  pool: ConnectionPool,
  callToMock: number = 1,
  errorToMock?: Error | null,
  returnValue?: unknown,
  fn?: (original: (...args: any[]) => void, ...args: any[]) => boolean | void
) {
  mockMethod(pool, 'get', callToMock, errorToMock, returnValue, fn)
}

export function mockConnectionPoolGetFirstAvailable (
  pool: ConnectionPool,
  callToMock: number = 1,
  errorToMock?: Error | null,
  returnValue?: unknown,
  fn?: (original: (...args: any[]) => void, ...args: any[]) => boolean | void
) {
  mockMethod(pool, 'getFirstAvailable', callToMock, errorToMock, returnValue, fn)
}

export function mockMetadata (
  client: Base<BaseOptions>,
  callToMock: number = 1,
  errorToMock?: Error | null,
  returnValue?: unknown,
  fn?: (original: (...args: any[]) => void, ...args: any[]) => boolean | void
) {
  mockMethod(client, kMetadata, callToMock, errorToMock, returnValue, fn)
}

export function mockAPI (
  pool: ConnectionPool,
  apiKeyToMock: number | ((current: number) => boolean),
  errorToMock?: Error | null,
  returnValue?: unknown,
  fn?: (original: (...args: any[]) => void, ...args: any[]) => boolean | void
) {
  if (typeof errorToMock === 'undefined') {
    errorToMock = new MultipleErrors(mockedErrorMessage, [new Error(mockedErrorMessage + ' (internal)')])
  }

  const originalGet = pool.get.bind(pool)

  pool.get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    originalGet(broker, (error: Error | null, connection: Connection) => {
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
        const shouldMock = typeof apiKeyToMock === 'function' ? apiKeyToMock(apiKey) : apiKey === apiKeyToMock

        if (shouldMock) {
          if (fn) {
            const shouldKeepMock = fn(
              originalSend,
              apiKey,
              apiVersion,
              payload,
              responseParser,
              hasRequestHeaderTaggedFields,
              hasResponseHeaderTaggedFields,
              callback
            )

            if (!shouldKeepMock) {
              connection.send = originalSend
              pool.get = originalGet
            }
          } else {
            connection.send = originalSend
            pool.get = originalGet

            callback(errorToMock, returnValue as ReturnType)
          }
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
  } as typeof originalGet
}

export function createCreationChannelVerifier<InstanceType> (
  channel: string,
  filter: (data: InstanceType) => boolean = () => true
) {
  let instance: InstanceType | null = null

  function creationSubscriber (candidate: InstanceType) {
    if (filter(candidate)) {
      instance = candidate
    }
  }

  subscribe(channel, creationSubscriber as ChannelListener)

  return function get (): InstanceType | null {
    unsubscribe(channel, creationSubscriber as ChannelListener)
    return instance
  }
}

export function createTracingChannelVerifier<DiagnosticEvent extends Record<string, unknown>> (
  channelName: string,
  unclonable: string | string[],
  verifiers: Record<string, Function>,
  filter: (label: string, data: DiagnosticEvent) => boolean = () => true
) {
  const channel = tracingChannel<string, DiagnosticEvent>(channelName)
  const eventsData: Record<string, object> = {}
  const operationsId = new Set<bigint>()

  function tracker (label: string, data: DiagnosticEvent) {
    if (!filter(label, data)) {
      return
    }
    if (!Array.isArray(unclonable)) {
      unclonable = [unclonable]
    }

    const toClone: Record<string, unknown> = {}
    const toCopy: Record<string, unknown> = {}

    for (const [key, value] of Object.entries(data)) {
      if (unclonable.includes(key)) {
        toCopy[key] = value
      } else {
        toClone[key] = value
      }
    }

    operationsId.add(data.operationId as bigint)
    eventsData[label] = { ...toCopy, ...structuredClone(toClone), operationId: mockedOperationId }
  }

  const subscribers = {
    start: tracker.bind(null, 'start'),
    end: tracker.bind(null, 'end'),
    asyncStart: tracker.bind(null, 'asyncStart'),
    asyncEnd: tracker.bind(null, 'asyncEnd'),
    error: tracker.bind(null, 'error')
  }

  channel.subscribe(subscribers as TracingChannelSubscribers<DiagnosticEvent>)

  return function verify () {
    channel.unsubscribe(subscribers as TracingChannelSubscribers<DiagnosticEvent>)
    deepStrictEqual(operationsId.size, 1, 'Only one operationId should be present, got: ' + operationsId.toString())

    for (const [label, verifier] of Object.entries(verifiers)) {
      verifier(eventsData[label])
    }
  }
}
