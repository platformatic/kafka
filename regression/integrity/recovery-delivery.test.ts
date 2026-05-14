import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { type AddressInfo, connect, createServer, type Server, type Socket } from 'node:net'
import { once } from 'node:events'
import { test } from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { type Message, MessagesStreamModes, parseBroker } from '../../src/index.ts'
import {
  assertContiguousIds,
  assertNoDuplicateIds,
  createConsumer,
  createProducer,
  createTopic,
  regressionSingleBootstrapServers,
  type ConsumedValue
} from '../helpers/index.ts'

interface ProxyServer {
  server: Server
  sockets: Set<Socket>
  port: number
}

function jsonDeserializers () {
  return {
    key: (data: Buffer | undefined) => data?.toString() ?? '',
    value: (data: Buffer | undefined) => JSON.parse(data?.toString() ?? '{}') as ConsumedValue,
    headerKey: (data: Buffer | undefined) => data?.toString() ?? '',
    headerValue: (data: Buffer | undefined) => data?.toString() ?? ''
  }
}

async function createKafkaProxy (): Promise<ProxyServer> {
  const broker = parseBroker(regressionSingleBootstrapServers[0]!)
  const sockets = new Set<Socket>()
  const server = createServer(client => {
    const upstream = connect(broker.port, broker.host)
    sockets.add(client)
    sockets.add(upstream)

    client.pipe(upstream)
    upstream.pipe(client)

    client.on('close', () => sockets.delete(client))
    upstream.on('close', () => sockets.delete(upstream))
    client.on('error', () => upstream.end())
    upstream.on('error', () => client.end())
  })
  const listening = once(server, 'listening')

  server.listen(0)
  await listening

  return { server, sockets, port: (server.address() as AddressInfo).port }
}

async function restartProxy (proxy: ProxyServer): Promise<void> {
  for (const socket of proxy.sockets) {
    socket.destroy()
  }

  const closed = once(proxy.server, 'close')
  proxy.server.close()
  await closed

  const listening = once(proxy.server, 'listening')
  proxy.server.listen(proxy.port)
  await listening
}

async function closeProxy (proxy: ProxyServer): Promise<void> {
  for (const socket of proxy.sockets) {
    socket.destroy()
  }

  if (!proxy.server.listening) {
    return
  }

  const closed = once(proxy.server, 'close')
  proxy.server.close()
  await closed
}

test('regression #206: reconnect recovery preserves delivery invariants', { timeout: 90_000 }, async t => {
  // Route the consumer through a local TCP proxy, force the proxy to restart after
  // some messages arrive, then verify the recovered stream has no gaps or repeats.
  const total = 20
  const topic = await createTopic(t, 1, regressionSingleBootstrapServers)
  const producer = createProducer(t, { bootstrapBrokers: regressionSingleBootstrapServers })
  const proxy = await createKafkaProxy()
  t.after(() => closeProxy(proxy))

  await producer.send({
    messages: Array.from({ length: total }, (_, id) => ({ topic, key: String(id), value: JSON.stringify({ id }) }))
  })

  const consumer = createConsumer<string, ConsumedValue, string, string>(t, {
    bootstrapBrokers: [`localhost:${proxy.port}`],
    retries: true,
    retryDelay: 100,
    deserializers: jsonDeserializers()
  })
  const messages: Array<Message<string, ConsumedValue, string, string>> = []
  const firstBatchReceived = once(consumer, 'regression:first-batch')
  const completed = once(consumer, 'regression:completed')

  consumer.on('client:broker:disconnect', () => {})
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup()

  const stream = await consumer.consume({ topics: [topic], mode: MessagesStreamModes.EARLIEST, autocommit: false, maxWaitTime: 100 })
  stream.on('data', message => {
    messages.push(message)

    if (messages.length === 5) {
      consumer.emit('regression:first-batch')
    }

    if (messages.length === total) {
      consumer.emit('regression:completed')
    }
  })

  await firstBatchReceived
  const disconnected = once(consumer, 'client:broker:disconnect')
  const recovered = once(consumer, 'client:broker:connect')
  await restartProxy(proxy)
  await disconnected
  await recovered
  await completed
  await sleep(100)
  await stream.close()

  strictEqual(messages.length, total)
  assertNoDuplicateIds(messages)
  assertContiguousIds(messages, total)

  for (let index = 1; index < messages.length; index++) {
    deepStrictEqual(messages[index]!.offset - messages[index - 1]!.offset, 1n)
  }

  ok(consumer.isConnected())
})
