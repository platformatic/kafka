import { deepStrictEqual, strictEqual } from 'node:assert'
import { once } from 'node:events'
import { connect, createServer, type AddressInfo, type Socket } from 'node:net'
import { test } from 'node:test'
import { kGetConnection } from '../../src/clients/base/base.ts'
import { createConsumer, createProducer, createTopic, kafkaSingleBootstrapServers, mockMethod } from '../helpers.ts'

// A TCP chunk can contain part of a Kafka frame, or several complete frames.
function readFrames (socket: Socket, handle: (frame: Buffer) => void): void {
  let pending: Buffer = Buffer.alloc(0)
  socket.on('data', chunk => {
    pending = Buffer.concat([pending, chunk])
    while (pending.length >= 4) {
      const size = 4 + pending.readInt32BE()
      if (pending.length < size) {
        break
      }

      handle(pending.subarray(0, size))
      pending = pending.subarray(size)
    }
  })
}

for (const loseAck of [false, true]) {
  test(`issue-472: preserve partition order across a lost ${loseAck ? 'ack' : 'request'}`, { timeout: 30000 }, async t => {
    const topic = await createTopic(t, true, 1, kafkaSingleBootstrapServers)
    // On a freshly started cluster the coordinator may still be loading on the first InitProducerId.
    await createProducer(t, { bootstrapBrokers: kafkaSingleBootstrapServers, retries: 5 }).initIdempotentProducer({})
    const broker = new URL(`tcp://${kafkaSingleBootstrapServers[0]}`)
    let initCalls = 0
    let produceCalls = 0
    const proxy = createServer(client => {
      const upstream = connect(Number(broker.port), broker.hostname)
      let dropReply: number | undefined
      client.on('close', () => upstream.destroy())
      upstream.on('close', () => client.destroy())
      client.on('error', () => upstream.destroy())
      upstream.on('error', () => client.destroy())

      readFrames(client, frame => {
        const apiKey = frame.readInt16BE(4)
        if (apiKey === 22) {
          initCalls++
        }

        if (apiKey === 0 && ++produceCalls === 1) {
          if (!loseAck) {
            client.destroy()
            return
          }

          // Wait for the broker to append A before discarding its response.
          dropReply = frame.readInt32BE(8)
        }

        upstream.write(frame)
      })
      readFrames(upstream, frame => {
        if (frame.readInt32BE(4) === dropReply) {
          client.destroy()
        } else {
          client.write(frame)
        }
      })
    })

    proxy.listen(0, '127.0.0.1')
    await once(proxy, 'listening')
    t.after(() => { proxy.close() })
    const { port } = proxy.address() as AddressInfo
    const backoff = Promise.withResolvers<void>()
    const producer = createProducer(t, {
      bootstrapBrokers: [`127.0.0.1:${port}`],
      idempotent: true,
      retries: 3,
      retryDelay: (_client, operation) => {
        if (operation === 'produce') {
          backoff.resolve()
        }
        return 200
      }
    })

    // Route the advertised partition leader through the proxy as well as the bootstrap broker.
    mockMethod(producer, kGetConnection, () => true, null, undefined, (original, broker, callback) => {
      original({ ...broker, host: '127.0.0.1', port }, callback)
      return true
    })

    const expected = loseAck ? ['A', 'B', 'C', 'D', 'E', 'F', 'G'] : ['A', 'B']
    const send = (value: string, index: number) => producer.send({
      messages: [{ topic, partition: 0, value: Buffer.from(value) }],
      acks: -1,
      // Alternate options so the six waiting calls cannot coalesce into one batch.
      repeatOnStaleMetadata: index % 2 === 0
    })
    const first = send('A', 0)
    await backoff.promise
    await Promise.all([first, ...expected.slice(1).map((value, index) => send(value, index + 1))])
    strictEqual(initCalls, 1)
    strictEqual(produceCalls, expected.length + 1)

    // The next offset detects even a duplicate appended after the last expected message.
    const end = await send('end', 0)
    strictEqual(end.offsets![0].offset, BigInt(expected.length))
    const consumer = createConsumer(t, { bootstrapBrokers: kafkaSingleBootstrapServers })
    const stream = await consumer.consume({ topics: [topic], mode: 'earliest' })
    const values: string[] = []
    for await (const message of stream) {
      values.push(message.value.toString())
      if (message.value.toString() === 'end') {
        break
      }
    }
    deepStrictEqual(values, [...expected, 'end'])
  })
}
