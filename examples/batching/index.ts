import { randomUUID } from 'node:crypto'
import { Duplex } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import {
  type Callback,
  Consumer,
  debugDump,
  type Message,
  MessagesStreamModes,
  Producer,
  stringDeserializers,
  stringSerializers
} from '../../src/index.ts'

const bootstrapBrokers = ['localhost:9092']
const topic = `test-batching-${randomUUID()}`
const totalMessages = 10

class MessageBatcher extends Duplex {
  #size
  #timeout
  #batch
  #batchTimeout

  constructor (size, timeout) {
    super({ objectMode: true })
    this.#size = size
    this.#timeout = timeout
    this.#batch = []
  }

  _read () {
    // No-op - Data is pushed from the writable side
  }

  _write (chunk: Message, _encoding: string, callback: Callback<void>) {
    this.#batch.push(chunk)

    if (this.#batch.length < this.#size) {
      this.#batchTimeout ??= setTimeout(() => {
        this.#flush()
      }, this.#timeout)
    } else {
      this.#flush()
    }

    callback(null)
  }

  // Write side is closed, flush the remaining messages
  _final (callback) {
    this.#flush()
    this.push(null) // End readable side
    callback()
  }

  #flush () {
    clearTimeout(this.#batchTimeout)
    this.#batchTimeout = null

    if (this.#batch.length === 0) {
      return
    }

    this.push(this.#batch)
    this.#batch = []
  }
}

async function main () {
  const producer = new Producer({
    clientId: 'id',
    bootstrapBrokers,
    strict: true,
    serializers: stringSerializers,
    autocreateTopics: true
  })

  for (let i = 0; i < totalMessages; i++) {
    await producer.send({ messages: [{ topic, key: `key-${i}`, value: `value-${i}` }] })
  }

  await producer.close()

  // Create the consumer
  const consumer = new Consumer({
    groupId: randomUUID(),
    clientId: 'id',
    bootstrapBrokers: ['localhost:9092'],
    strict: true,
    deserializers: stringDeserializers
  })

  const stream = await consumer.consume({ topics: [topic], maxWaitTime: 500, mode: MessagesStreamModes.EARLIEST })

  let read = 0
  const batch = new MessageBatcher(Math.floor(totalMessages / 3), 1000)
  batch.on('data', data => {
    debugDump('| RECEIVED BATCH |', { length: data.length })

    for (const message of data) {
      debugDump('|        MESSAGE |', { key: message.key, value: message.value })
    }

    read += data.length

    if (read === totalMessages) {
      debugDump('|       CLOSING |')
      stream.close()
    }
  })

  await pipeline(stream, batch)
  await consumer.close()
}

await main()
