import { randomUUID } from 'node:crypto'
import {
  Consumer,
  debugDump,
  MessagesStreamModes,
  Producer,
  stringDeserializers,
  stringSerializers
} from '../../src/index.ts'
import type { CommitOptionsPartition } from '../../src/index.ts'
import { batchIterator } from 'hwp'

const bootstrapBrokers = [process.env.KAFKA_BROKER ?? 'localhost:9092']
const topic = `test-batching-${randomUUID()}`
const totalMessages = 10

async function main () {
  const producer = new Producer({
    clientId: 'id',
    bootstrapBrokers,
    strict: true,
    serializers: stringSerializers,
    autocreateTopics: true
  })

  try {
    for (let i = 0; i < totalMessages; i++) {
      await producer.send({ messages: [{ topic, key: `key-${i}`, value: `value-${i}` }] })
    }
  } finally {
    await producer.close()
  }

  const consumer = new Consumer({
    groupId: randomUUID(),
    clientId: 'id',
    bootstrapBrokers,
    strict: true,
    deserializers: stringDeserializers
  })

  try {
    const stream = await consumer.consume({
      autocommit: false,
      topics: [topic],
      maxWaitTime: 500,
      mode: MessagesStreamModes.EARLIEST
    })

    try {
      let read = 0
      const source = stream[Symbol.asyncIterator]()

      for await (const messages of batchIterator(source, Math.floor(totalMessages / 3), 1000)) {
        debugDump('| RECEIVED BATCH |', { length: messages.length })

        // Process the whole batch before committing any of its offsets.
        const offsets = new Map<string, CommitOptionsPartition>()
        for (const message of messages) {
          debugDump('|        MESSAGE |', { key: message.key, value: message.value })
          offsets.set(`${message.topic}:${message.partition}`, {
            topic: message.topic,
            partition: message.partition,
            offset: message.offset + 1n,
            leaderEpoch: message.leaderEpoch
          })
        }

        await consumer.commit({ offsets: [...offsets.values()] })
        read += messages.length

        if (read === totalMessages) {
          debugDump('|       CLOSING |')
          break
        }
      }
    } finally {
      await stream.close()
    }
  } finally {
    await consumer.close(true)
  }
}

await main()
