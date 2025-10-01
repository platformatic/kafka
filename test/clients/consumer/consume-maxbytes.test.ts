import assert from 'node:assert'
import test from 'node:test'
import { Admin, Consumer, type MessageToProduce, Producer } from '../../../src/index.ts'

test('consumer should properly consume messages with maxBytes limit', async (t) => {
  const publishMessages = 100
  const batchSize = 10
  const bootstrapBrokers = ['127.0.0.1:9001']

  const topic = 'consume-maxbytes-topic' + Date.now()
  const clientId = 'consume-maxbytes' + Date.now()
  const groupId = 'consume-maxbytes-group' + Date.now()

  const admin = new Admin({ bootstrapBrokers, clientId })
  const producer = new Producer({ bootstrapBrokers, clientId })
  const consumer = new Consumer({
    bootstrapBrokers,
    clientId,
    groupId,
  })

  await admin.createTopics({
    topics: [topic],
    partitions: 3,
  })

  const batch: MessageToProduce[] = []
  for (let i = 0; i < publishMessages; i++) {
    const message = JSON.stringify({ id: i })

    batch.push({
      key: Buffer.from('customer_id'),
      value: Buffer.from(message),
      topic,
    })

    if (batch.length >= batchSize) {
      await producer.send({ messages: batch })
      batch.length = 0
    }
  }
  if (batch.length > 0) {
    await producer.send({ messages: batch })
  }

  const stream = await consumer.consume({
    topics: [topic],
    autocommit: true,
    mode: 'earliest',
    maxBytes: 1024 // magic number that will make the stream receive messages in more "onData" calls
  })

  let receivedMessages = 0
  // eslint-disable-next-line
  for await (const _ of stream) {
    if (++receivedMessages === publishMessages) {
      break
    }
  }

  assert.strictEqual(receivedMessages, publishMessages)

  t.after(async () => {
    await admin.close()
    await stream.close()
    await producer.close()
    await consumer.close()
  })
})
