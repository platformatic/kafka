import { deepStrictEqual, ok, rejects, strictEqual, throws } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { Readable } from 'node:stream'
import { test, type TestContext } from 'node:test'
import * as Prometheus from 'prom-client'
import {
  type CallbackWithPromise,
  Consumer,
  type ConsumerOptions,
  type Deserializers,
  executeWithTimeout,
  type Message,
  MessagesStream,
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  MultipleErrors,
  ProduceAcks,
  Producer,
  type ProducerOptions,
  type Serializers,
  sleep,
  type StreamOptions,
  stringDeserializers,
  stringSerializers,
  UserError
} from '../../../src/index.ts'
import { mockMetadata } from '../../helpers.ts'

interface ConsumeResult<K = string, V = string, HK = string, HV = string> {
  messages: Message<K, V, HK, HV>[]
  stream: MessagesStream<K, V, HK, HV>
  consumer: Consumer<K, V, HK, HV>
  timeout: boolean
}

const kafkaBootstrapServers = ['localhost:29092']

// Helper functions
function createTestGroupId () {
  return `test-consumer-group-${randomUUID()}`
}

function createTestTopic () {
  return `test-topic-${randomUUID()}`
}

function createProducer<K = string, V = string, HK = string, HV = string> (
  t: TestContext,
  options?: Partial<ProducerOptions<K, V, HK, HV>>
): Producer<K, V, HK, HV> {
  options ??= {}

  const producer = new Producer({
    clientId: `test-producer-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    serializers: stringSerializers as Serializers<K, V, HK, HV>,
    autocreateTopics: true,
    ...options
  })

  t.after(() => producer.close())

  return producer
}

// This helper creates a consumer and ensure proper termination
function createConsumer<K = string, V = string, HK = string, HV = string> (
  t: TestContext,
  groupId?: string,
  options?: Partial<ConsumerOptions<K, V, HK, HV>>
): Consumer<K, V, HK, HV> {
  options ??= {}

  const consumer = new Consumer<K, V, HK, HV>({
    clientId: `test-consumer-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    groupId: groupId ?? createTestGroupId(),
    deserializers: stringDeserializers as Deserializers<K, V, HK, HV>,
    timeout: 1000,
    sessionTimeout: 6000,
    rebalanceTimeout: 6000,
    heartbeatInterval: 1000,
    retries: 1,
    ...options
  })

  t.after(async () => {
    await consumer.close(true)
  })

  return consumer
}

// This function produces sample messages to a topic for testing the consumer
async function produceTestMessages (
  t: TestContext,
  topic: string,
  count = 3,
  options?: Partial<ProducerOptions<string, string, string, string>>,
  startIndex: number = 0
) {
  const producer = createProducer(t, options)

  for (let i = startIndex; i < count + startIndex; i++) {
    await producer.send({
      messages: [
        {
          topic,
          key: `key-${i}`,
          value: `value-${i}`,
          headers: { headerKey: `headerValue-${i}` }
        }
      ],
      acks: ProduceAcks.LEADER
    })
  }
}

// This helper consumes messages from a topic using a stream
async function consumeMessages<K = string, V = string, HK = string, HV = string> (
  t: TestContext,
  groupId: string,
  topic: string,
  options: Partial<ConsumerOptions<K, V, HK, HV> & StreamOptions> = {}
): Promise<ConsumeResult<K, V, HK, HV>> {
  const consumer = createConsumer<K, V, HK, HV>(t, groupId, options)
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup({})
  options.metrics = undefined

  const stream: MessagesStream<K, V, HK, HV> = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    maxWaitTime: 4000,
    autocommit: false,
    ...options
  })

  const messages: Message<K, V, HK, HV>[] = []
  const { promise, resolve, reject } = Promise.withResolvers<void>()

  function onData (message: Message<K, V, HK, HV>) {
    messages.push(message)

    if (messages.length >= 3) {
      resolve()
    }
  }

  function onEnd () {
    resolve()
  }

  function onError (error: Error) {
    reject(error)
  }

  stream.on('end', onEnd)
  stream.on('error', onError)
  stream.on('data', onData)

  const result = await executeWithTimeout(promise, 5000, 'timeout')

  stream.removeListener('end', onEnd)
  stream.removeListener('error', onError)
  stream.removeListener('data', onData)

  return { messages, stream, consumer, timeout: result === 'timeout' }
}

test('MessagesStream - should be an instance of Readable', async t => {
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  const consumer = createConsumer(t, createTestGroupId())

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST
  })

  ok(stream instanceof Readable, 'MessagesStream should be an instance of Readable')
})

test('MessagesStream - should throw error when specifying offsets with non-MANUAL mode', async t => {
  const consumer = createConsumer(t)

  await throws(
    () => {
      return new MessagesStream(consumer, {
        topics: [createTestTopic()],
        mode: MessagesStreamModes.LATEST,
        offsets: [{ topic: 'test', partition: 0, offset: 0n }]
      })
    },
    (error: any) => {
      ok(error instanceof UserError)
      strictEqual(error.message, 'Cannot specify offsets when the stream mode is not MANUAL.')
      return true
    }
  )
})

test('MessagesStream - should throw error when not specifying offsets with MANUAL mode', async t => {
  const consumer = createConsumer(t)

  throws(
    () => {
      return new MessagesStream(consumer, {
        topics: [createTestTopic()],
        mode: MessagesStreamModes.MANUAL
      })
    },
    (error: any) => {
      ok(error instanceof UserError)
      strictEqual(error.message, 'Must specify offsets when the stream mode is MANUAL.')
      return true
    }
  )
})

test('MessagesStream - should support autocommit and COMMITTED mode', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  // Consume messages with autocommit
  const {
    consumer: firstConsumer,
    stream: firstStream,
    messages: firstBatch
  } = await consumeMessages(t, groupId, topic, {
    mode: MessagesStreamModes.EARLIEST,
    autocommit: true
  })

  // Have the first consumer leave the group so that we are sure assignments are sent to the second consumer
  await firstStream.close()
  await firstConsumer.leaveGroup()

  // Consume again with same group ID but using COMMITTED mode
  // If autocommit worked, we shouldn't see any messages
  const { messages: secondBatch } = await consumeMessages(t, groupId, topic, {
    mode: MessagesStreamModes.COMMITTED
  })

  strictEqual(firstBatch.length, 3, 'Should consume 3 messages in first batch')
  strictEqual(secondBatch.length, 0, 'Should not consume any messages in second batch')
})

test('MessagesStream - should support timed autocommits', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  // Consume messages without autocommit
  const { stream, consumer, messages } = await consumeMessages(t, groupId, topic, {
    mode: MessagesStreamModes.EARLIEST,
    autocommit: 1000
  })

  strictEqual(messages.length, 3, 'Should consume 3 messages')

  // Try to get committed offsets for the topic, it should return none
  {
    const committedOffsets = await consumer.listCommittedOffsets({ topics: [{ topic, partitions: [0] }] })
    deepStrictEqual(committedOffsets.get(topic), [-1n])
  }

  // Close the stream, this should wait for autocommitting to finish
  await stream.close()

  // Get committed offsets again
  {
    const committedOffsets = await consumer.listCommittedOffsets({ topics: [{ topic, partitions: [0] }] })
    deepStrictEqual(committedOffsets.get(topic), [messages.at(-1)!.offset])
  }
})

test('MessagesStream - should support timed empty autocommits', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  const consumer = createConsumer(t, groupId)
  const stream = await consumer.consume({
    topics: [topic],
    autocommit: 100
  })

  await sleep(1000)

  // No error should have happened
  strictEqual(stream.errored, null)
})

test('MessagesStream - should support manual commits', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  // Consume messages without autocommit
  const {
    consumer: firstConsumer,
    stream: firstStream,
    messages: firstBatch
  } = await consumeMessages(t, groupId, topic, {
    mode: MessagesStreamModes.EARLIEST,
    autocommit: false
  })

  strictEqual(firstBatch.length, 3, 'Should consume 3 messages in first batch')
  // Manually commit the first message
  await firstBatch[0].commit()

  // Have the first consumer leave the group so that we are sure assignments are sent to the second consumer
  await firstStream.close()
  await firstConsumer.leaveGroup()

  // Consume again with same group ID but using COMMITTED mode
  const { messages: secondBatch } = await consumeMessages(t, groupId, topic, {
    mode: MessagesStreamModes.COMMITTED
  })

  // Should receive messages starting from offset after the committed one
  strictEqual(secondBatch.length, 2, 'Should consume 2 messages in second batch')
  deepStrictEqual(secondBatch[0].key, 'key-1', 'First message in second batch should be key-1')
  deepStrictEqual(secondBatch[1].key, 'key-2', 'Second message in second batch should be key-2')
})

test('MessagesStream - should consume messages with LATEST mode', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  // LATEST by default
  let messages = []
  const { stream, messages: _messages } = await consumeMessages(t, groupId, topic, {
    mode: MessagesStreamModes.LATEST,
    maxWaitTime: 1000
  })
  messages = _messages

  // Verify no messages are returned initially
  strictEqual(messages.length, 0, 'Should consume 0 messages')

  const { promise, resolve } = Promise.withResolvers<void>()
  function onData (message: Message<string, string, string, string>) {
    messages.push(message)

    if (messages.length >= 3) {
      stream.removeListener('data', onData)
      resolve()
    }
  }

  stream.on('data', onData)

  // Produce another test messages and wait for them to be consumed
  await produceTestMessages(t, topic, 3, {}, 3)
  await promise

  strictEqual(messages.length, 3, 'Should consume 3 messages')

  for (let i = 0; i < 3; i++) {
    const j = i + 3

    deepStrictEqual(messages[i].key, `key-${j}`, `Message ${j} should have correct key`)
    deepStrictEqual(messages[i].value, `value-${j}`, `Message ${j} should have correct value`)
    deepStrictEqual(messages[i].headers.get('headerKey'), `headerValue-${j}`, `Message ${j} should have correct header`)
    strictEqual(messages[i].topic, topic, `Message ${j} should have correct topic`)
    strictEqual(typeof messages[i].offset, 'bigint', `Message ${j} should have bigint offset`)
    strictEqual(typeof messages[i].timestamp, 'bigint', `Message ${j} should have bigint timestamp`)
    strictEqual(typeof messages[i].commit, 'function', `Message ${j} should have commit function`)
  }
})

test('MessagesStream - should consume messages with EARLIEST mode', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  const { messages } = await consumeMessages(t, groupId, topic, {
    mode: MessagesStreamModes.EARLIEST
  })

  // Verify messages
  strictEqual(messages.length, 3, 'Should consume 3 messages')

  for (let i = 0; i < 3; i++) {
    deepStrictEqual(messages[i].key, `key-${i}`, `Message ${i} should have correct key`)
    deepStrictEqual(messages[i].value, `value-${i}`, `Message ${i} should have correct value`)
    deepStrictEqual(messages[i].headers.get('headerKey'), `headerValue-${i}`, `Message ${i} should have correct header`)
    strictEqual(messages[i].topic, topic, `Message ${i} should have correct topic`)
    strictEqual(typeof messages[i].offset, 'bigint', `Message ${i} should have bigint offset`)
    strictEqual(typeof messages[i].timestamp, 'bigint', `Message ${i} should have bigint timestamp`)
    strictEqual(typeof messages[i].commit, 'function', `Message ${i} should have commit function`)
  }
})

test('MessagesStream - should consume messages with MANUAL mode', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  // First consume to get current offsets
  const { messages: firstBatch } = await consumeMessages(t, groupId, topic, {
    mode: MessagesStreamModes.EARLIEST
  })

  strictEqual(firstBatch.length, 3, 'Should consume 3 messages in first batch')

  // Get second message offset
  const secondMessageOffset = firstBatch[1].offset

  // Now use MANUAL mode to start from specific offset
  const secondConsumer = createConsumer(t, createTestGroupId(), {
    clientId: `test-consumer-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    groupId: createTestGroupId(), // Use different group ID
    deserializers: stringDeserializers
  })

  await secondConsumer.topics.trackAll(topic)
  await secondConsumer.joinGroup({})

  const stream = await secondConsumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.MANUAL,
    offsets: [{ topic, partition: 0, offset: secondMessageOffset! }],
    maxWaitTime: 1000
  })

  const manualMessages = []
  for await (const message of stream) {
    manualMessages.push(message)
    if (manualMessages.length >= 2) {
      break // Only consume 2 messages
    }
  }

  strictEqual(manualMessages.length, 2, 'Should consume 2 messages with MANUAL mode')
  deepStrictEqual(manualMessages[0].key, 'key-1', 'First message should be key-1')
  deepStrictEqual(manualMessages[1].key, 'key-2', 'Second message should be key-2')
})

test('MessagesStream - should support different fallback modes', async t => {
  const groupIdFail = createTestGroupId()
  const groupIdLatest = createTestGroupId()
  const groupIdEarliest = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  // Test with COMMITTED mode and FAIL fallback
  await rejects(
    async () => {
      await consumeMessages(t, groupIdFail, topic, {
        mode: MessagesStreamModes.COMMITTED,
        fallbackMode: MessagesStreamFallbackModes.FAIL
      })
    },
    (error: any) => {
      ok(error instanceof UserError)
      ok(error.message.includes('has no committed offset'), 'Error message should mention no committed offset')
      return true
    }
  )

  // Test with COMMITTED mode and LATEST fallback
  const { messages: latestMessages } = await consumeMessages(t, groupIdLatest, topic, {
    mode: MessagesStreamModes.COMMITTED,
    fallbackMode: MessagesStreamFallbackModes.LATEST
  })

  // With LATEST fallback, we should have no messages (since we haven't produced any after creating the consumer)
  strictEqual(latestMessages.length, 0, 'With LATEST fallback, should not consume any messages')

  // Test with COMMITTED mode and EARLIEST fallback
  const { messages: earliestMessages } = await consumeMessages(t, groupIdEarliest, topic, {
    mode: MessagesStreamModes.COMMITTED,
    fallbackMode: MessagesStreamFallbackModes.EARLIEST
  })

  // With EARLIEST fallback, we should get all messages
  strictEqual(earliestMessages.length, 3, 'With EARLIEST fallback, should consume all messages')
})

test('MessagesStream - should support asyncIterator interface', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  const consumer = createConsumer(t, groupId, { deserializers: stringDeserializers })

  await consumer.topics.trackAll(topic)
  await consumer.joinGroup({})

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    maxWaitTime: 1000
  })

  const messages = []
  for await (const message of stream) {
    messages.push(message)
    if (messages.length >= 3) {
      break // Only consume 3 messages
    }
  }

  strictEqual(messages.length, 3, 'Should consume 3 messages using asyncIterator')
  for (let i = 0; i < 3; i++) {
    deepStrictEqual(messages[i].key, `key-${i}`, `Message ${i} should have correct key`)
    deepStrictEqual(messages[i].value, `value-${i}`, `Message ${i} should have correct value`)
  }
})

test('MessagesStream - should support custom deserializers', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Create a producer with custom serializers (uppercase values)
  const producer = createProducer(t, {
    serializers: {
      key: (key?: string) => Buffer.from(key!),
      value: (value?: string) => Buffer.from(value!.toUpperCase()),
      headerKey: (key?: string) => Buffer.from(key!),
      headerValue: (value?: string) => Buffer.from(value!)
    },
    autocreateTopics: true
  })

  // Produce test messages
  const messages = []
  for (let i = 0; i < 3; i++) {
    messages.push({
      topic,
      key: `key-${i}`,
      value: `value-${i}`,
      headers: { headerKey: `headerValue-${i}` }
    })
  }

  await producer.send({
    messages,
    acks: ProduceAcks.LEADER
  })

  // Consume messages with custom deserializers
  const { messages: consumedMessages } = await consumeMessages(t, groupId, topic, {
    mode: MessagesStreamModes.EARLIEST,
    deserializers: {
      key: (key?: Buffer) => key?.toString(),
      value: (value?: Buffer) => value?.toString().toLowerCase(),
      headerKey: (key?: Buffer) => key?.toString(),
      headerValue: (value?: Buffer) => value?.toString()
    }
  })

  strictEqual(consumedMessages.length, 3, 'Should consume 3 messages')

  // Values should have been uppercase in Kafka but lowercased by our deserializer
  for (let i = 0; i < 3; i++) {
    deepStrictEqual(consumedMessages[i].key, `key-${i}`, `Message ${i} should have correct key`)
    deepStrictEqual(consumedMessages[i].value, `value-${i}`, `Message ${i} should have lowercase value`)
    deepStrictEqual(
      consumedMessages[i].headers.get('headerKey'),
      `headerValue-${i}`,
      `Message ${i} should have correct header`
    )
  }
})

test('MessagesStream - should properly handle close', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic, 1)

  const consumer = createConsumer(t, groupId)

  await consumer.topics.trackAll(topic)
  await consumer.joinGroup({})

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    autocommit: true,
    maxWaitTime: 1000
  })

  // Consume one message
  const message = await new Promise(resolve => {
    stream.once('data', message => {
      resolve(message)
    })
  })

  ok(message, 'Should receive at least one message')

  // Close the stream - Issue a second call before the first has completed
  const promise = new Promise<void>((resolve, reject) => {
    stream.close(error => {
      if (error) {
        reject(error)
        return
      }

      resolve()
    })
  })

  await stream.close()
  await promise

  // Wait for a bit to ensure we're properly closed
  await new Promise(resolve => setTimeout(resolve, 100))

  strictEqual(stream.destroyed, true, 'Stream should be destroyed after close')
  strictEqual(stream.readable, false, 'Stream should not be readable after close')

  // Subsequent call to close are no harm
  await stream.close()
})

test('MessagesStream - should properly handle going back and forth to empty assignments', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic, 1)

  const firstConsumer = createConsumer(t, groupId)
  const secondConsumer = createConsumer(t, groupId)

  await firstConsumer.topics.trackAll(topic)
  await firstConsumer.joinGroup({})
  const rejoinPromise = once(firstConsumer, 'consumer:group:join')
  await secondConsumer.topics.trackAll(topic)
  await secondConsumer.joinGroup({})
  await rejoinPromise

  let consumerWithAssignments = firstConsumer
  let consumerWithoutAssignments = secondConsumer

  if (firstConsumer.assignments?.length === 0) {
    consumerWithAssignments = secondConsumer
    consumerWithoutAssignments = firstConsumer
  }

  strictEqual(consumerWithAssignments.assignments!.length, 1, 'Should have one assignment')
  strictEqual(consumerWithoutAssignments.assignments!.length, 0, 'Should have no assignment')

  // Create a stream - It should not process any messages
  const stream = await consumerWithoutAssignments.consume({
    topics: [topic],
    mode: MessagesStreamModes.COMMITTED,
    fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    maxWaitTime: 1000
  })

  // Produce messages
  await produceTestMessages(t, topic, 1)

  // The stream should not receive any message since it has no assignment
  deepStrictEqual(await executeWithTimeout(once(stream, 'data'), 2000), 'timeout')

  // Remove the consumerWithAssignments - The stream should now receive messages
  await consumerWithAssignments.close()
  const [response] = await once(stream, 'data')

  deepStrictEqual(response.key, 'key-0')
})

test('MessagesStream - should handle errors from Base.metadata', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  const consumer = createConsumer(t, groupId, { deserializers: stringDeserializers })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup({})

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    maxWaitTime: 1000
  })

  mockMetadata(consumer, 2)

  const errorPromise = once(stream, 'error')
  const [dataOrError] = (await Promise.race([once(stream, 'data'), errorPromise])) as (Error | object)[]

  strictEqual(dataOrError instanceof MultipleErrors, true)
  strictEqual((dataOrError as Error).message.includes('Cannot connect to any broker.'), true)
})

test('MessagesStream - should handle errors from Consumer.fetch', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  const consumer = createConsumer(t, groupId, { deserializers: stringDeserializers })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup({})

  // Override the fetch method to simulate a connection error
  consumer.fetch = function (_options: any, callback: CallbackWithPromise<any>) {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])

    callback(connectionError, undefined)
  } as typeof consumer.fetch

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    maxWaitTime: 1000
  })

  const errorPromise = once(stream, 'error')
  const [dataOrError] = (await Promise.race([once(stream, 'data'), errorPromise])) as (Error | object)[]

  strictEqual(dataOrError instanceof MultipleErrors, true)
  strictEqual((dataOrError as Error).message.includes('Cannot connect to any broker.'), true)
})

test('MessagesStream - should handle errors from Consumer.commit', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  const consumer = createConsumer(t, groupId, { deserializers: stringDeserializers })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup({})

  // Override the fetch method to simulate a connection error
  consumer.commit = function (_options: any, callback: CallbackWithPromise<any>) {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])

    callback(connectionError, undefined)
  } as typeof consumer.commit

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    maxWaitTime: 1000,
    autocommit: 300
  })

  stream.resume()

  const [error] = await once(stream, 'error')
  strictEqual(error instanceof MultipleErrors, true)
  strictEqual(error.message.includes('Cannot connect to any broker.'), true)
})

test('MessagesStream - should handle errors from Consumer.listOffsets', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  const consumer = createConsumer(t, groupId, { deserializers: stringDeserializers })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup({})

  // Override the fetch method to simulate a connection error
  consumer.listOffsets = function (_options: any, callback: CallbackWithPromise<any>) {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])

    callback(connectionError, undefined)
  } as typeof consumer.listOffsets

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    maxWaitTime: 1000
  })

  const errorPromise = once(stream, 'error')
  const [dataOrError] = (await Promise.race([once(stream, 'data'), errorPromise])) as (Error | object)[]

  strictEqual(dataOrError instanceof MultipleErrors, true)
  strictEqual((dataOrError as Error).message.includes('Cannot connect to any broker.'), true)
})

test('MessagesStream - should handle errors from Consumer.listCommittedOffsets', async t => {
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  const consumer = createConsumer(t, groupId, { deserializers: stringDeserializers })
  await consumer.topics.trackAll(topic)
  await consumer.joinGroup({})

  // Override the fetch method to simulate a connection error
  consumer.listCommittedOffsets = function (_options: any, callback: CallbackWithPromise<any>) {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])

    callback(connectionError, undefined)
  } as typeof consumer.listCommittedOffsets

  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.COMMITTED,
    maxWaitTime: 1000
  })

  const errorPromise = once(stream, 'error')
  const [dataOrError] = (await Promise.race([once(stream, 'data'), errorPromise])) as (Error | object)[]

  strictEqual(dataOrError instanceof MultipleErrors, true)
  strictEqual((dataOrError as Error).message.includes('Cannot connect to any broker.'), true)
})

test('MessagesStream - metrics should track the number of consumed messages', async t => {
  const registry = new Prometheus.Registry()
  const groupId = createTestGroupId()
  const topic = createTestTopic()

  // Produce test messages
  await produceTestMessages(t, topic)

  {
    const { consumer, messages } = await consumeMessages(t, groupId, topic, {
      mode: MessagesStreamModes.EARLIEST,
      metrics: { registry, client: Prometheus }
    })

    // Verify messages
    strictEqual(messages.length, 3, 'Should consume 3 messages')

    const metrics = await registry.getMetricsAsJSON()
    const consumedMessages = metrics.find(m => m.name === 'kafka_consumers_messages')!

    deepStrictEqual(consumedMessages, {
      aggregator: 'sum',
      help: 'Number of consumed Kafka messages',
      name: 'kafka_consumers_messages',
      type: 'counter',
      values: [
        {
          labels: {},
          value: 3
        }
      ]
    })

    await consumer.close(true)
  }

  {
    const { messages } = await consumeMessages(t, groupId, topic, {
      mode: MessagesStreamModes.EARLIEST,
      metrics: { registry, client: Prometheus }
    })

    // Verify messages
    strictEqual(messages.length, 3, 'Should consume 3 messages')

    const metrics = await registry.getMetricsAsJSON()
    const consumedMessages = metrics.find(m => m.name === 'kafka_consumers_messages')!

    deepStrictEqual(consumedMessages.values[0].value, 6)
  }
})
