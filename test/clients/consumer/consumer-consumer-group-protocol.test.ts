import { ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { test, type TestContext } from 'node:test'
import { kClosed, kConnections, kMetadata } from '../../../src/clients/base/base.ts'
import {
  consumerGroupHeartbeatV0,
  MultipleErrors,
  ProduceAcks,
  ResponseError,
  sleep,
  TimeoutError,
  UnsupportedApiError,
  type CallbackWithPromise,
  type ClusterMetadata,
  type MessageToProduce,
  type ProducerOptions
} from '../../../src/index.ts'
import {
  createConsumer,
  createProducer,
  createTopic,
  isKafka,
  mockAPI,
  mockedErrorMessage,
  mockMetadata,
  mockUnavailableAPI,
  waitFor
} from '../../helpers.ts'

async function produceTestMessages ({
  t,
  messages,
  batchSize = 3,
  delay = 0,
  overrideOptions
}: {
  t: TestContext
  messages: MessageToProduce<string, string, string, string>[]
  batchSize?: number
  delay?: number
  overrideOptions?: Partial<ProducerOptions<Buffer, Buffer, Buffer, Buffer>>
}): Promise<void> {
  const producer = createProducer(t, overrideOptions)

  for (let i = 0; i < messages.length; i += batchSize) {
    await producer.send({
      messages: messages.slice(i, i + batchSize).map(msg => ({
        topic: msg.topic,
        key: Buffer.from(msg.key ?? ''),
        value: Buffer.from(msg.value ?? ''),
        headers: new Map(
          Object.entries(msg.headers ?? {}).map(([key, value]) => [Buffer.from(key), Buffer.from(value)])
        )
      })),
      acks: ProduceAcks.LEADER
    })
    await sleep(delay)
  }
}

const skipConsumerGroupProtocol = { skip: isKafka(['7.5.0', '7.6.0', '7.7.0', '7.8.0', '7.9.0']) }

test('consumer should consume messages with new consumer protocol', skipConsumerGroupProtocol, async t => {
  const topic = await createTopic(t, true, 3)
  await produceTestMessages({
    t,
    messages: ['1', '2', '3'].map(v => ({ topic, value: v }))
  })
  const consumer = createConsumer(t, { groupProtocol: 'consumer', maxWaitTime: 100 })
  const stream = await consumer.consume({ topics: [topic], mode: 'committed', fallbackMode: 'earliest' })
  t.after(() => stream.close())
  let count = 0
  stream.on('data', () => count++)
  await waitFor(
    () => {
      strictEqual(count, 3)
    },
    { timeout: 3000 }
  )

  ok(consumer.isActive())

  await stream.close()
})

test(
  'consumer.assignments should change on rebalance with new consumer protocol',
  skipConsumerGroupProtocol,
  async t => {
    const topic = await createTopic(t, true, 2)
    const groupId = `test-consumer-group-${randomUUID()}`

    const consumer1 = createConsumer(t, { groupId, groupProtocol: 'consumer', maxWaitTime: 100 })
    const stream1 = await consumer1.consume({ topics: [topic] })
    t.after(() => stream1.close())

    await waitFor(() => {
      strictEqual(consumer1.assignments?.length, 1)
      strictEqual(consumer1.assignments[0].partitions.length, 2)
    })

    const consumer2 = createConsumer(t, { groupId, groupProtocol: 'consumer', maxWaitTime: 100 })
    const stream2 = await consumer2.consume({ topics: [topic] })
    t.after(() => stream2.close())

    await waitFor(() => {
      strictEqual(consumer1.assignments?.length, 1)
      strictEqual(consumer1.assignments[0].partitions.length, 1)
    })
    await Promise.all([stream1.close(), stream2.close()])
  }
)

test('consumer should handle fenced member epoch error', skipConsumerGroupProtocol, async t => {
  const topic = await createTopic(t, true)
  const consumer = createConsumer(t, { groupProtocol: 'consumer', maxWaitTime: 100 })
  const stream = await consumer.consume({ topics: [topic] })
  t.after(() => stream.close())

  const heartbeatError = once(consumer, 'consumer:heartbeat:error')

  mockAPI(
    consumer[kConnections],
    consumerGroupHeartbeatV0.api.key,
    new ResponseError(
      consumerGroupHeartbeatV0.api.key,
      0,
      { '/': [110, null] },
      {
        throttleTimeMs: 0,
        errorCode: 110,
        errorMessage: 'fenced',
        memberId: '',
        memberEpoch: 0,
        heartbeatIntervalMs: 100,
        assignment: null
      }
    )
  )

  const [{ error }] = await heartbeatError
  strictEqual(error.response.errorCode, 110)
  await stream.close()
})

test('#consumerGroupHeartbeat should handle unavailable API errors', skipConsumerGroupProtocol, async t => {
  const consumer = createConsumer(t, { groupProtocol: 'consumer', maxWaitTime: 100 })
  const topic = await createTopic(t, true, 1)

  mockUnavailableAPI(consumer, 'ConsumerGroupHeartbeat')

  try {
    await consumer.consume({ topics: [topic] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    ok(error instanceof UnsupportedApiError)
    ok(error.message.includes('Unsupported API ConsumerGroupHeartbeat.'))
  }
})

test('#consumerGroupHeartbeat should ignore response when closed ', skipConsumerGroupProtocol, async t => {
  const consumer = createConsumer(t, { groupProtocol: 'consumer', maxWaitTime: 100 })
  const topic = await createTopic(t, true, 1)
  const stream = await consumer.consume({ topics: [topic] })
  consumer[kClosed] = true
  await once(consumer, 'consumer:heartbeat:end')
  consumer[kClosed] = false
  await stream.close()
})

test('#consumerGroupHeartbeat should timeout and schedule another heartbeat', skipConsumerGroupProtocol, async t => {
  const consumer = createConsumer(t, { groupProtocol: 'consumer', maxWaitTime: 100 })
  const topic = await createTopic(t, true, 1)
  const stream = await consumer.consume({ topics: [topic] })
  mockAPI(consumer[kConnections], consumerGroupHeartbeatV0.api.key, null, null, (
    _originalSend,
    _apiKey,
    _apiVersion,
    _payload,
    _responseParser,
    _hasRequestHeaderTaggedFields,
    _hasResponseHeaderTaggedFields,
    callback
  ) => {
    callback(new TimeoutError('Request timed out'), null)
    return false
  })
  await once(consumer, 'consumer:heartbeat:error')
  await once(consumer, 'consumer:heartbeat:end')
  await stream.close()
})

test('#leaveGroupNewProtocol should handle unavailable API errors', skipConsumerGroupProtocol, async t => {
  const consumer = createConsumer(t, { groupProtocol: 'consumer', maxWaitTime: 100 })
  const topic = await createTopic(t, true, 1)
  const stream = await consumer.consume({ topics: [topic] })
  mockUnavailableAPI(consumer, 'ConsumerGroupHeartbeat', false)
  await stream.close()
})

test('#updateAssignments should handle metadata error', skipConsumerGroupProtocol, async t => {
  const consumer = createConsumer(t, { groupProtocol: 'consumer', maxWaitTime: 100 })
  const topic = await createTopic(t, true, 1)

  let metadataErrorDelivered = false

  // #updateAssignments is invoked synchronously while a ConsumerGroupHeartbeat response is being
  // handled, so the metadata mock is armed only for the duration of that handling. Arming it from a
  // 'consumer:heartbeat:start' listener instead leaves it live across the whole request/response
  // round trip, where the stream's own metadata calls can consume it and destroy the stream with the
  // mocked error - which surfaced as an uncaught exception under load. See #341.
  mockAPI(consumer[kConnections], consumerGroupHeartbeatV0.api.key, null, null, (
    originalSend,
    apiKey,
    apiVersion,
    payload,
    responseParser,
    hasRequestHeaderTaggedFields,
    hasResponseHeaderTaggedFields,
    callback
  ) => {
    originalSend(
      apiKey,
      apiVersion,
      payload,
      responseParser,
      hasRequestHeaderTaggedFields,
      hasResponseHeaderTaggedFields,
      (error: Error | null, response: unknown) => {
        if (metadataErrorDelivered) {
          callback(error, response)
          return
        }

        const originalMetadata = consumer[kMetadata]

        mockMetadata(consumer, 1, undefined, undefined, (_original, ...args: unknown[]) => {
          metadataErrorDelivered = true
          ;(args.at(-1) as CallbackWithPromise<ClusterMetadata>)(
            new MultipleErrors(mockedErrorMessage, [new Error(mockedErrorMessage + ' (internal)')], {
              canRetry: false
            })
          )
          return false
        })

        try {
          callback(error, response)
        } finally {
          consumer[kMetadata] = originalMetadata
        }
      }
    )

    return true
  })

  const stream = await consumer.consume({ topics: [topic] })
  await waitFor(() => ok(metadataErrorDelivered), { timeout: 10000 })

  // The error was handled rather than thrown, and the assignments were left untouched
  strictEqual(consumer.assignments, null)

  await stream.close()
})
