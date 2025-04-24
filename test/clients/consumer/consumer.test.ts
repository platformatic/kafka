import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { test } from 'node:test'
import { kConnections, kFetchConnections, kOptions } from '../../../src/clients/base/base.ts'
import { TopicsMap } from '../../../src/clients/consumer/topics-map.ts'
import {
  Consumer,
  fetchV17,
  findCoordinatorV6,
  heartbeatV4,
  joinGroupV9,
  leaveGroupV5,
  MessagesStream,
  MultipleErrors,
  NetworkError,
  offsetCommitV9,
  offsetFetchV9,
  ProtocolError,
  syncGroupV5,
  UserError
} from '../../../src/index.ts'
import {
  createConsumer,
  createGroupId,
  createTopic,
  mockAPI,
  mockConnectionPoolGet,
  mockConnectionPoolGetFirstAvailable,
  mockMetadata,
  mockMethod
} from '../../helpers.ts'

test('constructor should initialize properly with default options', t => {
  const consumer = createConsumer(t)

  // Verify instance type
  strictEqual(consumer instanceof Consumer, true)
  strictEqual(consumer.closed, false)

  // Verify group properties
  strictEqual(typeof consumer.groupId, 'string')
  strictEqual(consumer.generationId, 0)
  strictEqual(consumer.memberId, null)
  deepStrictEqual(consumer.assignments, null)
  strictEqual(consumer.topics instanceof TopicsMap, true)

  // Clean up
  consumer.close()
})

test('constructor should initialize with custom options', t => {
  const groupId = `custom-group-${randomUUID()}`
  const consumer = createConsumer(t, {
    groupId,
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    heartbeatInterval: 2000,
    protocols: [{ name: 'customprotocol', version: 2 }],
    minBytes: 100,
    maxBytes: 5242880, // 5MB
    maxWaitTime: 3000,
    isolationLevel: 'READ_UNCOMMITTED',
    highWaterMark: 512
  })

  // Verify instance properties
  strictEqual(consumer.groupId, groupId)
  strictEqual(consumer.generationId, 0)
  strictEqual(consumer.memberId, null)
  deepStrictEqual(consumer.assignments, null)
  strictEqual(consumer.topics instanceof TopicsMap, true)

  // Clean up
  consumer.close()
})

test('constructor should throw on invalid options when strict mode is enabled', () => {
  // Test with missing required groupId
  try {
    // @ts-expect-error - Intentionally passing invalid options
    // eslint-disable-next-line no-new
    new Consumer({
      clientId: 'test-consumer',
      bootstrapBrokers: ['localhost:29092'],
      strict: true
    })
    throw new Error('Should have thrown for missing groupId')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with invalid sessionTimeout
  try {
    // eslint-disable-next-line no-new
    new Consumer({
      clientId: 'test-consumer',
      bootstrapBrokers: ['localhost:29092'],
      groupId: 'test-group',
      // @ts-expect-error - Intentionally passing invalid option
      sessionTimeout: 'not-a-number',
      strict: true
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('sessionTimeout'), true)
  }

  // Test with invalid negative sessionTimeout value
  try {
    // eslint-disable-next-line no-new
    new Consumer({
      clientId: 'test-consumer',
      bootstrapBrokers: ['localhost:29092'],
      groupId: 'test-group',
      sessionTimeout: -1, // Negative value
      strict: true
    })
    throw new Error('Should have thrown for negative sessionTimeout')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('sessionTimeout'), true)
  }

  // Test with invalid protocols type
  try {
    // eslint-disable-next-line no-new
    new Consumer({
      clientId: 'test-consumer',
      bootstrapBrokers: ['localhost:29092'],
      groupId: 'test-group',
      // @ts-expect-error - Intentionally passing invalid option
      protocols: 'not-an-array',
      strict: true
    })
    throw new Error('Should have thrown for invalid protocols')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('protocols'), true)
  }

  // Valid options should not throw
  const consumer = new Consumer({
    clientId: 'test-consumer',
    bootstrapBrokers: ['localhost:9092'],
    groupId: 'test-group',
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    heartbeatInterval: 2000,
    strict: true
  })

  strictEqual(consumer instanceof Consumer, true)
  consumer.close()
})

test('constructor should validate group options relationship', () => {
  // Test with rebalanceTimeout < sessionTimeout (invalid)
  try {
    // eslint-disable-next-line no-new
    new Consumer({
      clientId: 'test-consumer',
      bootstrapBrokers: ['localhost:29092'],
      groupId: 'test-group',
      sessionTimeout: 30000,
      rebalanceTimeout: 20000 // Less than sessionTimeout
    })
    throw new Error('Should have thrown for rebalanceTimeout < sessionTimeout')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, '/options/rebalanceTimeout must be greater than or equal to /options/sessionTimeout.')
  }

  // Test with heartbeatInterval > sessionTimeout and heartbeatInterval > rebalanceTimeout (invalid)
  try {
    // eslint-disable-next-line no-new
    new Consumer({
      clientId: 'test-consumer',
      bootstrapBrokers: ['localhost:29092'],
      groupId: 'test-group',
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      heartbeatInterval: 40000 // Greater than sessionTimeout
    })
    throw new Error('Should have thrown for heartbeatInterval > sessionTimeout')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, '/options/heartbeatInterval must be less than or equal to /options/sessionTimeout.')
  }

  // Test with heartbeatInterval > rebalanceTimeout (invalid)
  try {
    // eslint-disable-next-line no-new
    new Consumer({
      clientId: 'test-consumer',
      bootstrapBrokers: ['localhost:29092'],
      groupId: 'test-group',
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      heartbeatInterval: 70000 // Greater than rebalanceTimeout
    })
    throw new Error('Should have thrown for heartbeatInterval > rebalanceTimeout')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(
      error.message,
      '/options/heartbeatInterval must be less than or equal to /options/sessionTimeout, /options/heartbeatInterval must be less than or equal to /options/rebalanceTimeout.'
    )
  }

  // Valid relationship between options
  const consumer = new Consumer({
    clientId: 'test-consumer',
    bootstrapBrokers: ['localhost:9092'],
    groupId: 'test-group',
    sessionTimeout: 30000,
    rebalanceTimeout: 60000,
    heartbeatInterval: 5000 // Valid: less than both sessionTimeout and rebalanceTimeout
  })

  strictEqual(consumer instanceof Consumer, true)
  consumer.close()
})

test('close should properly clean up resources and set closed state', async t => {
  const consumer = createConsumer(t)

  // Verify initial state
  strictEqual(consumer.closed, false)

  // Close the consumer
  await consumer.close()

  // Verify closed state
  strictEqual(consumer.closed, true)
})

test('close should support both promise and callback API', t => {
  return new Promise<void>((resolve, reject) => {
    const consumer = createConsumer(t)

    // Use callback API
    consumer.close(err => {
      if (err) {
        reject(err)
        return
      }

      // Verify consumer is closed
      strictEqual(consumer.closed, true)

      // Create another consumer to test promise API
      const promiseConsumer = createConsumer(t)

      // Use promise API
      promiseConsumer
        .close()
        .then(() => {
          strictEqual(promiseConsumer.closed, true)
          resolve()
        })
        .catch(reject)
    })
  })
})

test('close should leave consumer group if currently joined', async t => {
  const consumer = createConsumer(t)

  // Join a group first
  await consumer.joinGroup({})

  // Verify we're in a group
  strictEqual(typeof consumer.memberId, 'string')
  ok(consumer.memberId!.length > 0, 'Member ID should be a non-empty string')
  strictEqual(consumer.generationId > 0, true, 'Generation ID should be greater than 0')

  // Close the consumer
  await consumer.close()

  // Verify we're no longer in a group and the consumer is closed
  strictEqual(consumer.memberId, null, 'Member ID should be reset to null')
  strictEqual(consumer.generationId, 0, 'Generation ID should be reset to 0')
  strictEqual(consumer.closed, true, 'Consumer should be marked as closed')
})

test('close should be idempotent - calling it multiple times has no effect', async t => {
  const consumer = createConsumer(t)

  // Close the consumer
  await consumer.close()
  strictEqual(consumer.closed, true)

  // Close it again
  await consumer.close()
  strictEqual(consumer.closed, true)

  // And again
  await consumer.close()
  strictEqual(consumer.closed, true)
})

test('close should handle errors from leaveGroup', async t => {
  const consumer = createConsumer(t)

  // Join a group first
  await consumer.joinGroup({})

  mockAPI(consumer[kConnections], leaveGroupV5.api.key)

  // Attempt to find coordinator with the mocked connection
  try {
    await consumer.close()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('close should handle errors from ConnectionPool.close', async t => {
  const consumer = createConsumer(t)

  // Join a group first
  await consumer.joinGroup({})

  // Mock the connection to fail
  mockMethod(
    consumer[kFetchConnections],
    'close',
    1,
    new MultipleErrors('Cannot close the pool.', [new Error('Cannot close the pool.')])
  )

  // Attempt to find coordinator with the mocked connection
  try {
    await consumer.close()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot close the pool.'), true)
  }
})

test('close with force=true should force close all resources', async t => {
  const consumer = createConsumer(t)

  // Create a stream
  const stream = await consumer.consume({ topics: [] })

  // Close with force=true
  await consumer.close(true)

  // Verify consumer is closed regardless of active streams
  strictEqual(consumer.closed, true)
  strictEqual(consumer.memberId, null)
  strictEqual(consumer.generationId, 0)

  // Verify the stream was closed
  strictEqual(stream.closed, true)
})

test('consume should return a MessagesStream instance', async t => {
  const consumer = createConsumer(t)

  // Call consume with empty topics array
  consumer[kOptions].autocommit = undefined
  const stream = await consumer.consume({ topics: [] })

  // Verify the stream is a MessagesStream instance
  ok(stream instanceof MessagesStream, 'Should return a MessagesStream instance')
  strictEqual(stream.closed, false, 'Stream should not be closed initially')
})

test('consume should automatically join the group if not already joined', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  // Initially we're not in a group
  strictEqual(consumer.memberId, null)

  // Call consume which should join the group
  const stream = await consumer.consume({ topics: [topic] })

  // Verify we're now in a group
  strictEqual(typeof consumer.memberId, 'string')
  ok(consumer.memberId!.length > 0, 'Should have a valid member ID')
  strictEqual(consumer.generationId > 0, true, 'Should have a valid generation ID')

  // Clean up
  await stream.close()
})

test('consume should support both promise and callback API', t => {
  return new Promise<void>((resolve, reject) => {
    const consumer = createConsumer(t)

    // Use callback API
    consumer.consume({ topics: [] }, (err, stream) => {
      if (err) {
        reject(err)
        return
      }

      // Verify returned stream
      ok(stream instanceof MessagesStream, 'Should return a MessagesStream instance')

      // Now try with Promise API
      const promiseConsumer = createConsumer(t)

      promiseConsumer
        .consume({ topics: [] })
        .then(promiseStream => {
          ok(promiseStream instanceof MessagesStream, 'Should return a MessagesStream instance')

          // Clean up
          return Promise.all([stream.close(), promiseStream.close()])
        })
        .then(() => resolve())
        .catch(reject)
    })
  })
})

test('consume should fail when consumer is closed', async t => {
  const consumer = createConsumer(t)

  // Close the consumer first
  await consumer.close()

  // Attempt to consume on a closed consumer
  try {
    await consumer.consume({ topics: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof NetworkError, true)
    strictEqual(error.message, 'Client is closed.')
  }
})

test('consume should validate the supplied options', async t => {
  const consumer = createConsumer(t, { strict: true })

  // Test with invalid topics type
  try {
    await consumer.consume({
      // @ts-expect-error - Intentionally passing invalid option
      topics: 'not-an-array'
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid autocommit type
  try {
    await consumer.consume({
      topics: [],
      // @ts-expect-error - Intentionally passing invalid option
      autocommit: 'not-a-boolean'
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('autocommit'), true)
  }

  // Test with invalid maxBytes type
  try {
    await consumer.consume({
      topics: [],
      // @ts-expect-error - Intentionally passing invalid option
      maxBytes: 'not-a-number'
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('maxBytes'), true)
  }
})

test('consume should track topics in the consumer', async t => {
  const consumer = createConsumer(t)
  const testTopic = await createTopic(t, true)

  // Mock the topics.track method to verify it's called
  let called = false
  mockMethod(consumer.topics, 'track', 1, null, null, (original, ...args) => {
    called = true
    original(...args)
  })

  // Call consume with our test topic
  const stream = await consumer.consume({ topics: [testTopic] })

  // Verify topics are tracked
  strictEqual(called, true, 'Should call topics.track')
  strictEqual(consumer.topics.has(testTopic), true, 'Topic should be tracked')

  // Clean up
  await stream.close()
})

test('consume should join group if needed due to new topic', async t => {
  const consumer = createConsumer(t)
  const testTopic1 = await createTopic(t, true)
  const testTopic2 = await createTopic(t, true)

  // First join with one topic
  await consumer.joinGroup({})

  // Spy on joinGroup to see if it's called again
  let called = false
  mockMethod(consumer, 'joinGroup', 1, null, null, (original, ...args) => {
    called = true
    original(...args)
  })

  // Consume with a new topic should trigger rejoin
  const stream = await consumer.consume({ topics: [testTopic1, testTopic2] })

  // Verify join was triggered
  strictEqual(called, true, 'Should call joinGroup for new topic')

  // Clean up
  await stream.close()
})

test('consume should add stream to internal streams set', async t => {
  const consumer = createConsumer(t)

  // Call consume
  const stream = await consumer.consume({ topics: [] })

  // Verify stream is in the internal streams set
  deepStrictEqual(consumer.streamsCount, 1)

  // Clean up
  await stream.close()

  // Verify stream is removed from set when closed
  deepStrictEqual(consumer.streamsCount, 0)
})

test('consume should handle errors from joinGroup', async t => {
  const consumer = createConsumer(t)

  mockMethod(consumer, 'joinGroup')

  // Attempt to consume
  try {
    await consumer.consume({ topics: ['test-topic'] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Cannot connect to any broker.')
  }
})

test('consume should integrate with custom deserializers', async t => {
  // Define custom string deserializers
  const stringDeserializers = {
    key: (buffer?: Buffer) => buffer!.toString('utf8'),
    value: (buffer?: Buffer) => buffer!.toString('utf8'),
    headerKey: (buffer?: Buffer) => buffer!.toString('utf8'),
    headerValue: (buffer?: Buffer) => buffer!.toString('utf8')
  }

  // Create consumer with custom deserializers
  const consumer = createConsumer<string, string, string, string>(t, { deserializers: stringDeserializers })

  // Call consume with the same deserializers
  const stream = await consumer.consume({ topics: [] })

  // Verify stream is created with string types
  ok(stream instanceof MessagesStream, 'Should return a MessagesStream instance')

  // Clean up
  await stream.close()
})

test('fetch should return data', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  const metadata = await consumer.metadata({ topics: [topic] })
  const topicInfo = metadata.topics.get(topic)!

  const response = await consumer.fetch({
    node: topicInfo.partitions[0].leader,
    maxWaitTime: 1000,
    topics: [
      {
        topicId: topicInfo.id,
        partitions: [
          {
            partition: 0,
            currentLeaderEpoch: 0,
            fetchOffset: 0n,
            lastFetchedEpoch: 0,
            partitionMaxBytes: 1048576
          }
        ]
      }
    ]
  })

  response.sessionId = 0
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    sessionId: 0,
    responses: [
      {
        topicId: topicInfo.id,
        partitions: [
          {
            partitionIndex: 0,
            errorCode: 0,
            highWatermark: 0n,
            lastStableOffset: 0n,
            logStartOffset: 0n,
            abortedTransactions: [],
            preferredReadReplica: -1
          }
        ]
      }
    ]
  })
})

test('fetch should support both promise and callback API', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  const metadata = await consumer.metadata({ topics: [topic] })
  const topicInfo = metadata.topics.get(topic)!

  await new Promise<void>((resolve, reject) => {
    const consumer = createConsumer(t)

    // First test callback API
    consumer.fetch(
      {
        node: topicInfo.partitions[0].leader,
        maxWaitTime: 1000,
        topics: [
          {
            topicId: topicInfo.id,
            partitions: [
              {
                partition: 0,
                currentLeaderEpoch: 0,
                fetchOffset: 0n,
                lastFetchedEpoch: 0,
                partitionMaxBytes: 1048576
              }
            ]
          }
        ]
      },
      (err, result) => {
        if (err) {
          reject(err)
          return
        }

        strictEqual(result.errorCode, 0)
        strictEqual(Array.isArray(result.responses), true)

        // Now test Promise API
        consumer
          .fetch({
            node: topicInfo.partitions[0].leader,
            topics: [
              {
                topicId: topicInfo.id,
                partitions: [
                  {
                    partition: 0,
                    currentLeaderEpoch: 0,
                    fetchOffset: 0n,
                    lastFetchedEpoch: 0,
                    partitionMaxBytes: 1048576
                  }
                ]
              }
            ]
          })
          .then(promiseResult => {
            strictEqual(promiseResult.errorCode, 0)
            strictEqual(Array.isArray(promiseResult.responses), true)
            resolve()
          })
          .catch(reject)
      }
    )
  })
})

test('fetch should fail when consumer is closed', async t => {
  const consumer = createConsumer(t)

  // Close the consumer first
  await consumer.close()

  // Attempt to fetch with a closed consumer
  try {
    await consumer.fetch({
      node: 0,
      topics: [
        {
          topicId: 'test-topic',
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 0,
              fetchOffset: 0n,
              lastFetchedEpoch: 0,
              partitionMaxBytes: 1048576
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof NetworkError, true)
    strictEqual(error.message, 'Client is closed.')
  }
})

test('fetch should validate the supplied options', async t => {
  const consumer = createConsumer(t, { strict: true })

  // Test with missing node
  try {
    // @ts-expect-error - intentionally omitting required field
    await consumer.fetch({
      topics: [
        {
          topicId: 'test-topic',
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 0,
              fetchOffset: 0n,
              lastFetchedEpoch: 0,
              partitionMaxBytes: 1048576
            }
          ]
        }
      ]
    })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('node'), true)
  }

  // Test with missing topics
  try {
    // @ts-expect-error - intentionally omitting required field
    await consumer.fetch({ node: 0 })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid topics type
  try {
    // @ts-expect-error - intentionally passing invalid type
    await consumer.fetch({ node: 0, topics: 'not-an-array' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('topics'), true)
  }
})

test('fetch should handle errors from Connection.get', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  mockMetadata(consumer, 1, null, {
    brokers: new Map([[0, { nodeId: 0, host: 'localhost', port: 29092 }]])
  })

  mockConnectionPoolGet(consumer[kFetchConnections], 1)

  // Attempt to fetch with the mocked connection
  try {
    await consumer.fetch({
      node: 0,
      topics: [
        {
          topicId: topic,
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 0,
              fetchOffset: 0n,
              lastFetchedEpoch: 0,
              partitionMaxBytes: 1048576
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('fetch should handle errors from Base.metadata', async t => {
  const consumer = createConsumer(t)

  mockMetadata(consumer, 1)

  // Attempt to fetch with mocked metadata
  try {
    await consumer.fetch({
      node: 0,
      topics: [
        {
          topicId: 'test-topic',
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 0,
              fetchOffset: 0n,
              lastFetchedEpoch: 0,
              partitionMaxBytes: 1048576
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('fetch should handle missing nodes from Base.metadata', async t => {
  const consumer = createConsumer(t)

  // Mock metadata to fail
  mockMetadata(consumer, 1, null, {
    brokers: new Map([[0, { nodeId: 0, host: 'localhost', port: 29092 }]])
  })

  // Attempt to fetch with mocked metadata
  try {
    await consumer.fetch({
      node: 1,
      topics: [
        {
          topicId: 'test-topic',
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 0,
              fetchOffset: 0n,
              lastFetchedEpoch: 0,
              partitionMaxBytes: 1048576
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('Cannot find broker with node id 1'), true)
  }
})

test('fetch should handle errors from the API', async t => {
  const consumer = createConsumer(t)

  mockMetadata(consumer, 1, null, {
    brokers: new Map([[0, { nodeId: 0, host: 'localhost', port: 29092 }]])
  })

  mockAPI(consumer[kFetchConnections], fetchV17.api.key)

  // Attempt to fetch with the mocked API
  try {
    await consumer.fetch({
      node: 0,
      topics: [
        {
          topicId: 'test-topic',
          partitions: [
            {
              partition: 0,
              currentLeaderEpoch: 0,
              fetchOffset: 0n,
              lastFetchedEpoch: 0,
              partitionMaxBytes: 1048576
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('commit should commit offsets to Kafka', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  // First join the group
  await consumer.joinGroup({})

  // Commit offsets
  await consumer.commit({
    offsets: [{ topic, partition: 0, offset: 100n, leaderEpoch: 0 }]
  })

  // Verification is implicit - if commit doesn't throw, it succeeded
  strictEqual(consumer.closed, false)
})

test('commit should support both promise and callback API', async t => {
  const topic = await createTopic(t, true)

  await new Promise<void>((resolve, reject) => {
    const consumer = createConsumer(t)

    consumer
      .joinGroup({})
      .then(() => {
        const commitOptions = {
          offsets: [{ topic, partition: 0, offset: 100n, leaderEpoch: 0 }]
        }

        // Test callback API
        consumer.commit(commitOptions, err => {
          if (err) {
            reject(err)
            return
          }

          // Now test Promise API
          consumer
            .commit(commitOptions)
            .then(() => resolve())
            .catch(reject)
        })
      })
      .catch(reject)
  })
})

test('commit should fail when consumer is closed', async t => {
  const consumer = createConsumer(t)

  // Close the consumer
  await consumer.close()

  // Attempt to commit on a closed consumer
  try {
    await consumer.commit({
      offsets: [{ topic: 'test-topic', partition: 0, offset: 100n, leaderEpoch: 0 }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof NetworkError, true)
    strictEqual(error.message, 'Client is closed.')
  }
})

test('commit should validate the supplied options', async t => {
  const consumer = createConsumer(t, { strict: true })

  // Test with missing offsets array
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await consumer.commit({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('offsets'), true)
  }

  // Test with invalid offset type
  try {
    await consumer.commit({
      offsets: [
        {
          topic: 'test-topic',
          partition: 0,
          // @ts-expect-error - Intentionally passing invalid option
          offset: 'not-a-bigint',
          leaderEpoch: 0
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('offset'), true)
  }
})

test('commit should handle errors from the API (findGroupCoordinator)', async t => {
  const consumer = createConsumer(t)

  mockAPI(consumer[kConnections], findCoordinatorV6.api.key)

  // Attempt to commit with mocked error
  try {
    await consumer.commit({
      offsets: [{ topic: 'test-topic', partition: 0, offset: 100n, leaderEpoch: 0 }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message, 'Cannot connect to any broker.')
  }
})

test('commit should handle errors from Base.metadata', async t => {
  const consumer = createConsumer(t)

  // First join the group to set memberId
  await consumer.joinGroup({})

  mockMetadata(consumer)

  // Attempt to commit with mocked error
  try {
    await consumer.commit({
      offsets: [{ topic: 'test-topic', partition: 0, offset: 100n, leaderEpoch: 0 }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message, 'Cannot connect to any broker.')
  }
})

test('commit should handle errors from the API (offsetCommit)', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  // First join the group
  await consumer.joinGroup({})

  mockAPI(consumer[kConnections], offsetCommitV9.api.key)

  // Attempt to commit with mocked error
  try {
    await consumer.commit({
      offsets: [{ topic, partition: 0, offset: 100n, leaderEpoch: 0 }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof Error, true)
    strictEqual(error.message, 'Cannot connect to any broker.')
  }
})

test('listOffsets should return offset values for topics and partitions', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true, 2)

  // Get offsets for the test topic
  const offsets = await consumer.listOffsets({ topics: [topic] })

  // Verify the offsets structure
  strictEqual(offsets instanceof Map, true, 'Should return a Map of offsets')
  strictEqual(offsets.has(topic), true, 'Should contain the requested topic')

  const topicOffsets = offsets.get(topic)!
  strictEqual(Array.isArray(topicOffsets), true, 'Topic offsets should be an array')
  strictEqual(topicOffsets.length, 2, 'Should have offsets for all partitions')

  // For new topics, offsets should typically be 0
  strictEqual(typeof topicOffsets[0], 'bigint', 'Offset should be a bigint')
  strictEqual(typeof topicOffsets[1], 'bigint', 'Offset should be a bigint')
})

test('listOffsets should support both promise and callback API', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  // Test callback API
  await new Promise<void>((resolve, reject) => {
    consumer.listOffsets({ topics: [topic] }, (err, offsets) => {
      if (err) {
        reject(err)
        return
      }

      // Verify structure
      strictEqual(offsets instanceof Map, true, 'Should return a Map of offsets')
      strictEqual(offsets.has(topic), true, 'Should contain the requested topic')

      // Now test the promise API
      consumer
        .listOffsets({ topics: [topic] })
        .then(promiseOffsets => {
          strictEqual(promiseOffsets instanceof Map, true, 'Promise API should return a Map of offsets')
          strictEqual(promiseOffsets.has(topic), true, 'Promise result should contain the requested topic')
          resolve()
        })
        .catch(reject)
    })
  })
})

test('listOffsets should fail when consumer is closed', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  // Close the consumer first
  await consumer.close()

  // Attempt to list offsets with a closed consumer
  try {
    await consumer.listOffsets({ topics: [topic] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof NetworkError, true)
    strictEqual(error.message, 'Client is closed.')
  }
})

test('listOffsets should validate the supplied options', async t => {
  const consumer = createConsumer(t, { strict: true })

  // Test with missing topics
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await consumer.listOffsets({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid topics type
  try {
    await consumer.listOffsets({
      // @ts-expect-error - Intentionally passing invalid option
      topics: 'not-an-array'
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid timestamp type
  try {
    await consumer.listOffsets({
      topics: ['test-topic'],
      // @ts-expect-error - Intentionally passing invalid option
      timestamp: 'not-a-bigint'
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('timestamp'), true)
  }
})

test('listOffsets should handle errors from Base.metadata', async t => {
  const consumer = createConsumer(t)

  mockMetadata(consumer)

  // Attempt to list offsets with the mocked error
  try {
    await consumer.listOffsets({ topics: ['test-topic'] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('listOffsets should handle errors from Connection.get', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)
  await consumer.metadata({ topics: [topic] })

  mockConnectionPoolGet(consumer[kConnections], 1)

  // Attempt to list offsets with the mocked connection
  try {
    await consumer.listOffsets({ topics: [topic] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Listing offsets failed.'), true)
  }
})

test('listOffsets should use custom isolation level when provided', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  // Use a specific isolation level
  const isolationLevel = 'READ_COMMITTED'
  const offsets = await consumer.listOffsets({ topics: [topic], isolationLevel })

  // Verification is implicit - if the call doesn't throw, it succeeded
  strictEqual(offsets instanceof Map, true, 'Should return a Map of offsets')
  strictEqual(offsets.has(topic), true, 'Should contain the requested topic')
})

test('listCommittedOffsets should return committed offset values for topics and partitions', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true, 2)

  // First join the group
  await consumer.joinGroup({})

  // Commit some offsets
  await consumer.commit({
    offsets: [
      { topic, partition: 0, offset: 100n, leaderEpoch: 0 },
      { topic, partition: 1, offset: 200n, leaderEpoch: 0 }
    ]
  })

  // Get the committed offsets for the test topic
  const committed = await consumer.listCommittedOffsets({
    topics: [{ topic, partitions: [0, 1] }]
  })

  // Verify the offsets structure
  strictEqual(committed instanceof Map, true, 'Should return a Map of offsets')
  strictEqual(committed.has(topic), true, 'Should contain the requested topic')

  const topicOffsets = committed.get(topic)!
  strictEqual(Array.isArray(topicOffsets), true, 'Topic offsets should be an array')
  strictEqual(topicOffsets.length, 2, 'Should have offsets for all partitions')

  // Verify the committed offsets match what we committed
  strictEqual(topicOffsets[0], 100n, 'Partition 0 offset should be 100n')
  strictEqual(topicOffsets[1], 200n, 'Partition 1 offset should be 200n')
})

test('listCommittedOffsets should support both promise and callback API', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  // First join the group
  await consumer.joinGroup({})

  // Commit an offset
  await consumer.commit({
    offsets: [{ topic, partition: 0, offset: 100n, leaderEpoch: 0 }]
  })

  // Test callback API
  await new Promise<void>((resolve, reject) => {
    consumer.listCommittedOffsets({ topics: [{ topic, partitions: [0] }] }, (err, committed) => {
      if (err) {
        reject(err)
        return
      }

      // Verify structure
      strictEqual(committed instanceof Map, true, 'Should return a Map of offsets')
      strictEqual(committed.has(topic), true, 'Should contain the requested topic')

      // Now test the promise API
      consumer
        .listCommittedOffsets({ topics: [{ topic, partitions: [0] }] })
        .then(promiseOffsets => {
          strictEqual(promiseOffsets instanceof Map, true, 'Promise API should return a Map of offsets')
          strictEqual(promiseOffsets.has(topic), true, 'Promise result should contain the requested topic')
          resolve()
        })
        .catch(reject)
    })
  })
})

test('listCommittedOffsets should fail when consumer is closed', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true)

  // First join the group and commit an offset
  await consumer.joinGroup({})
  await consumer.commit({
    offsets: [{ topic, partition: 0, offset: 100n, leaderEpoch: 0 }]
  })

  // Close the consumer
  await consumer.close()

  // Attempt to list committed offsets with a closed consumer
  try {
    await consumer.listCommittedOffsets({ topics: [{ topic, partitions: [0] }] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof NetworkError, true)
    strictEqual(error.message, 'Client is closed.')
  }
})

test('listCommittedOffsets should validate the supplied options', async t => {
  const consumer = createConsumer(t, { strict: true })
  await consumer.joinGroup({})

  // Test with missing topics
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await consumer.listCommittedOffsets({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid topics type
  try {
    await consumer.listCommittedOffsets({
      // @ts-expect-error - Intentionally passing invalid option
      topics: 'not-an-array'
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid topic object (missing partitions)
  try {
    await consumer.listCommittedOffsets({
      topics: [
        {
          topic: 'test-topic',
          // @ts-expect-error - Intentionally omitting required field
          wrongField: []
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('partitions'), true)
  }
})

test('listCommittedOffsets should handle errors from the API', async t => {
  const consumer = createConsumer(t)
  const topic = await createTopic(t, true, 2)

  mockAPI(consumer[kConnections], offsetFetchV9.api.key)

  // First join the group
  await consumer.joinGroup({})

  // Commit some offsets
  await consumer.commit({
    offsets: [
      { topic, partition: 0, offset: 100n, leaderEpoch: 0 },
      { topic, partition: 1, offset: 200n, leaderEpoch: 0 }
    ]
  })

  try {
    await consumer.listCommittedOffsets({
      topics: [{ topic, partitions: [0, 1] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('findGroupCoordinator should return the coordinator nodeId', async t => {
  const consumer = createConsumer(t)

  // First call - should find the coordinator
  const coordinatorId = await consumer.findGroupCoordinator()

  // Verify the result is a positive number
  strictEqual(typeof coordinatorId, 'number')
  ok(coordinatorId >= 0, 'Coordinator ID should be a non-negative number')

  // Second call - should return the cached coordinator
  const cachedCoordinatorId = await consumer.findGroupCoordinator()

  // Should be the same coordinator ID from the cache
  strictEqual(cachedCoordinatorId, coordinatorId)
})

test('findGroupCoordinator should support both promise and callback API', t => {
  return new Promise<void>((resolve, reject) => {
    const consumer = createConsumer(t)

    // Use callback API
    consumer.findGroupCoordinator((err, coordinatorId) => {
      if (err) {
        reject(err)
        return
      }

      // Verify the result is a positive number
      strictEqual(typeof coordinatorId, 'number')
      ok(coordinatorId >= 0, 'Coordinator ID should be a non-negative number')

      // Now try with Promise API to verify both work
      consumer
        .findGroupCoordinator()
        .then(promiseCoordinatorId => {
          // Should be the same coordinator ID from the cache
          strictEqual(promiseCoordinatorId, coordinatorId)
          resolve()
        })
        .catch(reject)
    })
  })
})

test('findGroupCoordinator should fail when consumer is closed', async t => {
  const consumer = createConsumer(t)

  // Close the consumer first
  await consumer.close()

  // Attempt to find coordinator on a closed consumer
  try {
    await consumer.findGroupCoordinator()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof NetworkError, true)
    strictEqual(error.message, 'Client is closed.')
  }
})

test('findGroupCoordinator should handle errors from Connection.getFirstAvailable', async t => {
  const consumer = createConsumer(t)

  mockConnectionPoolGetFirstAvailable(consumer[kConnections])

  // Attempt to find coordinator with the mocked connection
  try {
    await consumer.findGroupCoordinator()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('findGroupCoordinator should handle errors from the API', async t => {
  const consumer = createConsumer(t)

  mockAPI(consumer[kConnections], findCoordinatorV6.api.key)

  // Attempt to find coordinator with the mocked API
  try {
    await consumer.findGroupCoordinator()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Cannot connect to any broker.')
  }
})

test('joinGroup should join the consumer group and return memberId', async t => {
  const consumer = createConsumer(t)

  // Join the group
  const memberId = await consumer.joinGroup({})

  // Verify memberId is a non-empty string
  strictEqual(typeof memberId, 'string')
  ok(memberId.length > 0, 'Member ID should be a non-empty string')

  // Verify internal state was updated
  strictEqual(consumer.memberId, memberId)
  strictEqual(consumer.generationId > 0, true, 'Generation ID should be greater than 0')
  ok(Array.isArray(consumer.assignments), 'Assignments should be an array')
})

test('joinGroup should setup assignment for a topic', async t => {
  const topic = await createTopic(t, true, 3)
  const consumer = createConsumer(t)

  await consumer.topics.trackAll(topic)

  await consumer.joinGroup({})

  deepStrictEqual(consumer.assignments, [{ topic, partitions: [0, 1, 2] }])
})

test('joinGroup should setup assignment with a round robin policy', async t => {
  const topic = await createTopic(t, true, 3)
  const groupId = createGroupId()

  const consumer1 = createConsumer(t, { groupId })
  const consumer2 = createConsumer(t, { groupId })

  await consumer1.topics.trackAll(topic)
  await consumer2.topics.trackAll(topic)

  await consumer1.joinGroup({ protocols: [{ name: 'roundrobin', version: 1, metadata: '123' }] })
  const rejoinPromise = once(consumer1, 'consumer:group:join')
  await consumer2.joinGroup({ protocols: [{ name: 'roundrobin', version: 1, metadata: Buffer.from('123') }] })
  await rejoinPromise

  if (consumer1.assignments![0].partitions.length === 2) {
    deepStrictEqual(consumer1.assignments, [{ topic, partitions: [0, 2] }])
    deepStrictEqual(consumer2.assignments, [{ topic, partitions: [1] }])
  } else {
    deepStrictEqual(consumer1.assignments, [{ topic, partitions: [1] }])
    deepStrictEqual(consumer2.assignments, [{ topic, partitions: [0, 2] }])
  }
})

test('joinGroup might receive no assignment', async t => {
  const topic = await createTopic(t, true)
  const groupId = createGroupId()

  const consumer1 = createConsumer(t, { groupId })
  const consumer2 = createConsumer(t, { groupId })

  await consumer1.topics.trackAll(topic)
  await consumer2.topics.trackAll(topic)

  await consumer1.joinGroup({})
  const rejoinPromise = once(consumer1, 'consumer:group:join')
  await consumer2.joinGroup({})
  await rejoinPromise

  ok(consumer1.assignments!.length === 0 || consumer2.assignments!.length === 0)
})

test('joinGroup should support both promise and callback API', t => {
  return new Promise<void>((resolve, reject) => {
    const consumer = createConsumer(t)

    // Use callback API
    consumer.joinGroup({}, (err, memberId) => {
      if (err) {
        reject(err)
        return
      }

      // Verify the result is a non-empty string
      strictEqual(typeof memberId, 'string')
      ok(memberId.length > 0, 'Member ID should be a non-empty string')

      // Now try with Promise API to verify both work
      consumer
        .joinGroup({})
        .then(promiseMemberId => {
          // Should be the same member ID
          strictEqual(promiseMemberId, memberId)
          resolve()
        })
        .catch(reject)
    })
  })
})

test('joinGroup should fail when consumer is closed', async t => {
  const consumer = createConsumer(t)

  // Close the consumer first
  await consumer.close()

  // Attempt to join group on a closed consumer
  try {
    await consumer.joinGroup({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof NetworkError, true)
    strictEqual(error.message, 'Client is closed.')
  }
})

test('joinGroup should validate the supplied options', async t => {
  const consumer = createConsumer(t, { strict: true })

  // Test with invalid sessionTimeout
  try {
    await consumer.joinGroup({
      // @ts-expect-error - Intentionally passing invalid option
      sessionTimeout: 'not-a-number'
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message.includes('sessionTimeout'), true)
  }

  // Attempt to join with invalid rebalanceTimeout < sessionTimeout
  try {
    await consumer.joinGroup({
      sessionTimeout: 30000,
      rebalanceTimeout: 20000 // Less than sessionTimeout
    })

    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, '/options/rebalanceTimeout must be greater than or equal to /options/sessionTimeout.')
  }

  // Attempt to join with invalid heartbeatInterval > sessionTimeout
  try {
    await consumer.joinGroup({
      sessionTimeout: 30000,
      rebalanceTimeout: 60000,
      heartbeatInterval: 40000 // Greater than sessionTimeout
    })

    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, '/options/heartbeatInterval must be less than or equal to /options/sessionTimeout.')
  }
})

test('joinGroup should provide default values for missing options', async t => {
  const consumer = createConsumer(t, {
    sessionTimeout: 11000,
    rebalanceTimeout: 22000,
    heartbeatInterval: 3000,
    protocols: [{ name: 'default-protocol', version: 1 }]
  })

  // Define a custom set of group options with some values missing
  const groupOptions = {
    sessionTimeout: 10000
    // other options are intentionally omitted to test defaults
  }

  // Join with partial options
  const memberId = await consumer.joinGroup(groupOptions)

  // Just verify join worked and returned valid memberId
  strictEqual(typeof memberId, 'string')
  ok(memberId.length > 0, 'Member ID should be a non-empty string')
})

test('joinGroup should handle errors from Connection.get', async t => {
  const consumer = createConsumer(t)

  mockConnectionPoolGet(consumer[kConnections], 3)

  // Attempt to join group with the mocked error
  try {
    await consumer.joinGroup({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('joinGroup should handle errors from the API (joinGroup)', async t => {
  const consumer = createConsumer(t)

  mockAPI(consumer[kConnections], joinGroupV9.api.key)

  // Attempt to join group with the mocked error
  try {
    await consumer.joinGroup({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('joinGroup should handle errors from the API (syncGroup)', async t => {
  const consumer = createConsumer(t)

  mockAPI(consumer[kConnections], syncGroupV5.api.key)

  // Attempt to join group with the mocked error
  try {
    await consumer.joinGroup({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('joinGroup should handle errors from the API (findCoordinator)', async t => {
  const consumer = createConsumer(t)

  mockAPI(consumer[kConnections], findCoordinatorV6.api.key)

  // Attempt to join group with the mocked error
  try {
    await consumer.joinGroup({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('joinGroup should handle errors from Base.metadata', async t => {
  const consumer = createConsumer(t)

  mockMetadata(consumer, 1)

  // Attempt to join group with the mocked error
  try {
    await consumer.joinGroup({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('joinGroup should handle errors from Base.metadata during sync', async t => {
  const topic = await createTopic(t, true, 3)
  const groupId = createGroupId()

  const consumer1 = createConsumer(t, { groupId })
  const consumer2 = createConsumer(t, { groupId })

  await consumer1.topics.trackAll(topic)
  await consumer2.topics.trackAll(topic)

  await consumer1.joinGroup({})
  consumer1.on('consumer:group:rebalance', () => {
    mockMetadata(consumer1, 2)
  })

  const errorPromise = once(consumer1, 'error')
  await consumer2.joinGroup({})

  const [error] = await errorPromise
  // Error should contain our mock error message
  strictEqual(error instanceof MultipleErrors, true)
  strictEqual(error.message.includes('Cannot connect to any broker.'), true)
})

test('joinGroup should cancel when membership has been cancelled during join', async t => {
  const consumer = createConsumer(t)

  const promise = consumer.joinGroup({})
  consumer.leaveGroup()
  deepStrictEqual(await promise, undefined)
})

test('joinGroup should cancel when membership has been cancelled during sync', async t => {
  const consumer = createConsumer(t)

  mockAPI(consumer[kConnections], syncGroupV5.api.key, null, null, (original: Function, ...args: any[]) => {
    consumer.leaveGroup()
    original(...args)
  })

  deepStrictEqual(await consumer.joinGroup({}), undefined)
})

test('joinGroup should cancel when membership has been cancelled during metadata-insync', async t => {
  const topic = await createTopic(t, true, 3)
  const groupId = createGroupId()

  const consumer1 = createConsumer(t, { groupId })
  const consumer2 = createConsumer(t, { groupId })

  await consumer1.topics.trackAll(topic)
  await consumer2.topics.trackAll(topic)

  await consumer1.joinGroup({})
  consumer1.on('consumer:group:rebalance', () => {
    mockMetadata(consumer1, 2, null, null, (original, ...args) => {
      consumer1.leaveGroup()
      original(...args)
    })
  })

  await consumer2.joinGroup({})

  deepStrictEqual(consumer1.assignments, null)
  deepStrictEqual(consumer2.assignments, [{ topic, partitions: [0, 1, 2] }])
})

test('joinGroup should cancel when membership has been cancelled during rejoin (rebalance)', async t => {
  const consumer = createConsumer(t)

  mockAPI(
    consumer[kConnections],
    syncGroupV5.api.key,
    new ProtocolError('REBALANCE_IN_PROGRESS', { cancelMembership: true })
  )

  deepStrictEqual(await consumer.joinGroup({}), undefined)
})

test('joinGroup should cancel when membership has been cancelled during rejoin (membership expired)', async t => {
  const consumer = createConsumer(t)

  mockAPI(
    consumer[kConnections],
    syncGroupV5.api.key,
    new ProtocolError('UNKNOWN_MEMBER_ID', { cancelMembership: true })
  )

  deepStrictEqual(await consumer.joinGroup({}), undefined)
})

test('leaveGroup should reset group state and leave the consumer group', async t => {
  const consumer = createConsumer(t)

  await consumer.joinGroup({})

  // Verify we're in a group
  strictEqual(typeof consumer.memberId, 'string')
  ok(consumer.memberId!.length > 0, 'Member ID should be a non-empty string')
  strictEqual(consumer.generationId > 0, true, 'Generation ID should be greater than 0')

  // Leave the group
  await consumer.leaveGroup()

  // Verify group state is reset
  strictEqual(consumer.memberId, null, 'Member ID should be reset to null')
  strictEqual(consumer.generationId, 0, 'Generation ID should be reset to 0')
})

test('leaveGroup should support both promise and callback API', t => {
  return new Promise<void>((resolve, reject) => {
    const consumer = createConsumer(t)

    // First join the group
    consumer.joinGroup({}, joinErr => {
      if (joinErr) {
        reject(joinErr)
        return
      }

      // Now test the callback API for leaving
      consumer.leaveGroup(leaveErr => {
        if (leaveErr) {
          reject(leaveErr)
          return
        }

        // Verify state has been reset
        strictEqual(consumer.memberId, null, 'Member ID should be reset to null')
        strictEqual(consumer.generationId, 0, 'Generation ID should be reset to 0')

        // Now test the promise API
        consumer
          .joinGroup({})
          .then(() => {
            return consumer.leaveGroup()
          })
          .then(() => {
            // Verify state has been reset again
            strictEqual(consumer.memberId, null, 'Member ID should be reset to null')
            strictEqual(consumer.generationId, 0, 'Generation ID should be reset to 0')
            resolve()
          })
          .catch(reject)
      })
    })
  })
})

test('leaveGroup should fail when consumer is closed', async t => {
  const consumer = createConsumer(t)

  // First join the group
  await consumer.joinGroup({})

  // Now close the consumer
  await consumer.close()

  // Attempt to leave group on a closed consumer
  try {
    await consumer.leaveGroup()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof NetworkError, true)
    strictEqual(error.message, 'Client is closed.')
  }
})

test('leaveGroup should silently succeed if not in a group', async t => {
  const consumer = createConsumer(t)

  // We haven't joined a group yet, so memberId should be null
  strictEqual(consumer.memberId, null)

  // Leave group should just succeed without errors
  await consumer.leaveGroup()

  // State should remain unchanged
  strictEqual(consumer.memberId, null)
  strictEqual(consumer.generationId, 0)
})

test('leaveGroup should handle errors from Connection.get', async t => {
  const consumer = createConsumer(t)

  // First join the group
  await consumer.joinGroup({})

  mockConnectionPoolGet(consumer[kConnections], 1)

  // Attempt to leave group with the mocked connection
  try {
    await consumer.leaveGroup()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('leaveGroup should handle errors from the API', async t => {
  const consumer = createConsumer(t)

  // First join the group
  await consumer.joinGroup({})

  mockAPI(consumer[kConnections], leaveGroupV5.api.key)

  // Attempt to leave group with the mocked API
  try {
    await consumer.leaveGroup()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('leaveGroup should handle unknown member errors gracefully', async t => {
  const consumer = createConsumer(t)

  // First join the group
  await consumer.joinGroup({})

  mockAPI(consumer[kConnections], leaveGroupV5.api.key, new ProtocolError('UNKNOWN_MEMBER_ID'))

  // This should succeed despite the error because we handle the unknown member case
  await consumer.leaveGroup()

  // State should be reset
  strictEqual(consumer.memberId, null)
  strictEqual(consumer.generationId, 0)
})

test('leaveGroup should fail when consumer is actively consuming messages', async t => {
  const consumer = createConsumer(t)
  await consumer.consume({ topics: [] })

  // Attempt to leave group without force - should fail
  try {
    await consumer.leaveGroup()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof UserError, true)
    strictEqual(error.message, 'Cannot leave group while consuming messages.')
  }

  // Now try with force=true - should close streams and succeed
  await consumer.leaveGroup(true)

  // State should be reset
  strictEqual(consumer.memberId, null)
  strictEqual(consumer.generationId, 0)
})

test('leaveGroup handle stream closing error', async t => {
  const consumer = createConsumer(t)
  const stream = await consumer.consume({ topics: [] })

  mockMethod(stream, 'close', 1, new UserError('Stream close error'))

  try {
    await consumer.leaveGroup(true)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message, 'Stream close error')
  }
})

test('leaveGroup should ignore closed streams', async t => {
  const consumer = createConsumer(t)
  const stream = await consumer.consume({ topics: [] })

  stream.removeAllListeners('close')

  await stream.close()
  await consumer.leaveGroup()
})

test('#heartbeat should regularly trigger events', async t => {
  const consumer = createConsumer(t)

  await consumer.joinGroup({})

  await once(consumer, 'consumer:heartbeat:start')
  await once(consumer, 'consumer:heartbeat:end')
})

test('#heartbeat should handle errors from the API', async t => {
  const consumer = createConsumer(t)

  mockAPI(consumer[kConnections], heartbeatV4.api.key)

  await consumer.joinGroup({})

  const [{ error }] = await once(consumer, 'consumer:heartbeat:error')

  // Error should contain our mock error message
  strictEqual(error instanceof MultipleErrors, true)
  strictEqual(error.message.includes('Cannot connect to any broker.'), true)
})

test('#heartbeat should emit events when it was cancelled while waiting for API response', async t => {
  const consumer = createConsumer(t)

  consumer.on('consumer:heartbeat:start', () => {
    mockMetadata(consumer, 1, null, null, (original, options, callback) => {
      consumer.leaveGroup()
      original(options, callback)
    })
  })

  await consumer.joinGroup({})
  await once(consumer, 'consumer:heartbeat:start')
  await once(consumer, 'consumer:heartbeat:cancel')
})

test('#heartbeat should emit events when it was cancelled while waiting for Heartbeat API response', async t => {
  const consumer = createConsumer(t)

  mockAPI(consumer[kConnections], heartbeatV4.api.key, null, null, (original: Function, ...args: any[]) => {
    consumer.leaveGroup()
    original(...args)
  })

  await consumer.joinGroup({})
  await once(consumer, 'consumer:heartbeat:start')
  await once(consumer, 'consumer:heartbeat:cancel')
})
