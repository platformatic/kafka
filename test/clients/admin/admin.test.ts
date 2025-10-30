import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { scheduler } from 'node:timers/promises'
import {
  ClientQuotaEntityTypes,
  ClientQuotaKeys,
  ConfigResourceTypes,
  ConfigSources,
  ConfigTypes,
  IncrementalAlterConfigOperationTypes
} from '../../../src/apis/enumerations.ts'
import { kConnections } from '../../../src/clients/base/base.ts'
import {
  Admin,
  adminClientQuotasChannel,
  adminConsumerGroupOffsetsChannel,
  adminConfigsChannel,
  adminGroupsChannel,
  adminLogDirsChannel,
  adminTopicsChannel,
  alterClientQuotasV1,
  type Broker,
  alterConfigsV2,
  type BrokerLogDirDescription,
  type Callback,
  type CallbackWithPromise,
  type ClientDiagnosticEvent,
  ClientQuotaMatchTypes,
  type ClusterPartitionMetadata,
  type Connection,
  type ConfigDescription,
  Consumer,
  type CreatedTopic,
  createPartitionsV3,
  type DescribeClientQuotasOptions,
  describeClientQuotasV0,
  describeConfigsV4,
  describeGroupsV5,
  describeLogDirsV4,
  EMPTY_BUFFER,
  type GroupBase,
  incrementalAlterConfigsV1,
  instancesChannel,
  type ListConsumerGroupOffsetsGroup,
  listGroupsV5,
  MultipleErrors,
  ResponseError,
  type ResponseParser,
  sleep,
  UnsupportedApiError,
  type Writer
} from '../../../src/index.ts'
import {
  createAdmin,
  createConsumer,
  createCreationChannelVerifier,
  createTopic,
  createTracingChannelVerifier,
  kafkaBootstrapServers,
  mockAPI,
  mockConnectionPoolGet,
  mockConnectionPoolGetFirstAvailable,
  mockedErrorMessage,
  mockedOperationId,
  mockMetadata,
  mockUnavailableAPI,
  retry
} from '../../helpers.ts'

test('constructor should initialize properly', t => {
  const created = createCreationChannelVerifier(instancesChannel)
  const admin = createAdmin(t)

  strictEqual(admin instanceof Admin, true)
  strictEqual(admin.closed, false)
  deepStrictEqual(created(), { type: 'admin', instance: admin })
})

test('should support both promise and callback API', (t, done) => {
  const admin = createAdmin(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-${randomUUID()}`

  // Use callback API
  admin.createTopics(
    {
      topics: [topicName],
      partitions: 1,
      replicas: 1
    },
    (err, created) => {
      strictEqual(err, null)
      strictEqual(created?.length, 1)
      strictEqual(created?.[0].name, topicName)

      // Clean up and close
      admin.deleteTopics({ topics: [topicName] }, err => {
        if (err) {
          done(err)
          return
        }

        try {
          strictEqual(err, null)
          done()
        } catch (err) {
          done(err)
        }
      })
    }
  )
})

test('all operations should fail when admin is closed', async t => {
  const admin = createAdmin(t)

  // Close the admin first
  await admin.close()

  // Attempt to call listTopics on closed admin
  try {
    await admin.listTopics()
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should be about admin being closed
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call createTopics on closed admin
  try {
    await admin.createTopics({
      topics: ['test-topic'],
      partitions: 1,
      replicas: 1
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should be about admin being closed
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call deleteTopics on closed admin
  try {
    await admin.deleteTopics({
      topics: ['test-topic']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call listGroups on closed admin
  try {
    await admin.listGroups()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call describeGroups on closed admin
  try {
    await admin.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call deleteGroups on closed admin
  try {
    await admin.deleteGroups({
      groups: ['test-group']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call describeClientQuotas on closed admin
  try {
    await admin.describeClientQuotas({
      components: [
        { entityType: ClientQuotaEntityTypes.CLIENT_ID, matchType: ClientQuotaMatchTypes.EXACT, match: 'Client1' }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call alterClientQuotas on closed admin
  try {
    await admin.alterClientQuotas({ entries: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call removeMembersFromConsumerGroup on closed admin
  try {
    await admin.removeMembersFromConsumerGroup({
      groupId: 'test-group',
      members: [{ memberId: 'test-member', reason: 'test-reason' }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call listConsumerGroupOffsets on closed admin
  try {
    await admin.listConsumerGroupOffsets({
      groups: ['test-group']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call alterConsumerGroupOffsets on closed admin
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: []
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call deleteConsumerGroupOffsets on closed admin
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: []
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call describeConfigs on closed admin
  try {
    await admin.describeConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic' }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call alterConfigs on closed admin
  try {
    await admin.alterConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic', configs: [] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call incrementalAlterConfigs on closed admin
  try {
    await admin.incrementalAlterConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic', configs: [] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }
})

test('listTopics should list topics and support diagnostic channels', async t => {
  const admin = createAdmin(t)

  const topic1 = await createTopic(t, true)
  const topic2 = await createTopic(t, true)
  const topic3 = await createTopic(t, true)

  // It might take a little while for the metadata to propagate, let's retry a few times
  let topics: string[] = []
  for (let i = 0; i < 5; i++) {
    await sleep(500)
    topics = await admin.listTopics()

    if (topics.includes(topic1) && topics.includes(topic2) && topics.includes(topic3)) {
      break
    }
  }

  ok(topics.length >= 3)
  ok(topics.includes(topic1))
  ok(topics.includes(topic2))
  ok(topics.includes(topic3))
})

test('listTopics should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with wrong type (includeInternals)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.listTopics({ includeInternals: 'WTF' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('includeInternals'), true)
  }
})

test('listTopics should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections])

  try {
    // Attempt to create a topic - should fail with connection error
    await admin.listTopics()
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('listTopics should handle unavailable API errors', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'Metadata')

  try {
    // Attempt to create a topic - should fail with connection error
    await admin.listTopics({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Unsupported API Metadata.'), true)
  }
})

test('createTopics should create a new topic and support diagnostic channels', async t => {
  const admin = createAdmin(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-${randomUUID()}`
  const options = {
    topics: [topicName],
    partitions: 3,
    replicas: 1
  }

  const verifyTracingChannel = createTracingChannelVerifier(
    adminTopicsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, { client: admin, operation: 'createTopics', options, operationId: mockedOperationId })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        deepStrictEqual((context.result as CreatedTopic[])[0].name, topicName)
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'createTopics'
  )

  // Create a new topic
  const created = await admin.createTopics(options)

  // Verify the response contains the created topic
  deepStrictEqual(created, [
    {
      name: topicName,
      partitions: 3,
      replicas: 1,
      id: created[0].id, // Preserve dynamic value
      configuration: created[0].configuration // Preserve dynamic value
    }
  ])

  verifyTracingChannel()

  // Clean up by deleting the topic
  await admin.deleteTopics({ topics: [topicName] })
})

test('createTopics should create a topic with specific partitions', async t => {
  const admin = createAdmin(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-partitions-${randomUUID()}`

  // Create a new topic with a specific number of partitions
  const created = await admin.createTopics({
    topics: [topicName],
    partitions: 3, // Specify 3 partitions
    replicas: 1 // With 1 replica (appropriate for single-broker setup)
  })

  // Verify the response contains the created topic
  strictEqual(created.length, 1)
  strictEqual(created[0].name, topicName)
  strictEqual(created[0].partitions, 3)
  strictEqual(created[0].replicas, 1)

  // Fetch the topic metadata to verify partitions
  const topicMetadata = await admin.metadata({ topics: [topicName] })
  const topic = topicMetadata.topics.get(topicName)

  // Verify the topic has exactly 3 partitions
  strictEqual(topic?.partitionsCount, 3)

  // Verify each partition has a valid leader
  for (let i = 0; i < 3; i++) {
    const partition: ClusterPartitionMetadata = topic?.partitions[i]
    strictEqual(typeof partition?.leader, 'number')
    strictEqual(partition?.leader! >= 0, true)
  }

  // Clean up by deleting the topic
  await admin.deleteTopics({ topics: [topicName] })
})

test('createTopics should create multiple topics', async t => {
  const admin = createAdmin(t)

  // Generate unique topic names for testing
  const topicName1 = `test-topic-${randomUUID()}`
  const topicName2 = `test-topic-${randomUUID()}`

  // Create multiple topics in a single request
  const created = await admin.createTopics({
    topics: [topicName1, topicName2]
  })

  // Verify the response contains both created topics
  strictEqual(created.length, 2)

  // Sort the created topics to make the assertion order predictable
  const sortedTopics = [...created].sort((a, b) => a.name.localeCompare(b.name))

  // Check first topic
  const firstTopicIndex = topicName1 < topicName2 ? 0 : 1
  strictEqual(sortedTopics[firstTopicIndex].name, topicName1)
  strictEqual(sortedTopics[firstTopicIndex].partitions, 1)
  strictEqual(sortedTopics[firstTopicIndex].replicas, 1)

  // Check second topic
  const secondTopicIndex = topicName1 < topicName2 ? 1 : 0
  strictEqual(sortedTopics[secondTopicIndex].name, topicName2)
  strictEqual(sortedTopics[secondTopicIndex].partitions, 1)
  strictEqual(sortedTopics[secondTopicIndex].replicas, 1)

  // Fetch metadata to verify both topics exist
  const topicMetadata = await admin.metadata({ topics: [topicName1, topicName2] })
  strictEqual(topicMetadata.topics.has(topicName1), true)
  strictEqual(topicMetadata.topics.has(topicName2), true)

  // Clean up by deleting the topics
  await admin.deleteTopics({ topics: [topicName1, topicName2] })
})

test('createTopics with multiple partitions', async t => {
  const admin = createAdmin(t)

  // Generate a unique topic name
  const topicName = `test-topic-partitions-${randomUUID()}`

  // Create topic with multiple partitions
  const created = await admin.createTopics({
    topics: [topicName],
    partitions: 3,
    replicas: 1
  })

  // Verify the created topic
  deepStrictEqual(created, [
    {
      id: created[0].id, // Preserve dynamic value
      name: topicName,
      partitions: 3,
      replicas: 1,
      configuration: created[0].configuration // Preserve dynamic value
    }
  ])

  // Get metadata to verify partitions
  const topicMetadata = await admin.metadata({ topics: [topicName] })
  const topic = topicMetadata.topics.get(topicName)

  // Should have exactly 3 partitions
  strictEqual(topic?.partitionsCount, 3)

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('createTopics with custom configuration', async t => {
  const admin = createAdmin(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-${randomUUID()}`

  // Create a topic with custom configuration
  const created = await admin.createTopics({
    topics: [topicName],
    configs: [
      {
        name: 'cleanup.policy',
        value: 'compact'
      }
    ]
  })

  strictEqual(created[0].configuration['cleanup.policy'], 'compact')

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('createTopics using assignments', async t => {
  const admin = createAdmin(t)

  // First, get cluster metadata to find the broker
  const metadata = await admin.metadata({ topics: [] })
  const brokerIds = Array.from(metadata.brokers.keys())
  const topicName = `test-topic-leader-${randomUUID()}`

  // Create a topic with a single partition - leader will be automatically assigned
  const created = await admin.createTopics({
    topics: [topicName],
    assignments: brokerIds.map((brokerId, i) => ({ partition: i, brokers: [brokerId] }))
  })

  deepStrictEqual(created, [
    {
      id: created[0].id, // Preserve dynamic value
      name: topicName,
      partitions: brokerIds.length,
      replicas: 1,
      configuration: created[0].configuration // Preserve dynamic value
    }
  ])

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('createTopics should retarget controller when needed', async t => {
  const admin = createAdmin(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-partitions-${randomUUID()}`
  let correctControllerId: number | null = null

  const pool = admin[kConnections]
  const originalGet = pool.get.bind(pool)
  // @ts-ignore
  pool.get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    originalGet(broker, (error: Error | null, connection: Connection) => {
      // Define the next broker in sequence as the correct controller
      if (correctControllerId === null) {
        correctControllerId = (connection.port! - 9010 + 1) % 3
        mockMetadata(admin, 1, null, {
          brokers: new Map([
            [1, { host: 'localhost', port: 9011 }],
            [2, { host: 'localhost', port: 9012 }],
            [3, { host: 'localhost', port: 9013 }]
          ]),
          controllerId: correctControllerId
        })
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
        if (apiKey === 19) {
          if (connection.port! - 9010 !== correctControllerId) {
            callback(new ResponseError(19, 7, { '/': [41, 'Kaboom!'] }, {}), undefined as unknown as ReturnType)
            return
          }
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
      }

      callback(error, connection)
    })
  }

  await admin.createTopics({ topics: [topicName] })

  // Clean up by deleting the topic
  await admin.deleteTopics({ topics: [topicName] })
})

test('createTopics should not deduplicate creation of different topics', async t => {
  const admin = createAdmin(t)

  const topicNames = [`test-topic-${randomUUID()}`, `test-topic-${randomUUID()}`]

  await Promise.all(
    topicNames.map(topicName =>
      admin.createTopics({
        topics: [topicName]
      }))
  )

  const topicMetadata = await admin.metadata({ topics: topicNames })
  strictEqual(topicMetadata.topics.has(topicNames[0]), true)
  strictEqual(topicMetadata.topics.has(topicNames[1]), true)

  await admin.deleteTopics({ topics: topicNames })
})

test('createTopics should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (topics)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.createTopics({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid topics type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.createTopics({ topics: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with non-string topic in topics array
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.createTopics({ topics: [123] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid partitions type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.createTopics({ topics: ['test-topic'], partitions: 'not-a-number' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitions'), true)
  }

  // Test with invalid replicas type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.createTopics({ topics: ['test-topic'], replicas: 'not-a-number' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('replicas'), true)
  }

  // Test with invalid assignments type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.createTopics({ topics: ['test-topic'], assignments: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('assignments'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.createTopics({ topics: ['test-topic'], invalidProperty: true })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('createTopics should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections])

  try {
    // Attempt to create a topic - should fail with connection error
    await admin.createTopics({
      topics: ['test-topic'],
      partitions: 1,
      replicas: 1
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('createTopics should handle unavailable API errors', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'CreateTopics')

  try {
    // Attempt to create a topic - should fail with connection error
    await admin.createTopics({
      topics: ['test-topic'],
      partitions: 1,
      replicas: 1
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Unsupported API CreateTopics.'), true)
  }
})

test('deleteTopics should delete a topic and support diagnostic channels', async t => {
  const admin = createAdmin(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-${randomUUID()}`

  const verifyTracingChannel = createTracingChannelVerifier(
    adminTopicsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'deleteTopics',
          options: { topics: [topicName] },
          operationId: mockedOperationId
        })
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'deleteTopics'
  )

  // Create a new topic
  await admin.createTopics({
    topics: [topicName],
    partitions: 1,
    replicas: 1
  })

  // Delete the topic
  await admin.deleteTopics({ topics: [topicName] })

  // Verify the topic was deleted by trying to recreate it
  // (If it still existed, this would fail with a topic already exists error)
  const created = await admin.createTopics({
    topics: [topicName],
    partitions: 1,
    replicas: 1
  })

  strictEqual(created.length, 1)
  strictEqual(created[0].name, topicName)

  verifyTracingChannel()

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('deleteTopics should retarget controller when needed', async t => {
  const admin = createAdmin(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-partitions-${randomUUID()}`
  await admin.createTopics({ topics: [topicName] })

  let correctControllerId: number | null = null

  const pool = admin[kConnections]
  const originalGet = pool.get.bind(pool)
  // @ts-ignore
  pool.get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    originalGet(broker, (error: Error | null, connection: Connection) => {
      // Define the next broker in sequence as the correct controller
      if (correctControllerId === null) {
        correctControllerId = (connection.port! - 9010 + 1) % 3
        mockMetadata(admin, 1, null, {
          brokers: new Map([
            [1, { host: 'localhost', port: 9011 }],
            [2, { host: 'localhost', port: 9012 }],
            [3, { host: 'localhost', port: 9013 }]
          ]),
          controllerId: correctControllerId
        })
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
        if (apiKey === 20) {
          if (connection.port! - 9010 !== correctControllerId) {
            callback(new ResponseError(20, 6, { '/': [41, 'Kaboom!'] }, {}), undefined as unknown as ReturnType)
            return
          }
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
      }

      callback(error, connection)
    })
  }

  // Clean up by deleting the topic
  await admin.deleteTopics({ topics: [topicName] })
})

test('deleteTopics should not deduplicate deletion of different topics', async t => {
  const admin = createAdmin(t)

  const topicNames = [`test-topic-${randomUUID()}`, `test-topic-${randomUUID()}`]

  admin.createTopics({ topics: topicNames })

  const topicMetadata = await admin.metadata({ topics: topicNames })
  strictEqual(topicMetadata.topics.has(topicNames[0]), true)
  strictEqual(topicMetadata.topics.has(topicNames[1]), true)

  await Promise.all(
    topicNames.map(topicName =>
      admin.deleteTopics({
        topics: [topicName]
      }))
  )

  // Deletion needs some time to propagate, retry a few times
  await retry(15, 500, async () => {
    try {
      await admin.metadata({ topics: [topicNames[0]] })
      throw Error('Topic still exists: ' + topicNames[0])
    } catch (error) {
      // ApiCode 3 = UnknownTopicOrPartition
      ok(error.findBy?.('apiCode', 3))
    }
    try {
      await admin.metadata({ topics: [topicNames[1]] })
      throw Error('Topic still exists: ' + topicNames[1])
    } catch (error) {
      // ApiCode 3 = UnknownTopicOrPartition
      ok(error.findBy?.('apiCode', 3))
    }
  })
})

test('deleteTopics should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (topics)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteTopics({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid type for topics
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteTopics({ topics: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with non-string topic in topics array
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteTopics({ topics: [123] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteTopics({ topics: ['test-topic'], invalidProperty: true })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('deleteTopics should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections])

  try {
    // Attempt to delete a topic - should fail with connection error
    await admin.deleteTopics({
      topics: ['test-topic']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('deleteTopics should handle unavailable API errors', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'DeleteTopics')

  try {
    // Attempt to delete a topic - should fail with connection error
    await admin.deleteTopics({
      topics: ['test-topic']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Unsupported API DeleteTopics.'), true)
  }
})

test('createPartitions should create additional partitions with manual assignments', async t => {
  const admin = createAdmin(t)
  const topicName = `test-topic-${randomUUID()}`
  await admin.createTopics({
    topics: [topicName],
    partitions: 1,
    replicas: 1
  })

  await scheduler.wait(1000)

  const options = {
    topics: [
      {
        name: topicName,
        count: 2,
        assignments: [
          {
            brokerIds: [1]
          }
        ]
      }
    ],
    validateOnly: false
  }

  await admin.createPartitions(options)

  await scheduler.wait(1000)

  const metadata = await admin.metadata({ topics: [topicName] })

  strictEqual(metadata.topics.get(topicName)?.partitionsCount, 2)

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('createPartitions should create additional partitions with automatic assignments', async t => {
  const admin = createAdmin(t)
  const topicName = `test-topic-${randomUUID()}`
  await admin.createTopics({
    topics: [topicName],
    partitions: 1,
    replicas: 1
  })

  await scheduler.wait(1000)

  const options = {
    topics: [
      {
        name: topicName,
        count: 2,
        assignments: null
      }
    ],
    validateOnly: false
  }

  await admin.createPartitions(options)

  await scheduler.wait(1000)

  const metadata = await admin.metadata({ topics: [topicName] })

  strictEqual(metadata.topics.get(topicName)?.partitionsCount, 2)

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('createPartitions should support validateOnly option', async t => {
  const admin = createAdmin(t)
  const topicName = `test-topic-${randomUUID()}`
  await admin.createTopics({
    topics: [topicName],
    partitions: 1,
    replicas: 1
  })

  await scheduler.wait(1000)

  const options = {
    topics: [
      {
        name: topicName,
        count: 2,
        assignments: null
      }
    ],
    validateOnly: true
  }

  await admin.createPartitions(options)

  const metadata = await admin.metadata({ topics: [topicName] })

  await scheduler.wait(1000)

  strictEqual(metadata.topics.get(topicName)?.partitionsCount, 1)

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('createPartitions support diagnostic channels', async t => {
  const admin = createAdmin(t)
  const topicName = `test-topic-${randomUUID()}`
  await admin.createTopics({
    topics: [topicName],
    partitions: 1,
    replicas: 1
  })

  const options = {
    topics: [
      {
        name: topicName,
        count: 2,
        assignments: [
          {
            brokerIds: [1]
          }
        ]
      }
    ],
    validateOnly: false
  }

  const verifyTracingChannel = createTracingChannelVerifier(
    adminTopicsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'createPartitions',
          options,
          operationId: mockedOperationId
        })
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'createPartitions'
  )

  await admin.createPartitions(options)

  verifyTracingChannel()

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('createPartitions should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (topics)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.createPartitions({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid type for topics
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.createPartitions({ topics: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with empty topics array
  try {
    await admin.createPartitions({ topics: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid topic object (missing name)
  try {
    await admin.createPartitions({
      topics: [{ count: 3 }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (missing count)
  try {
    await admin.createPartitions({
      topics: [{ name: 'test-topic' }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('count'), true)
  }

  // Test with invalid validateOnly type
  try {
    await admin.createPartitions({
      topics: [{ name: 'test-topic', count: 3 }],
      validateOnly: 'not-a-boolean'
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('validateOnly'), true)
  }

  // Test with invalid additional property
  try {
    await admin.createPartitions({
      topics: [{ name: 'test-topic', count: 3 }],
      invalidProperty: true
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('createPartitions should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 3)

  try {
    // Attempt to list groups - should fail with connection error
    await admin.createPartitions({
      topics: [
        { name: 'test-topic', count: 3, assignments: [{ brokerIds: [1] }, { brokerIds: [2] }, { brokerIds: [3] }] }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should be about connection failure
    strictEqual(error instanceof Error, true)
    strictEqual(error.message, 'Creating partitions failed.')
  }
})

test('createPartitions should handle errors from the API', async t => {
  const admin = createAdmin(t)

  mockAPI(admin[kConnections], createPartitionsV3.api.key)

  try {
    // Attempt to list groups - should fail with connection error
    await admin.createPartitions({
      topics: [
        { name: 'test-topic', count: 3, assignments: [{ brokerIds: [1] }, { brokerIds: [2] }, { brokerIds: [3] }] }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should be about connection failure
    strictEqual(error instanceof Error, true)
    strictEqual(error.message, 'Creating partitions failed.')
  }
})

test('listGroups should handle unavailable API errors', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'CreatePartitions')

  try {
    // Attempt to create partitions - should fail with connection error
    await admin.createPartitions({
      topics: [
        { name: 'test-topic', count: 3, assignments: [{ brokerIds: [1] }, { brokerIds: [2] }, { brokerIds: [3] }] }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should be about connection failure
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API CreatePartitions.'), true)
  }
})

test('listGroups should return consumer groups and support diagnostic channels', async t => {
  const groupId = `test-group-${randomUUID()}`
  // Create a consumer that joins a group
  const consumer = new Consumer({
    clientId: `test-admin-admin-${randomUUID()}`,
    groupId,
    bootstrapBrokers: kafkaBootstrapServers
  })
  t.after(() => consumer.close())

  const admin = createAdmin(t)

  // Get the API version for listGroups to see if we await the group type
  const apiVersion = await admin.listApis()
  const listGroupsApi = apiVersion.find(a => a.apiKey === listGroupsV5.api.key)!
  const groupType = listGroupsApi.maxVersion === 5 ? 'classic' : undefined

  await consumer.joinGroup({})

  const verifyTracingChannel = createTracingChannelVerifier(
    adminGroupsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'listGroups',
          options: { types: ['classic'] },
          operationId: mockedOperationId
        })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        ok((context.result as Map<string, GroupBase>).get(groupId))
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'listGroups'
  )

  // List all groups
  const groups = await admin.listGroups()

  // Basic validation of the groups map structure
  strictEqual(groups instanceof Map, true)

  deepStrictEqual(groups.get(consumer.groupId), {
    id: consumer.groupId,
    protocolType: 'consumer',
    state: 'STABLE',
    groupType
  })

  verifyTracingChannel()
})

test('listGroups should support filtering by types and states', async t => {
  // Create a consumer that joins a group
  const consumer = new Consumer({
    clientId: `test-admin-admin-${randomUUID()}`,
    groupId: `test-group-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers
  })
  t.after(() => consumer.close())

  const admin = createAdmin(t)

  await consumer.joinGroup({})

  // Test with explicit types parameter - include consumer groups
  const groups1 = await admin.listGroups({
    types: ['classic']
  })

  // Verify we found the consumer group
  strictEqual(groups1.has(consumer.groupId), true, 'The consumer group should be found with type "classic"')

  // Test with empty options
  const groups2 = await admin.listGroups({})
  // Default types should be used
  strictEqual(groups2.has(consumer.groupId), true, 'The consumer group should be found with default types')

  // Test with state filtering - stable should include our group
  const groups3 = await admin.listGroups({
    states: ['STABLE']
  })
  strictEqual(groups3.has(consumer.groupId), true, 'The consumer group should be found with state "STABLE"')

  // Test with a state that shouldn't match our group
  const groups4 = await admin.listGroups({
    states: ['DEAD']
  })
  strictEqual(groups4.has(consumer.groupId), false, 'The consumer group should not be found with state "DEAD"')
})

test('listGroups should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with invalid types field
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.listGroups({ types: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('types'), true)
  }

  // Test with invalid states field
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.listGroups({ states: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('states'), true)
  }

  // Test with invalid state value
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.listGroups({ states: ['INVALID_STATE'] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('states'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.listGroups({ invalidProperty: true })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Empty options should be valid
  const groups = await admin.listGroups({})
  strictEqual(groups instanceof Map, true)
})

test('listGroups should handle errors from Base.metadata', async t => {
  const admin = createAdmin(t)

  // Override the [kMetadata] method to simulate a connection error
  mockMetadata(admin)

  try {
    // Attempt to list groups - should fail with connection error
    await admin.listGroups()
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('listGroups should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 3)

  try {
    // Attempt to list groups - should fail with connection error
    await admin.listGroups()
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should be about connection failure
    strictEqual(error instanceof Error, true)
    strictEqual(error.message, 'Listing groups failed.')
  }
})

test('listGroups should handle errors from the API', async t => {
  const admin = createAdmin(t)

  mockAPI(admin[kConnections], listGroupsV5.api.key)

  try {
    // Attempt to list groups - should fail with connection error
    await admin.listGroups()
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should be about connection failure
    strictEqual(error instanceof Error, true)
    strictEqual(error.message, 'Listing groups failed.')
  }
})

test('listGroups should handle unavailable API errors', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'ListGroups')

  try {
    // Attempt to list groups - should fail with connection error
    await admin.listGroups()
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should be about connection failure
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API ListGroups.'), true)
  }
})

test('describeGroups should describe consumer groups and support diagnostic channels', async t => {
  // Create a consumer that joins a group
  const groupId = `test-group-${randomUUID()}`
  const consumer = new Consumer({
    clientId: `test-admin-admin-${randomUUID()}`,
    groupId,
    bootstrapBrokers: kafkaBootstrapServers
  })
  t.after(() => consumer.close())

  const admin = createAdmin(t)
  const testTopic = `test-topic-${randomUUID()}`

  // Create a unique test topic name
  await admin.createTopics({ topics: [testTopic], partitions: 1, replicas: 1 })

  await consumer.topics.trackAll(testTopic)
  await consumer.joinGroup({})

  const options = {
    groups: [consumer.groupId, 'non-existent-group'],
    includeAuthorizedOperations: true
  }

  const verifyTracingChannel = createTracingChannelVerifier(
    adminGroupsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'describeGroups',
          options,
          operationId: mockedOperationId
        })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        deepStrictEqual((context.result as Map<string, GroupBase>).get(groupId)!.state, 'STABLE')
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'describeGroups'
  )

  // Describe the groups
  const describedGroups = await admin.describeGroups(options)

  const describedGroup = describedGroups.get(consumer.groupId)!
  const { id, clientId, clientHost } = Array.from(describedGroup.members.values()!)[0]
  const authorizedOperations = describedGroup.authorizedOperations
  deepStrictEqual(
    Array.from(describedGroups.values()).sort((a, b) => a.state.localeCompare(b.state)),
    [
      {
        id: 'non-existent-group',
        state: 'DEAD',
        protocolType: '',
        protocol: '',
        members: new Map(),
        authorizedOperations
      },
      {
        id: consumer.groupId,
        protocol: 'roundrobin',
        protocolType: 'consumer',
        state: 'STABLE',
        members: new Map([
          [
            id,
            {
              assignments: new Map([[testTopic, { topic: testTopic, partitions: [0] }]]),
              clientHost,
              clientId,
              groupInstanceId: null,
              id,
              metadata: {
                metadata: EMPTY_BUFFER,
                topics: [testTopic],
                version: 1
              }
            }
          ]
        ]),
        authorizedOperations
      }
    ]
  )

  verifyTracingChannel()
})

test('describeGroups should handle includeAuthorizedOperations option', async t => {
  // Create a consumer that joins a group
  const consumer = new Consumer({
    clientId: `test-admin-admin-${randomUUID()}`,
    groupId: `test-group-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers
  })
  t.after(() => consumer.close())

  const admin = createAdmin(t)

  await consumer.joinGroup({})

  // Test with includeAuthorizedOperations: true
  const groupsWithAuth = await admin.describeGroups({
    groups: [consumer.groupId],
    includeAuthorizedOperations: true
  })

  // Verify the authorizedOperations field is as expected
  const group1 = groupsWithAuth.get(consumer.groupId)!
  strictEqual(typeof group1.authorizedOperations, 'number')

  // Test with includeAuthorizedOperations: false (default)
  const groupsWithoutAuth = await admin.describeGroups({
    groups: [consumer.groupId]
  })

  // Verify the authorizedOperations field is 0 (default value when not included)
  const group2 = groupsWithoutAuth.get(consumer.groupId)!
  // Depending on broker configuration, this might still be populated
  // Just ensure the field exists without asserting a specific value
  strictEqual(typeof group2.authorizedOperations, 'number')
})

test('describeGroups should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (groups)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.describeGroups({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with invalid type for groups
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.describeGroups({ groups: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with non-string group in groups array
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.describeGroups({ groups: [123] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with empty groups array
  try {
    await admin.describeGroups({ groups: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with invalid includeAuthorizedOperations type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.describeGroups({ groups: ['test-group'], includeAuthorizedOperations: 'not-a-boolean' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('includeAuthorizedOperations'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.describeGroups({ groups: ['test-group'], invalidProperty: true })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('describeGroups should handle errors from Base.metadata', async t => {
  const admin = createAdmin(t)

  mockMetadata(admin)

  try {
    // Attempt to describe groups - should fail with connection error
    await admin.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('describeGroups should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections], 3)

  try {
    // Attempt to describe groups - should fail with connection error
    await admin.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('describeGroups should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 4)

  try {
    // Attempt to describe groups - should fail with connection error
    await admin.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing groups failed.'), true)
  }
})

test('describeGroups should handle errors from the API', async t => {
  const admin = createAdmin(t)

  mockAPI(admin[kConnections], describeGroupsV5.api.key)

  try {
    // Attempt to describe groups - should fail with connection error
    await admin.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing groups failed.'), true)
  }
})

test('describeGroups should handle unavailable API errors (FindCoordinator)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'FindCoordinator')

  try {
    // Attempt to describe groups - should fail with connection error
    await admin.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Unsupported API FindCoordinator.'), true)
  }
})

test('describeGroups should handle unavailable API errors (DescribeGroups)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'DescribeGroups')

  try {
    // Attempt to describe groups - should fail with connection error
    await admin.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API DescribeGroups.'), true)
  }
})

test('deleteGroups should delete groups and support diagnostic channels', async t => {
  const admin = createAdmin(t)

  // Create a consumer that joins a group and then immediately leaves
  const groupId = `test-group-${randomUUID()}`
  const consumer = new Consumer({
    clientId: `test-admin-admin-${randomUUID()}`,
    groupId,
    bootstrapBrokers: kafkaBootstrapServers
  })
  await consumer.joinGroup({})
  await consumer.leaveGroup()
  await consumer.close()

  const verifyTracingChannel = createTracingChannelVerifier(
    adminGroupsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'deleteGroups',
          options: { groups: [groupId] },
          operationId: mockedOperationId
        })
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'deleteGroups'
  )

  await admin.deleteGroups({ groups: [groupId] })
  verifyTracingChannel()
})

test('deleteGroups should handle non-existent groups properly', async t => {
  const admin = createAdmin(t)

  // Generate a unique group ID for testing
  const groupId = `test-group-${randomUUID()}`

  // Trying to delete a non-existent group will likely result in an error
  try {
    await admin.deleteGroups({ groups: [groupId] })
    // If no error, that's also fine
  } catch (error) {
    // This is expected - a non-existent group will cause an error
    strictEqual(error instanceof Error, true)
    // The error should contain specific error information
    strictEqual(Array.isArray(error.errors), true)
  }
})

test('deleteGroups should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (groups)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteGroups({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with invalid type for groups
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteGroups({ groups: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with non-string group in groups array
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteGroups({ groups: [123] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with empty groups array
  try {
    await admin.deleteGroups({ groups: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteGroups({ groups: ['test-group'], invalidProperty: true })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('deleteGroups should handle errors from Base.metadata', async t => {
  const admin = createAdmin(t)

  mockMetadata(admin)

  try {
    // Attempt to delete groups - should fail with connection error
    await admin.deleteGroups({ groups: ['non-existent'] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('deleteGroups should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections], 2)

  try {
    // Attempt to delete groups - should fail with connection error
    await admin.deleteGroups({
      groups: ['test-group']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('deleteGroups should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 4)

  try {
    // Attempt to delete groups - should fail with connection error
    await admin.deleteGroups({ groups: ['non-existent'] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Deleting groups failed.'), true)
  }
})

test('deleteGroups should handle unavailable API errors (DeleteGroups)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'DeleteGroups')

  try {
    // Attempt to delete groups - should fail with connection error
    await admin.deleteGroups({ groups: ['non-existent'] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API DeleteGroups.'), true)
  }
})

test('deleteGroups should handle unavailable API errors (FindCoordinator)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'FindCoordinator')

  try {
    // Attempt to delete groups - should fail with connection error
    await admin.deleteGroups({ groups: ['non-existent'] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Unsupported API FindCoordinator.'), true)
  }
})

test('removeMembersFromConsumerGroup should remove specific members', async t => {
  const admin = createAdmin(t)
  const groupId = `test-group-${randomUUID()}`
  const clientId = `test-client-${randomUUID()}`
  const consumer = createConsumer(t, { clientId, groupId })
  await consumer.joinGroup({})

  ok(consumer.memberId)

  const groups1 = await admin.describeGroups({ groups: [groupId] })
  strictEqual(groups1.get(groupId)!.members.has(consumer.memberId), true)

  await admin.removeMembersFromConsumerGroup({ groupId, members: [{ memberId: consumer.memberId }] })
  const groups2 = await admin.describeGroups({ groups: [groupId] })
  strictEqual(groups2.get(groupId)!.members.has(consumer.memberId), false)
})

test('removeMembersFromConsumerGroup should remove specific members with custom reason', async t => {
  const admin = createAdmin(t)
  const groupId = `test-group-${randomUUID()}`
  const clientId = `test-client-${randomUUID()}`
  const consumer = createConsumer(t, { clientId, groupId })
  await consumer.joinGroup({})

  ok(consumer.memberId)

  const groups1 = await admin.describeGroups({ groups: [groupId] })
  strictEqual(groups1.get(groupId)!.members.has(consumer.memberId), true)

  await admin.removeMembersFromConsumerGroup({
    groupId,
    members: [{ memberId: consumer.memberId, reason: 'Custom reason' }]
  })

  const groups2 = await admin.describeGroups({ groups: [groupId] })
  strictEqual(groups2.get(groupId)!.members.has(consumer.memberId), false)
})

test('removeMembersFromConsumerGroup should remove all members', async t => {
  const admin = createAdmin(t)
  const groupId = `test-group-${randomUUID()}`
  const clientId = `test-client-${randomUUID()}`
  const consumer = createConsumer(t, { clientId, groupId })
  await consumer.joinGroup({})

  ok(consumer.memberId)

  const groups1 = await admin.describeGroups({ groups: [groupId] })
  strictEqual(groups1.get(groupId)!.members.has(consumer.memberId), true)

  await admin.removeMembersFromConsumerGroup({ groupId })

  const groups2 = await admin.describeGroups({ groups: [groupId] })
  strictEqual(groups2.get(groupId)!.members.size, 0)
})

test('removeMembersFromConsumerGroup should support diagnostic channels', async t => {
  const admin = createAdmin(t)
  const groupId = `test-group-${randomUUID()}`
  const clientId = `test-client-${randomUUID()}`
  const consumer = createConsumer(t, { clientId, groupId })
  await consumer.joinGroup({})

  ok(consumer.memberId)

  const verifyTracingChannel = createTracingChannelVerifier(
    adminGroupsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'removeMembersFromConsumerGroup',
          options: { groupId, members: [{ memberId: consumer.memberId }] },
          operationId: mockedOperationId
        })
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'removeMembersFromConsumerGroup'
  )

  // Remove one specific member
  await admin.removeMembersFromConsumerGroup({ groupId, members: [{ memberId: consumer.memberId }] })
  verifyTracingChannel()
})

test('removeMembersFromConsumerGroup should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (groupId)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.removeMembersFromConsumerGroup({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.removeMembersFromConsumerGroup({ groupId: 'test-group', members: null, invalidProperty: true })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid type for groupId
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.removeMembersFromConsumerGroup({ groupId: 0 })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with invalid empty groupId
  try {
    await admin.removeMembersFromConsumerGroup({ groupId: '' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with invalid type for members
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.removeMembersFromConsumerGroup({ groupId: 'test-group', members: 123 })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('members'), true)
  }

  // Test with invalid member type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.removeMembersFromConsumerGroup({ groupId: 'test-group', members: [true] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('members'), true)
  }

  // Test with invalid object member missing memberId
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.removeMembersFromConsumerGroup({ groupId: 'test-group', members: [{ reason: 'No memberId' }] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('members'), true)
  }

  // Test with invalid object member with non-string memberId
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.removeMembersFromConsumerGroup({ groupId: 'test-group', members: [{ memberId: 123 }] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('members'), true)
  }

  // Test with invalid object member with empty string memberId
  try {
    await admin.removeMembersFromConsumerGroup({ groupId: 'test-group', members: [{ memberId: '' }] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('members'), true)
  }

  // Test with invalid object member with non-string reason
  try {
    await admin.removeMembersFromConsumerGroup({
      groupId: 'test-group',
      // @ts-expect-error - Intentionally passing invalid options
      members: [{ memberId: 'valid-id', reason: 123 }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('members'), true)
  }

  // Test with invalid object member with empty string reason
  try {
    await admin.removeMembersFromConsumerGroup({
      groupId: 'test-group',
      members: [{ memberId: 'valid-id', reason: '' }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('members'), true)
  }

  // Test with invalid object member with additional property
  try {
    await admin.removeMembersFromConsumerGroup({
      groupId: 'test-group',
      // @ts-expect-error - Intentionally passing invalid options
      members: [{ memberId: 'valid-id', reason: 'Valid reason', extra: true }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('members'), true)
  }
})

test('removeMembersFromConsumerGroup should handle errors from Base.metadata', async t => {
  const admin = createAdmin(t)

  mockMetadata(admin)

  try {
    await admin.removeMembersFromConsumerGroup({ groupId: 'non-existent', members: [{ memberId: 'fake-member' }] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('removeMembersFromConsumerGroup should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections], 2)

  try {
    await admin.removeMembersFromConsumerGroup({
      groupId: 'test-group',
      members: [{ memberId: 'fake-member' }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('removeMembersFromConsumerGroup should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 4)

  try {
    await admin.removeMembersFromConsumerGroup({ groupId: 'non-existent', members: [{ memberId: 'fake-member' }] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Removing members from consumer group failed.'), true)
  }
})

test('removeMembersFromConsumerGroup should handle unavailable API errors (LeaveGroup)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'LeaveGroup')

  try {
    await admin.removeMembersFromConsumerGroup({ groupId: 'non-existent', members: [{ memberId: 'fake-member' }] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API LeaveGroup.'), true)
  }
})

test('removeMembersFromConsumerGroup should handle unavailable API errors (FindCoordinator)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'FindCoordinator')

  try {
    await admin.removeMembersFromConsumerGroup({ groupId: 'non-existent', members: [{ memberId: 'fake-member' }] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message, 'Removing members from consumer group failed.')
    const subError = error.errors[0]
    ok(subError)
    strictEqual(subError instanceof UnsupportedApiError, true)
    strictEqual(subError.message.includes('Unsupported API FindCoordinator.'), true)
  }
})

test('removeMembersFromConsumerGroup should handle unavailable API errors (DescribeGroups)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'DescribeGroups')

  try {
    await admin.removeMembersFromConsumerGroup({ groupId: 'non-existent', members: null })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message, 'Removing members from consumer group failed.')
    const subError = error.errors[0]
    ok(subError)
    strictEqual(subError.message, 'Describing groups failed.')
    const subSubError = subError.errors[0]
    ok(subSubError)
    strictEqual(subSubError instanceof UnsupportedApiError, true)
    strictEqual(subSubError.message.includes('Unsupported API DescribeGroups.'), true)
  }
})

const clientQuotasTestCases = [
  { clientId: 'Client1', user: 'User1' },
  { clientId: 'Client1', user: null },
  { clientId: null, user: 'User1' },
  { clientId: null, user: null }
]

for (const testCase of clientQuotasTestCases) {
  test(`alterClientQuotas should quotas should set and remove quotas on ${testCase.user ? 'specific' : 'default'} user and ${testCase.clientId ? 'specific' : 'default'} client-id`, async t => {
    const admin = createAdmin(t)

    const clientQuotaEntries: alterClientQuotasV1.AlterClientQuotasRequestEntry[] = [
      {
        entities: [
          { entityType: ClientQuotaEntityTypes.USER, entityName: testCase.user },
          { entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: testCase.clientId }
        ],
        ops: [
          { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000, remove: false },
          { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, value: 1000, remove: false },
          { key: ClientQuotaKeys.REQUEST_PERCENTAGE, value: 0.5, remove: false }
        ]
      }
    ]

    const alterOptions = { entries: clientQuotaEntries, validateOnly: false }

    const alterResult = await admin.alterClientQuotas(alterOptions)

    deepStrictEqual(alterResult, [
      {
        errorCode: 0,
        errorMessage: null,
        entity: [
          { entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: testCase.clientId },
          { entityType: ClientQuotaEntityTypes.USER, entityName: testCase.user }
        ]
      }
    ])

    const describeComponents = []

    if (testCase.clientId) {
      describeComponents.push({
        entityType: ClientQuotaEntityTypes.CLIENT_ID,
        matchType: ClientQuotaMatchTypes.EXACT,
        match: testCase.clientId
      })
    } else {
      describeComponents.push({
        entityType: ClientQuotaEntityTypes.CLIENT_ID,
        matchType: ClientQuotaMatchTypes.DEFAULT
      })
    }
    if (testCase.user) {
      describeComponents.push({
        entityType: ClientQuotaEntityTypes.USER,
        matchType: ClientQuotaMatchTypes.EXACT,
        match: testCase.user
      })
    } else {
      describeComponents.push({
        entityType: ClientQuotaEntityTypes.USER,
        matchType: ClientQuotaMatchTypes.DEFAULT
      })
    }

    const options: DescribeClientQuotasOptions = { components: describeComponents, strict: false }

    await retry(10, 100, async () => {
      const result = await admin.describeClientQuotas(options)
      deepStrictEqual(result, [
        {
          entity: [
            {
              entityType: ClientQuotaEntityTypes.CLIENT_ID,
              entityName: testCase.clientId
            },
            {
              entityType: ClientQuotaEntityTypes.USER,
              entityName: testCase.user
            }
          ],
          values: [
            { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, value: 1000 },
            { key: ClientQuotaKeys.REQUEST_PERCENTAGE, value: 0.5 },
            { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000 }
          ]
        }
      ])
    })

    const removeQuotaEntries: alterClientQuotasV1.AlterClientQuotasRequestEntry[] = [
      {
        entities: [
          { entityType: ClientQuotaEntityTypes.USER, entityName: testCase.user },
          { entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: testCase.clientId }
        ],
        ops: [
          { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, remove: true },
          { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, remove: true },
          { key: ClientQuotaKeys.REQUEST_PERCENTAGE, remove: true }
        ]
      }
    ]

    const removeOptions = { entries: removeQuotaEntries, validateOnly: false }
    const removeResult = await admin.alterClientQuotas(removeOptions)

    deepStrictEqual(removeResult, [
      {
        errorCode: 0,
        errorMessage: null,
        entity: [
          { entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: testCase.clientId },
          { entityType: ClientQuotaEntityTypes.USER, entityName: testCase.user }
        ]
      }
    ])

    await retry(10, 100, async () => {
      const quotasAfterRemoval = await admin.describeClientQuotas(options)
      deepStrictEqual(quotasAfterRemoval, [])
    })
  })
}

test('alterClientQuotas should support validateOnly option', async t => {
  const admin = createAdmin(t)

  const clientQuotaEntries: alterClientQuotasV1.AlterClientQuotasRequestEntry[] = [
    {
      entities: [
        { entityType: ClientQuotaEntityTypes.USER, entityName: 'User1' },
        { entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: 'Client1' }
      ],
      ops: [
        { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 2000, remove: false },
        { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, value: 2000, remove: false },
        { key: ClientQuotaKeys.REQUEST_PERCENTAGE, value: 0.7, remove: false }
      ]
    }
  ]

  const validateOnlyOptions = { entries: clientQuotaEntries, validateOnly: true }
  const validateResult = await admin.alterClientQuotas(validateOnlyOptions)

  deepStrictEqual(validateResult, [
    {
      errorCode: 0,
      errorMessage: null,
      entity: [
        { entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: 'Client1' },
        { entityType: ClientQuotaEntityTypes.USER, entityName: 'User1' }
      ]
    }
  ])

  const options: DescribeClientQuotasOptions = {
    components: [
      {
        entityType: ClientQuotaEntityTypes.USER,
        matchType: ClientQuotaMatchTypes.EXACT,
        match: 'User1'
      },
      {
        entityType: ClientQuotaEntityTypes.CLIENT_ID,
        matchType: ClientQuotaMatchTypes.EXACT,
        match: 'Client1'
      }
    ],
    strict: false
  }

  // We should wait a bit to ensure the changes have been applied
  await scheduler.wait(1000)
  const quotas = await admin.describeClientQuotas(options)
  deepStrictEqual(quotas, [])
})

test('alterClientQuotas should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (entries)
  try {
    // @ts-expect-error
    await admin.alterClientQuotas({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('entries'), true)
  }

  // Test with invalid type for entries
  try {
    // @ts-expect-error
    await admin.alterClientQuotas({ entries: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('entries'), true)
  }

  // Test with empty entries array
  try {
    await admin.alterClientQuotas({ entries: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('entries'), true)
  }

  // Test with invalid validateOnly type
  try {
    await admin.alterClientQuotas({
      entries: [
        {
          entities: [{ entityType: ClientQuotaEntityTypes.USER, entityName: 'User1' }],
          ops: [{ key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000, remove: false }]
        }
      ],
      validateOnly: 'not-a-boolean'
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('validateOnly'), true)
  }

  // Test with invalid additional property
  try {
    await admin.alterClientQuotas({
      entries: [
        {
          entities: [{ entityType: ClientQuotaEntityTypes.USER, entityName: 'User1' }],
          ops: [{ key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000, remove: false }]
        }
      ],
      invalidProperty: true
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('alterClientQuotas should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections], 1)

  try {
    await admin.alterClientQuotas({
      entries: [
        {
          entities: [{ entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: 'Client1' }],
          ops: [{ key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000, remove: false }]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Altering client quotas failed.'), true)
  }
})

test('alterClientQuotas should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 1)

  try {
    await admin.alterClientQuotas({
      entries: [
        {
          entities: [{ entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: 'Client1' }],
          ops: [{ key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000, remove: false }]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Altering client quotas failed.'), true)
  }
})

test('alterClientQuotas should handle errors from the API', async t => {
  const admin = createAdmin(t)

  mockAPI(admin[kConnections], alterClientQuotasV1.api.key)

  try {
    await admin.alterClientQuotas({
      entries: [
        {
          entities: [{ entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: 'Client1' }],
          ops: [{ key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000, remove: false }]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Altering client quotas failed.'), true)
  }
})

test('alterClientQuotas should handle unavailable API errors (AlterClientQuotas)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'AlterClientQuotas')

  try {
    await admin.alterClientQuotas({
      entries: [
        {
          entities: [{ entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: 'Client1' }],
          ops: [{ key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000, remove: false }]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API AlterClientQuotas.'), true)
  }
})

test('describeClientQuotas should handle strict option', async t => {
  const admin = createAdmin(t)

  await admin.alterClientQuotas({
    entries: [
      {
        entities: [{ entityType: ClientQuotaEntityTypes.USER, entityName: 'User1' }],
        ops: [
          { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000, remove: false },
          { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, value: 1000, remove: false },
          { key: ClientQuotaKeys.REQUEST_PERCENTAGE, value: 0.5, remove: false }
        ]
      }
    ],
    validateOnly: false
  })
  await admin.alterClientQuotas({
    entries: [
      {
        entities: [
          { entityType: ClientQuotaEntityTypes.USER, entityName: 'User1' },
          { entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: 'Client1' }
        ],
        ops: [
          { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000, remove: false },
          { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, value: 1000, remove: false },
          { key: ClientQuotaKeys.REQUEST_PERCENTAGE, value: 0.5, remove: false }
        ]
      }
    ],
    validateOnly: false
  })

  await retry(10, 100, async () => {
    const result = await admin.describeClientQuotas({
      components: [
        {
          entityType: ClientQuotaEntityTypes.USER,
          matchType: ClientQuotaMatchTypes.EXACT,
          match: 'User1'
        }
      ],
      strict: false
    })
    deepStrictEqual(result, [
      {
        entity: [
          {
            entityType: ClientQuotaEntityTypes.USER,
            entityName: 'User1'
          }
        ],
        values: [
          { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, value: 1000 },
          { key: ClientQuotaKeys.REQUEST_PERCENTAGE, value: 0.5 },
          { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000 }
        ]
      },
      {
        entity: [
          {
            entityType: ClientQuotaEntityTypes.CLIENT_ID,
            entityName: 'Client1'
          },
          {
            entityType: ClientQuotaEntityTypes.USER,
            entityName: 'User1'
          }
        ],
        values: [
          { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, value: 1000 },
          { key: ClientQuotaKeys.REQUEST_PERCENTAGE, value: 0.5 },
          { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000 }
        ]
      }
    ])
    const strictResult = await admin.describeClientQuotas({
      components: [
        {
          entityType: ClientQuotaEntityTypes.USER,
          matchType: ClientQuotaMatchTypes.EXACT,
          match: 'User1'
        }
      ],
      strict: true
    })
    deepStrictEqual(strictResult, [
      {
        entity: [
          {
            entityType: ClientQuotaEntityTypes.USER,
            entityName: 'User1'
          }
        ],
        values: [
          { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, value: 1000 },
          { key: ClientQuotaKeys.REQUEST_PERCENTAGE, value: 0.5 },
          { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000 }
        ]
      }
    ])

    // Cleanup
    await admin.alterClientQuotas({
      entries: [
        {
          entities: [
            { entityType: ClientQuotaEntityTypes.USER, entityName: 'User1' },
            { entityType: ClientQuotaEntityTypes.CLIENT_ID, entityName: 'Client1' }
          ],
          ops: [
            { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, remove: true },
            { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, remove: true },
            { key: ClientQuotaKeys.REQUEST_PERCENTAGE, remove: true }
          ]
        }
      ],
      validateOnly: false
    })
    await admin.alterClientQuotas({
      entries: [
        {
          entities: [{ entityType: ClientQuotaEntityTypes.USER, entityName: 'User1' }],
          ops: [
            { key: ClientQuotaKeys.PRODUCER_BYTE_RATE, remove: true },
            { key: ClientQuotaKeys.CONSUMER_BYTE_RATE, remove: true },
            { key: ClientQuotaKeys.REQUEST_PERCENTAGE, remove: true }
          ]
        }
      ],
      validateOnly: false
    })
  })
})

test('describeClientQuotas and alterClientQuotas should support diagnostic channels', async t => {
  const admin = createAdmin(t)

  const clientQuotaEntries: alterClientQuotasV1.AlterClientQuotasRequestEntry[] = [
    {
      entities: [{ entityType: ClientQuotaEntityTypes.CLIENT_ID }],
      ops: [{ key: ClientQuotaKeys.PRODUCER_BYTE_RATE, value: 1000, remove: false }]
    }
  ]

  const alterOptions = { entries: clientQuotaEntries, validateOnly: false }

  const verifyAlterTracingChannel = createTracingChannelVerifier(
    adminClientQuotasChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'alterClientQuotas',
          options: alterOptions,
          operationId: mockedOperationId
        })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        // The result should be an array of alter responses
        strictEqual(Array.isArray(context.result), true)
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'alterClientQuotas'
  )

  await admin.alterClientQuotas(alterOptions)

  verifyAlterTracingChannel()

  const options = {
    components: [
      {
        entityType: ClientQuotaEntityTypes.CLIENT_ID,
        matchType: ClientQuotaMatchTypes.ANY,
        match: null
      }
    ],
    strict: false
  }

  const verifyDescribeTracingChannel = createTracingChannelVerifier(
    adminClientQuotasChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'describeClientQuotas',
          options,
          operationId: mockedOperationId
        })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        strictEqual(Array.isArray(context.result), true)
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'describeClientQuotas'
  )

  const quotas = await admin.describeClientQuotas(options)

  strictEqual(Array.isArray(quotas), true)

  for (const quota of quotas) {
    strictEqual(Array.isArray(quota.entity), true)
    strictEqual(Array.isArray(quota.values), true)

    for (const entity of quota.entity) {
      strictEqual(typeof entity.entityType, 'string')
      ok(entity.entityName === null || typeof entity.entityName === 'string')
    }

    for (const value of quota.values) {
      strictEqual(typeof value.key, 'string')
      strictEqual(typeof value.value, 'number')
    }
  }

  verifyDescribeTracingChannel()
})

test('describeClientQuotas should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (components)
  try {
    // @ts-expect-error
    await admin.describeClientQuotas({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('components'), true)
  }

  // Test with invalid type for components
  try {
    // @ts-expect-error
    await admin.describeClientQuotas({ components: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('components'), true)
  }

  // Test with empty components array
  try {
    await admin.describeClientQuotas({ components: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('components'), true)
  }

  // Test with invalid component object (missing entityType)
  try {
    await admin.describeClientQuotas({
      components: [{ matchType: 0, match: null }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('entityType'), true)
  }

  // Test with invalid component object (missing matchType)
  try {
    await admin.describeClientQuotas({
      components: [{ entityType: 'client-id', match: null }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('matchType'), true)
  }

  // Test with invalid entityType type
  try {
    await admin.describeClientQuotas({
      components: [{ entityType: 123, matchType: 0, match: null }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('entityType'), true)
  }

  // Test with empty entityType
  try {
    await admin.describeClientQuotas({
      components: [{ entityType: '', matchType: 0, match: null }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('entityType'), true)
  }

  // Test with invalid matchType type
  try {
    await admin.describeClientQuotas({
      components: [{ entityType: 'client-id', matchType: 'not-a-number', match: null }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('matchType'), true)
  }

  // Test with invalid match type (should be string or null)
  try {
    await admin.describeClientQuotas({
      components: [{ entityType: 'client-id', matchType: 0, match: 123 }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('match'), true)
  }

  // Test with invalid strict type
  try {
    await admin.describeClientQuotas({
      components: [{ entityType: 'client-id', matchType: 0, match: null }],
      strict: 'not-a-boolean'
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('strict'), true)
  }

  // Test with invalid additional property in component
  try {
    await admin.describeClientQuotas({
      components: [{ entityType: 'client-id', matchType: 0, match: null, invalidProperty: true }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid additional property in options
  try {
    await admin.describeClientQuotas({
      components: [{ entityType: 'client-id', matchType: 0, match: null }],
      invalidProperty: true
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('describeClientQuotas should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections], 1)

  try {
    await admin.describeClientQuotas({
      components: [
        { entityType: ClientQuotaEntityTypes.CLIENT_ID, matchType: ClientQuotaMatchTypes.EXACT, match: 'Client1' }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing client quotas failed.'), true)
  }
})

test('describeClientQuotas should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 1)

  try {
    await admin.describeClientQuotas({
      components: [
        { entityType: ClientQuotaEntityTypes.CLIENT_ID, matchType: ClientQuotaMatchTypes.EXACT, match: 'Client1' }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing client quotas failed.'), true)
  }
})

test('describeClientQuotas should handle errors from the API', async t => {
  const admin = createAdmin(t)

  mockAPI(admin[kConnections], describeClientQuotasV0.api.key)

  try {
    await admin.describeClientQuotas({
      components: [
        { entityType: ClientQuotaEntityTypes.CLIENT_ID, matchType: ClientQuotaMatchTypes.EXACT, match: 'Client1' }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing client quotas failed.'), true)
  }
})

test('describeClientQuotas should handle unavailable API errors (DescribeClientQuotas)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'DescribeClientQuotas')

  try {
    await admin.describeClientQuotas({
      components: [
        { entityType: ClientQuotaEntityTypes.CLIENT_ID, matchType: ClientQuotaMatchTypes.EXACT, match: 'Client1' }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API DescribeClientQuotas.'), true)
  }
})

test('describeLogDirs should describe log directories for topics and support diagnostic channels', async t => {
  const admin = createAdmin(t)

  const topicName1 = `test-topic-${randomUUID()}`
  await admin.createTopics({
    topics: [topicName1],
    partitions: 2,
    replicas: 1
  })
  const topicName2 = `test-topic-${randomUUID()}`
  const topicName3 = `test-topic-${randomUUID()}`
  await admin.createTopics({
    topics: [topicName2, topicName3],
    partitions: 4,
    replicas: 2
  })

  const options = {
    topics: [
      {
        name: topicName1,
        partitions: [0, 1]
      },
      {
        name: topicName2,
        partitions: [0, 2, 3]
      }
    ]
  }

  const verifyTracingChannel = createTracingChannelVerifier(
    adminLogDirsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'describeLogDirs',
          options,
          operationId: mockedOperationId
        })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        const result = context.result as BrokerLogDirDescription[]
        ok(result)
        strictEqual(Array.isArray(result), true)
        strictEqual(result.length, 3)
        deepStrictEqual(result.map(r => r.broker).sort(), [1, 2, 3])
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'describeLogDirs'
  )

  await scheduler.wait(1000)

  const responses = await admin.describeLogDirs(options)

  strictEqual(responses.length, 3)
  for (const response of responses) {
    strictEqual(typeof response.throttleTimeMs, 'number')
    for (const result of response.results) {
      strictEqual(result.logDir, '/var/lib/kafka/data')
      for (const topic of result.topics) {
        if (![topicName1, topicName2].includes(topic.name)) {
          // At least for Kafka 7.5.0 we get information for more topics than we requested.
          // So skip them here.
          continue
        }
        for (const partition of topic.partitions) {
          if (topic.name === topicName1) {
            strictEqual([0, 1].includes(partition.partitionIndex), true)
          } else {
            strictEqual([0, 2, 3].includes(partition.partitionIndex), true)
          }
          strictEqual(typeof partition.partitionSize, 'bigint')
          strictEqual(typeof partition.offsetLag, 'bigint')
          strictEqual(typeof partition.isFutureKey, 'boolean')
        }
      }
    }
  }

  // Each partition of a topic must be listed only once among all broker responses
  deepStrictEqual(
    responses
      .flatMap(response =>
        response.results.flatMap(result =>
          result.topics
            .filter(topic => topic.name === topicName1)
            .flatMap(topic => topic.partitions.map(partition => partition.partitionIndex))))
      .sort(),
    [0, 1]
  )
  // With 2 replicas each partition of a topic must be listed twice among all broker responses
  deepStrictEqual(
    responses
      .flatMap(response =>
        response.results.flatMap(result =>
          result.topics
            .filter(topic => topic.name === topicName2)
            .flatMap(topic => topic.partitions.map(partition => partition.partitionIndex))))
      .sort(),
    [0, 0, 2, 2, 3, 3]
  )

  verifyTracingChannel()

  // Clean up
  await admin.deleteTopics({ topics: [topicName1, topicName2] })
})

test('describeLogDirs should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (topics)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.describeLogDirs({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid type for topics
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.describeLogDirs({ topics: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with empty topics array
  try {
    await admin.describeLogDirs({ topics: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid topic object (missing name)
  try {
    await admin.describeLogDirs({
      topics: [{ partitions: [0] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (missing partitions)
  try {
    await admin.describeLogDirs({
      topics: [{ name: 'test-topic' }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitions'), true)
  }

  // Test with invalid name type
  try {
    await admin.describeLogDirs({
      topics: [{ name: 123, partitions: [0] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with empty name
  try {
    await admin.describeLogDirs({
      topics: [{ name: '', partitions: [0] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid partitions type
  try {
    await admin.describeLogDirs({
      topics: [{ name: 'test-topic', partitions: 'not-an-array' }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitions'), true)
  }

  // Test with empty partitions array
  try {
    await admin.describeLogDirs({
      topics: [{ name: 'test-topic', partitions: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitions'), true)
  }

  // Test with invalid partition number
  try {
    await admin.describeLogDirs({
      topics: [{ name: 'test-topic', partitions: [-1] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitions'), true)
  }

  // Test with invalid additional property in topic
  try {
    await admin.describeLogDirs({
      topics: [{ name: 'test-topic', partitions: [0], invalidProperty: true }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid additional property in options
  try {
    await admin.describeLogDirs({
      topics: [{ name: 'test-topic', partitions: [0] }],
      invalidProperty: true
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('describeLogDirs should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 4)

  try {
    // Attempt to delete groups - should fail with connection error
    await admin.describeLogDirs({
      topics: [{ name: 'test-topic', partitions: [0] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing log dirs failed.'), true)
  }
})

test('describeLogDirs should handle errors from the API', async t => {
  const admin = createAdmin(t)

  mockAPI(admin[kConnections], describeLogDirsV4.api.key)

  try {
    await admin.describeLogDirs({
      topics: [{ name: 'test-topic', partitions: [0] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing log dirs failed.'), true)
  }
})

test('describeLogDirs should handle unavailable API errors', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'DescribeLogDirs')

  try {
    await admin.describeLogDirs({
      topics: [{ name: 'test-topic', partitions: [0] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API DescribeLogDirs.'), true)
  }
})

test('listConsumerGroupOffsets should list selected offsets', async t => {
  const groupId = `test-group-${randomUUID()}`
  const topicName = `test-topic-${randomUUID()}`

  const admin = createAdmin(t)
  const consumer = createConsumer(t, { groupId })
  await admin.createTopics({ topics: [topicName], partitions: 3 })
  await consumer.joinGroup({})
  await consumer.commit({ offsets: [{ topic: topicName, partition: 0, leaderEpoch: 1000, offset: BigInt(1) }] })
  await consumer.commit({ offsets: [{ topic: topicName, partition: 1, leaderEpoch: 1001, offset: BigInt(2) }] })
  await consumer.commit({ offsets: [{ topic: topicName, partition: 2, leaderEpoch: 1002, offset: BigInt(3) }] })

  const listOptions = {
    groups: [
      {
        groupId,
        topics: [
          {
            name: topicName,
            partitionIndexes: [0, 1]
          }
        ]
      }
    ],
    requireStable: false
  }

  const offsets = await admin.listConsumerGroupOffsets(listOptions)

  offsets[0].topics[0].partitions.sort((a, b) => a.partitionIndex - b.partitionIndex)
  deepStrictEqual(offsets, [
    {
      groupId,
      topics: [
        {
          name: topicName,
          partitions: [
            {
              partitionIndex: 0,
              committedOffset: 1n,
              committedLeaderEpoch: 1000,
              metadata: ''
            },
            {
              partitionIndex: 1,
              committedOffset: 2n,
              committedLeaderEpoch: 1001,
              metadata: ''
            }
          ]
        }
      ]
    }
  ])
})

test('listConsumerGroupOffsets should list all offsets', async t => {
  const groupId = `test-group-${randomUUID()}`
  const topicName = `test-topic-${randomUUID()}`

  const admin = createAdmin(t)
  const consumer = createConsumer(t, { groupId })
  await admin.createTopics({ topics: [topicName], partitions: 3 })
  await consumer.joinGroup({})
  await consumer.commit({ offsets: [{ topic: topicName, partition: 0, leaderEpoch: 1000, offset: BigInt(1) }] })
  await consumer.commit({ offsets: [{ topic: topicName, partition: 1, leaderEpoch: 1001, offset: BigInt(2) }] })
  await consumer.commit({ offsets: [{ topic: topicName, partition: 2, leaderEpoch: 1002, offset: BigInt(3) }] })

  const listOptions = {
    groups: [
      {
        groupId,
        topics: null
      }
    ],
    requireStable: false
  }

  const offsets = await admin.listConsumerGroupOffsets(listOptions)

  offsets[0].topics[0].partitions.sort((a, b) => a.partitionIndex - b.partitionIndex)
  deepStrictEqual(offsets, [
    {
      groupId,
      topics: [
        {
          name: topicName,
          partitions: [
            {
              partitionIndex: 0,
              committedOffset: 1n,
              committedLeaderEpoch: 1000,
              metadata: ''
            },
            {
              partitionIndex: 1,
              committedOffset: 2n,
              committedLeaderEpoch: 1001,
              metadata: ''
            },
            {
              partitionIndex: 2,
              committedOffset: 3n,
              committedLeaderEpoch: 1002,
              metadata: ''
            }
          ]
        }
      ]
    }
  ])
})

test('alterConsumerGroupOffsets should alter offsets', async t => {
  const groupId = `test-group-${randomUUID()}`
  const topicName = `test-topic-${randomUUID()}`

  const admin = createAdmin(t)
  await admin.createTopics({ topics: [topicName], partitions: 2 })

  const alterOptions = {
    groupId,
    topics: [
      {
        name: topicName,
        partitionOffsets: [
          { partition: 0, offset: BigInt(10) },
          { partition: 1, offset: BigInt(20) }
        ]
      }
    ]
  }

  await admin.alterConsumerGroupOffsets(alterOptions)

  const listOptions = {
    groups: [
      {
        groupId,
        topics: [
          {
            name: topicName,
            partitionIndexes: [0, 1]
          }
        ]
      }
    ],
    requireStable: false
  }

  const offsets = await admin.listConsumerGroupOffsets(listOptions)

  deepStrictEqual(offsets, [
    {
      groupId,
      topics: [
        {
          name: topicName,
          partitions: [
            {
              partitionIndex: 0,
              committedOffset: 10n,
              committedLeaderEpoch: -1,
              metadata: ''
            },
            {
              partitionIndex: 1,
              committedOffset: 20n,
              committedLeaderEpoch: -1,
              metadata: ''
            }
          ]
        }
      ]
    }
  ])
})

test('deleteConsumerGroupOffsets should delete offsets', async t => {
  const groupId = `test-group-${randomUUID()}`
  const topicName = `test-topic-${randomUUID()}`

  const admin = createAdmin(t)
  const consumer = createConsumer(t, { groupId })
  await admin.createTopics({ topics: [topicName], partitions: 2 })
  await consumer.joinGroup({})
  await consumer.commit({ offsets: [{ topic: topicName, partition: 0, leaderEpoch: 1000, offset: BigInt(1) }] })
  await consumer.commit({ offsets: [{ topic: topicName, partition: 1, leaderEpoch: 1001, offset: BigInt(2) }] })

  const listOptions = {
    groups: [
      {
        groupId,
        topics: [
          {
            name: topicName,
            partitionIndexes: [0, 1]
          }
        ]
      }
    ],
    requireStable: false
  }

  const offsets1 = await admin.listConsumerGroupOffsets(listOptions)

  deepStrictEqual(offsets1, [
    {
      groupId,
      topics: [
        {
          name: topicName,
          partitions: [
            {
              partitionIndex: 0,
              committedOffset: 1n,
              committedLeaderEpoch: 1000,
              metadata: ''
            },
            {
              partitionIndex: 1,
              committedOffset: 2n,
              committedLeaderEpoch: 1001,
              metadata: ''
            }
          ]
        }
      ]
    }
  ])

  const deleteOptions = {
    groupId,
    topics: [
      {
        name: topicName,
        partitionIndexes: [1]
      }
    ]
  }

  const deleteResult = await admin.deleteConsumerGroupOffsets(deleteOptions)

  deepStrictEqual(deleteResult, [
    {
      name: topicName,
      partitionIndexes: [1]
    }
  ])

  const offsets2 = await admin.listConsumerGroupOffsets(listOptions)

  deepStrictEqual(offsets2, [
    {
      groupId,
      topics: [
        {
          name: topicName,
          partitions: [
            {
              partitionIndex: 0,
              committedOffset: 1n,
              committedLeaderEpoch: 1000,
              metadata: ''
            },
            {
              partitionIndex: 1,
              committedOffset: -1n,
              committedLeaderEpoch: -1,
              metadata: ''
            }
          ]
        }
      ]
    }
  ])
})

test('listConsumerGroupOffsets should support diagnostic channels', async t => {
  const groupId = `test-group-${randomUUID()}`
  const topicName = `test-topic-${randomUUID()}`

  const admin = createAdmin(t)
  await admin.createTopics({ topics: [topicName], partitions: 2 })

  const verifyListTracingChannel = createTracingChannelVerifier(
    adminConsumerGroupOffsetsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'listConsumerGroupOffsets',
          options: listOptions,
          operationId: mockedOperationId
        })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        const result = context.result as ListConsumerGroupOffsetsGroup[]
        ok(result)
        strictEqual(Array.isArray(result), true)
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'listConsumerGroupOffsets'
  )

  const listOptions = {
    groups: [
      {
        groupId,
        topics: null
      }
    ],
    requireStable: false
  }

  await admin.listConsumerGroupOffsets(listOptions)

  verifyListTracingChannel()
})

test('alterConsumerGroupOffsets should support diagnostic channels', async t => {
  const groupId = `test-group-${randomUUID()}`
  const topicName = `test-topic-${randomUUID()}`

  const admin = createAdmin(t)
  await admin.createTopics({ topics: [topicName], partitions: 2 })

  const alterOptions = {
    groupId,
    topics: [
      {
        name: topicName,
        partitionOffsets: [
          { partition: 0, offset: BigInt(10) },
          { partition: 1, offset: BigInt(20) }
        ]
      }
    ]
  }

  const verifyAlterTracingChannel = createTracingChannelVerifier(
    adminConsumerGroupOffsetsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'alterConsumerGroupOffsets',
          options: alterOptions,
          operationId: mockedOperationId
        })
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'alterConsumerGroupOffsets'
  )

  await admin.alterConsumerGroupOffsets(alterOptions)

  verifyAlterTracingChannel()
})

test('deleteConsumerGroupOffsets should support diagnostic channels', async t => {
  const groupId = `test-group-${randomUUID()}`
  const topicName = `test-topic-${randomUUID()}`

  const admin = createAdmin(t)
  const consumer = createConsumer(t, { groupId })
  await admin.createTopics({ topics: [topicName], partitions: 2 })
  await consumer.joinGroup({})
  await consumer.commit({ offsets: [{ topic: topicName, partition: 0, leaderEpoch: 1000, offset: BigInt(1) }] })

  const verifyDeleteTracingChannel = createTracingChannelVerifier(
    adminConsumerGroupOffsetsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'deleteConsumerGroupOffsets',
          options: deleteOptions,
          operationId: mockedOperationId
        })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        const result = context.result
        ok(result)
        strictEqual(Array.isArray(result), true)
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'deleteConsumerGroupOffsets'
  )

  const deleteOptions = {
    groupId,
    topics: [
      {
        name: topicName,
        partitionIndexes: [1]
      }
    ]
  }

  await admin.deleteConsumerGroupOffsets(deleteOptions)

  verifyDeleteTracingChannel()
})

test('listConsumerGroupOffsets should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (groups)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.listConsumerGroupOffsets({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with invalid type for groups
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.listConsumerGroupOffsets({ groups: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with empty groups array
  try {
    await admin.listConsumerGroupOffsets({ groups: [], requireStable: false })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with invalid requireStable type
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0] }] }],
      requireStable: 'not-a-boolean'
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('requireStable'), true)
  }

  // Test with invalid additional property
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0] }] }],
      invalidProperty: true
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid group (wrong type)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [0] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must match exactly one schema in oneOf'), true)
  }

  // Test with invalid group (empty string)
  try {
    await admin.listConsumerGroupOffsets({
      groups: ['']
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have fewer than 1 characters'), true)
  }

  // Test with invalid group object (no groupId)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{}] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with invalid group object (wrong type for groupId)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 123, topics: [{ name: 'test-topic', partitionIndexes: [0] }] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with invalid group object (empty groupId)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: '', topics: [{ name: 'test-topic', partitionIndexes: [0] }] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with invalid group object (additional property)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [
        { groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0] }], invalidProperty: true }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid group object (invalid topics type)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: 'not-an-array' }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid group object (empty topics array)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid topic object (missing name)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ partitionIndexes: [0] }] as any }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (empty name)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: '', partitionIndexes: [0] }] as any }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (wrong type for name)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 123, partitionIndexes: [0] }] as any }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (missing partitions)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 'test-topic' }] as any }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionIndexes'), true)
  }

  // Test with invalid topic object (empty partitions array)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [] }] as any }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionIndexes'), true)
  }

  // Test with invalid topic object (invalid partitions type)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [
        { groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: 'not-an-array' }] as any }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionIndexes'), true)
  }

  // Test with invalid topic object (additional property)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [
        { groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0], invalidProperty: true }] as any }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid partition (invalid type)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [
        { groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: ['not-a-number'] }] as any }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionIndexes'), true)
  }

  // Test with invalid partition (invalid negative number)
  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [-1] }] as any }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionIndexes'), true)
  }
})

test('alterConsumerGroupOffsets should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (groupId)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.alterConsumerGroupOffsets({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with missing required field (topics)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.alterConsumerGroupOffsets({ groupId: 'test-group' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid additional property
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }],
      invalidProperty: true
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid type for groupId
  try {
    await admin.alterConsumerGroupOffsets({
      // @ts-expect-error - Intentionally passing invalid options
      groupId: 123,
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with invalid empty groupId
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: '',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with invalid type for topics
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.alterConsumerGroupOffsets({ groupId: 'test-group', topics: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid empty topics array
  try {
    await admin.alterConsumerGroupOffsets({ groupId: 'test-group', topics: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid topic object (wrong type)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [0] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must be object'), true)
  }

  // Test with invalid topic object (missing name)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (empty name)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: '', partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (wrong type for name)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 123, partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (missing partitionOffsets)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic' }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionOffsets'), true)
  }

  // Test with invalid topic object (empty partitionOffsets)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionOffsets'), true)
  }

  // Test with invalid topic object (wrong type for partitionOffsets)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: 'not-an-array' }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionOffsets'), true)
  }

  // Test with invalid topic object (invalid additional property)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [
        { name: 'test-topic', partitionOffsets: [{ partition: 0, offset: BigInt(10) }], invalidProperty: true }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid partitionOffset object (wrong type)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [0] as any }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partition'), true)
  }

  // Test with invalid partitionOffset object (missing partition)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ offset: BigInt(10) }] as any }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partition'), true)
  }

  // Test with invalid partitionOffset object (invalid partition type)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 'not-a-number', offset: BigInt(10) }] as any }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partition'), true)
  }

  // Test with invalid partitionOffset object (negative partition number)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: -1, offset: BigInt(10) }] as any }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partition'), true)
  }

  // Test with invalid partitionOffset object (missing offset)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 0 }] as any }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('offset'), true)
  }

  // Test with invalid partitionOffset object (invalid offset type)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 0, offset: 'not-a-bigint' }] as any }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('offset'), true)
  }

  // Test with invalid partitionOffset object (additional property)
  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [
        { name: 'test-topic', partitionOffsets: [{ partition: 0, offset: BigInt(10), invalidProperty: true }] as any }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('deleteConsumerGroupOffsets should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing required field (groupId)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteConsumerGroupOffsets({})
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with missing required field (topics)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteConsumerGroupOffsets({ groupId: 'test-group' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid type for groupId
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteConsumerGroupOffsets({ groupId: 123, topics: [{ name: 'test-topic', partitionIndexes: [0] }] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('groupId'), true)
  }

  // Test with invalid type for topics
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await admin.deleteConsumerGroupOffsets({ groupId: 'test-group', topics: 'not-an-array' })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with empty topics array
  try {
    await admin.deleteConsumerGroupOffsets({ groupId: 'test-group', topics: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid additional property
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [0] }],
      invalidProperty: true
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid topic object (missing name)
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ partitionIndexes: [0] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (empty name)
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: '', partitionIndexes: [0] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (wrong type for name)
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 123, partitionIndexes: [0] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid topic object (missing partitions)
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic' }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionIndexes'), true)
  }

  // Test with invalid topic object (empty partitions array)
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionIndexes'), true)
  }

  // Test with invalid topic object (invalid partitions type)
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: 'not-an-array' }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionIndexes'), true)
  }

  // Test with invalid topic object (additional property)
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [0], invalidProperty: true }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid partition (invalid type)
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: ['not-a-number'] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionIndexes'), true)
  }

  // Test with invalid partition (invalid negative number)
  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [-1] }] as any
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('partitionIndexes'), true)
  }
})

test('listConsumerGroupOffsets should handle errors from Base.metadata', async t => {
  const admin = createAdmin(t)

  mockMetadata(admin)

  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0] }] }],
      requireStable: false
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('alterConsumerGroupOffsets should handle errors from Base.metadata', async t => {
  const admin = createAdmin(t)

  mockMetadata(admin)

  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('deleteConsumerGroupOffsets should handle errors from Base.metadata', async t => {
  const admin = createAdmin(t)

  mockMetadata(admin)

  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [0] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('listConsumerGroupOffsets should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections], 3)

  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0] }] }],
      requireStable: false
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Listing consumer group offsets failed.'), true)
  }
})

test('alterConsumerGroupOffsets should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections], 3)

  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Altering consumer group offsets failed.'), true)
  }
})

test('deleteConsumerGroupOffsets should handle errors from Connection.getFirstAvailable', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGetFirstAvailable(admin[kConnections], 3)

  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [0] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Deleting consumer group offsets failed.'), true)
  }
})

test('listConsumerGroupOffsets should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 4)

  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0] }] }],
      requireStable: false
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Listing consumer group offsets failed.'), true)
  }
})

test('alterConsumerGroupOffsets should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 4)

  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Altering consumer group offsets failed.'), true)
  }
})

test('deleteConsumerGroupOffsets should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)

  mockConnectionPoolGet(admin[kConnections], 4)

  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [0] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Deleting consumer group offsets failed.'), true)
  }
})

test('listConsumerGroupOffsets should handle unavailable API errors (FindCoordinator)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'FindCoordinator')

  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0] }] }],
      requireStable: false
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0] instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Listing consumer group offsets failed.'), true)
    strictEqual(error.errors[0].message.includes('Unsupported API FindCoordinator.'), true)
  }
})

test('alterConsumerGroupOffsets should handle unavailable API errors (FindCoordinator)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'FindCoordinator')

  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0] instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Altering consumer group offsets failed.'), true)
    strictEqual(error.errors[0].message.includes('Unsupported API FindCoordinator.'), true)
  }
})

test('deleteConsumerGroupOffsets should handle unavailable API errors (FindCoordinator)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'FindCoordinator')

  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [0] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0] instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Deleting consumer group offsets failed.'), true)
    strictEqual(error.errors[0].message.includes('Unsupported API FindCoordinator.'), true)
  }
})

test('listConsumerGroupOffsets should handle unavailable API errors (OffsetFetch)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'OffsetFetch')

  try {
    await admin.listConsumerGroupOffsets({
      groups: [{ groupId: 'test-group', topics: [{ name: 'test-topic', partitionIndexes: [0] }] }],
      requireStable: false
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0] instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Listing consumer group offsets failed.'), true)
    strictEqual(error.errors[0].message.includes('Unsupported API OffsetFetch.'), true)
  }
})

test('alterConsumerGroupOffsets should handle unavailable API errors (OffsetCommit)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'OffsetCommit')

  try {
    await admin.alterConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionOffsets: [{ partition: 0, offset: BigInt(10) }] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0] instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Altering consumer group offsets failed.'), true)
    strictEqual(error.errors[0].message.includes('Unsupported API OffsetCommit.'), true)
  }
})

test('deleteConsumerGroupOffsets should handle unavailable API errors (OffsetDelete)', async t => {
  const admin = createAdmin(t)

  mockUnavailableAPI(admin, 'OffsetDelete')

  try {
    await admin.deleteConsumerGroupOffsets({
      groupId: 'test-group',
      topics: [{ name: 'test-topic', partitionIndexes: [0] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0] instanceof UnsupportedApiError, true)
    strictEqual(error.message.includes('Deleting consumer group offsets failed.'), true)
    strictEqual(error.errors[0].message.includes('Unsupported API OffsetDelete.'), true)
  }
})

test('describeConfigs should list configs (TOPIC resource type)', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)
  const initialResult = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: []
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })
  strictEqual(Array.isArray(initialResult), true)
  strictEqual(initialResult.length, 1)
  strictEqual(initialResult[0].resourceType, ConfigResourceTypes.TOPIC)
  strictEqual(initialResult[0].resourceName, topicName)
  strictEqual(Array.isArray(initialResult[0].configs), true)
  strictEqual(initialResult[0].configs.length > 0, true)
  for (const config of initialResult[0].configs) {
    strictEqual(typeof config.name, 'string')
    strictEqual(typeof config.value, 'string')
    strictEqual(typeof config.readOnly, 'boolean')
    strictEqual(typeof config.configSource, 'number')
    strictEqual(typeof config.isSensitive, 'boolean')
    strictEqual(Array.isArray(config.synonyms), true)
    strictEqual(typeof config.configType, 'number')
    strictEqual(config.documentation, null)
  }

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('describeConfigs should list only desired configs (TOPIC resource type)', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)
  const initialResult = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: ['cleanup.policy', 'retention.ms']
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })
  deepStrictEqual(initialResult, [
    {
      resourceType: ConfigResourceTypes.TOPIC,
      resourceName: topicName,
      configs: [
        {
          name: 'cleanup.policy',
          value: 'delete',
          readOnly: false,
          configSource: ConfigSources.DEFAULT_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LIST,
          documentation: null
        },
        {
          name: 'retention.ms',
          value: '604800000',
          readOnly: false,
          configSource: ConfigSources.DEFAULT_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LONG,
          documentation: null
        }
      ]
    }
  ])

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('describeConfigs should include documentation and synonyms if requested (TOPIC resource type)', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)
  const initialResult = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: ['cleanup.policy']
      }
    ],
    includeSynonyms: true,
    includeDocumentation: true
  })
  deepStrictEqual(initialResult, [
    {
      resourceType: ConfigResourceTypes.TOPIC,
      resourceName: topicName,
      configs: [
        {
          name: 'cleanup.policy',
          value: 'delete',
          readOnly: false,
          configSource: ConfigSources.DEFAULT_CONFIG,
          isSensitive: false,
          synonyms: [
            {
              name: 'log.cleanup.policy',
              source: ConfigSources.DEFAULT_CONFIG,
              value: 'delete'
            }
          ],
          configType: ConfigTypes.LIST,
          documentation:
            'This config designates the retention policy to use on log segments. The "delete" policy (which is the default) will discard old segments when their retention time or size limit has been reached. The "compact" policy will enable <a href="#compaction">log compaction</a>, which retains the latest value for each key. It is also possible to specify both policies in a comma-separated list (e.g. "delete,compact"). In this case, old segments will be discarded per the retention time and size configuration, while retained segments will be compacted.'
        }
      ]
    }
  ])

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('describeConfigs should properly describe broker configs', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)

  // This test is more about not crashing at all. If any request with Broker resource type
  // is sent to the wrong broker, the broker will respond with an error and the test will fail.
  const result = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '1',
        configurationKeys: []
      },
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '2',
        configurationKeys: []
      },
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '3',
        configurationKeys: []
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })

  ok(result)
  strictEqual(result.length, 3)
  strictEqual(
    result.every(r => r.resourceType === ConfigResourceTypes.BROKER),
    true
  )
  strictEqual(result[0].resourceName, '1')
  strictEqual(Array.isArray(result[0].configs), true)
  strictEqual(result[1].resourceName, '2')
  strictEqual(Array.isArray(result[1].configs), true)
  strictEqual(result[2].resourceName, '3')
  strictEqual(Array.isArray(result[2].configs), true)

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('describeConfigs should support diagnostic channels', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)
  const options = {
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: []
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  }
  const verifyTracingChannel = createTracingChannelVerifier(
    adminConfigsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'describeConfigs',
          options,
          operationId: mockedOperationId
        })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        const result = context.result as ConfigDescription[]
        strictEqual(Array.isArray(result), true)
        strictEqual(result.length, 1)
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'describeConfigs'
  )
  await admin.describeConfigs(options)
  verifyTracingChannel()

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('describeConfigs should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing resources
  try {
    await admin.describeConfigs({} as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid additional property
  try {
    await admin.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configurationKeys: []
        }
      ],
      invalidProperty: true
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid type for resources
  try {
    await admin.describeConfigs({ resources: 'not-an-array' } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid type for includeSynonyms
  try {
    await admin.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configurationKeys: []
        }
      ],
      includeSynonyms: 'not-a-boolean'
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('includeSynonyms'), true)
  }

  // Test with invalid type for includeDocumentation
  try {
    await admin.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configurationKeys: []
        }
      ],
      includeDocumentation: 'not-a-boolean'
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('includeDocumentation'), true)
  }

  // Test with empty resources array
  try {
    await admin.describeConfigs({ resources: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid type for resources elements
  try {
    await admin.describeConfigs({
      resources: ['not-an-object'] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid additional property in resource object
  try {
    await admin.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configurationKeys: [],
          invalidProperty: true
        } as any
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with missing resourceType in resource object
  try {
    await admin.describeConfigs({
      resources: [{ resourceName: 'test-topic', configurationKeys: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceType'), true)
  }

  // Test with missing resourceName in resource object
  try {
    await admin.describeConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, configurationKeys: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceName'), true)
  }

  // Test with invalid type for resourceType
  try {
    await admin.describeConfigs({
      resources: [{ resourceType: true, resourceName: 'test-topic', configurationKeys: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceType'), true)
  }

  // Test with invalid enum member for resourceType
  try {
    await admin.describeConfigs({
      resources: [{ resourceType: 9999, resourceName: 'test-topic', configurationKeys: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceType'), true)
  }

  // Test with invalid type for resourceName
  try {
    await admin.describeConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: 123, configurationKeys: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceName'), true)
  }

  // Test with invalid empty resourceName
  try {
    await admin.describeConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: '', configurationKeys: [] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceName'), true)
  }

  // Test with invalid type for configurationKeys
  try {
    await admin.describeConfigs({
      resources: [
        { resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic', configurationKeys: 'not-an-array' }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configurationKeys'), true)
  }

  // Test with invalid type for configurationKeys elements
  try {
    await admin.describeConfigs({
      resources: [
        { resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic', configurationKeys: [123] }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configurationKeys'), true)
  }

  // Test with invalid empty configurationKeys elements
  try {
    await admin.describeConfigs({
      resources: [
        { resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic', configurationKeys: [''] }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configurationKeys'), true)
  }
})

test('describeConfigs should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)
  mockConnectionPoolGet(admin[kConnections], 4)
  try {
    await admin.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configurationKeys: []
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing configs failed.'), true)
  }
})

test('describeConfigs should handle errors from the API', async t => {
  const admin = createAdmin(t)
  mockAPI(admin[kConnections], describeConfigsV4.api.key)
  try {
    await admin.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configurationKeys: []
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing configs failed.'), true)
  }
})

test('describeConfigs should handle unavailable API errors', async t => {
  const admin = createAdmin(t)
  mockUnavailableAPI(admin, 'DescribeConfigs')
  try {
    await admin.describeConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configurationKeys: []
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API DescribeConfigs.'), true)
  }
})

test('alterConfigs should update configs', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)
  await admin.alterConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configs: [
          { name: 'cleanup.policy', value: 'compact' },
          { name: 'retention.ms', value: '86400000' }
        ]
      }
    ],
    validateOnly: false
  })

  await scheduler.wait(1000)

  const result = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: ['cleanup.policy', 'retention.ms']
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })
  deepStrictEqual(result, [
    {
      resourceType: ConfigResourceTypes.TOPIC,
      resourceName: topicName,
      configs: [
        {
          name: 'cleanup.policy',
          value: 'compact',
          readOnly: false,
          configSource: ConfigSources.TOPIC_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LIST,
          documentation: null
        },
        {
          name: 'retention.ms',
          value: '86400000',
          readOnly: false,
          configSource: ConfigSources.TOPIC_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LONG,
          documentation: null
        }
      ]
    }
  ])

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('alterConfigs should properly update broker configs', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)

  // This test is more about not crashing at all. If any request with Broker resource type
  // is sent to the wrong broker, the broker will respond with an error and the test will fail.
  await admin.alterConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '1',
        configs: [{ name: 'message.max.bytes', value: '2000000' }]
      },
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '2',
        configs: [{ name: 'message.max.bytes', value: '2000000' }]
      },
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '3',
        configs: [{ name: 'message.max.bytes', value: '2000000' }]
      }
    ]
  })

  await scheduler.wait(1000)

  const result = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '1',
        configurationKeys: ['message.max.bytes']
      },
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '2',
        configurationKeys: ['message.max.bytes']
      },
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '3',
        configurationKeys: ['message.max.bytes']
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })

  deepStrictEqual(result, [
    {
      resourceType: ConfigResourceTypes.BROKER,
      resourceName: '1',
      configs: [
        {
          name: 'message.max.bytes',
          value: '2000000',
          readOnly: false,
          configSource: ConfigSources.DYNAMIC_BROKER_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.INT,
          documentation: null
        }
      ]
    },
    {
      resourceType: ConfigResourceTypes.BROKER,
      resourceName: '2',
      configs: [
        {
          name: 'message.max.bytes',
          value: '2000000',
          readOnly: false,
          configSource: ConfigSources.DYNAMIC_BROKER_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.INT,
          documentation: null
        }
      ]
    },
    {
      resourceType: ConfigResourceTypes.BROKER,
      resourceName: '3',
      configs: [
        {
          name: 'message.max.bytes',
          value: '2000000',
          readOnly: false,
          configSource: ConfigSources.DYNAMIC_BROKER_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.INT,
          documentation: null
        }
      ]
    }
  ])

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('alterConfigs should not update configs when validateOnly is true', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)
  await admin.alterConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configs: [
          { name: 'cleanup.policy', value: 'compact' },
          { name: 'retention.ms', value: '86400000' }
        ]
      }
    ],
    validateOnly: true
  })

  await scheduler.wait(1000)

  const result = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: ['cleanup.policy', 'retention.ms']
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })
  deepStrictEqual(result, [
    {
      resourceType: ConfigResourceTypes.TOPIC,
      resourceName: topicName,
      configs: [
        {
          name: 'cleanup.policy',
          value: 'delete',
          readOnly: false,
          configSource: ConfigSources.DEFAULT_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LIST,
          documentation: null
        },
        {
          name: 'retention.ms',
          value: '604800000',
          readOnly: false,
          configSource: ConfigSources.DEFAULT_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LONG,
          documentation: null
        }
      ]
    }
  ])

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('alterConfigs should support diagnostic channels', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)
  const options = {
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configs: [
          { name: 'cleanup.policy', value: 'compact' },
          { name: 'retention.ms', value: '86400000' }
        ]
      }
    ],
    validateOnly: false
  }
  const verifyTracingChannel = createTracingChannelVerifier(
    adminConfigsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'alterConfigs',
          options,
          operationId: mockedOperationId
        })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        strictEqual(context.result, undefined)
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'alterConfigs'
  )
  await admin.alterConfigs(options)
  verifyTracingChannel()

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('alterConfigs should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing resources
  try {
    await admin.alterConfigs({} as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid additional property
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [{ name: 'cleanup.policy', value: 'compact' }]
        }
      ],
      invalidProperty: true
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid type for resources
  try {
    await admin.alterConfigs({ resources: 'not-an-array' } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid type for validateOnly
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [{ name: 'cleanup.policy', value: 'compact' }]
        }
      ],
      validateOnly: 'not-a-boolean'
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('validateOnly'), true)
  }

  // Test with empty resources array
  try {
    await admin.alterConfigs({ resources: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid type for resources elements
  try {
    await admin.alterConfigs({
      resources: ['not-an-object'] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid additional property in resource object
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [{ name: 'cleanup.policy', value: 'compact' }],
          invalidProperty: true
        } as any
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with missing resourceType in resource object
  try {
    await admin.alterConfigs({
      resources: [{ resourceName: 'test-topic', configs: [{ name: 'cleanup.policy', value: 'compact' }] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceType'), true)
  }

  // Test with missing resourceName in resource object
  try {
    await admin.alterConfigs({
      resources: [
        { resourceType: ConfigResourceTypes.TOPIC, configs: [{ name: 'cleanup.policy', value: 'compact' }] }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceName'), true)
  }

  // Test with missing configs in resource object
  try {
    await admin.alterConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic' }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configs'), true)
  }

  // Test with invalid type for resourceType
  try {
    await admin.alterConfigs({
      resources: [
        { resourceType: true, resourceName: 'test-topic', configs: [{ name: 'cleanup.policy', value: 'compact' }] }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceType'), true)
  }

  // Test with invalid enum member for resourceType
  try {
    await admin.alterConfigs({
      resources: [
        { resourceType: 9999, resourceName: 'test-topic', configs: [{ name: 'cleanup.policy', value: 'compact' }] }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceType'), true)
  }

  // Test with invalid type for resourceName
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 123,
          configs: [{ name: 'cleanup.policy', value: 'compact' }]
        }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceName'), true)
  }

  // Test with invalid empty resourceName
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: '',
          configs: [{ name: 'cleanup.policy', value: 'compact' }]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceName'), true)
  }

  // Test with invalid type for configs
  try {
    await admin.alterConfigs({
      resources: [
        { resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic', configs: 'not-an-array' }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configs'), true)
  }

  // Test with invalid empty configs array
  try {
    await admin.alterConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic', configs: [] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configs'), true)
  }

  // Test with invalid type for configs elements
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: ['not-an-object'] as any
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configs'), true)
  }

  // Test with missing name in config object
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [{ value: 'compact' }] as any
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid additional property in config object
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [{ name: 'cleanup.policy', value: 'compact', invalidProperty: true } as any]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid type for name in config object
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [{ name: 123, value: 'compact' }] as any
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid empty name in config object
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [{ name: '', value: 'compact' }]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid type for value in config object
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [{ name: 'cleanup.policy', value: 123 }] as any
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('value'), true)
  }
})

test('alterConfigs should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)
  mockConnectionPoolGet(admin[kConnections], 4)
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [{ name: 'cleanup.policy', value: 'compact' }]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Altering configs failed.'), true)
  }
})

test('alterConfigs should handle errors from the API', async t => {
  const admin = createAdmin(t)
  mockAPI(admin[kConnections], alterConfigsV2.api.key)
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [{ name: 'cleanup.policy', value: 'compact' }]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Altering configs failed.'), true)
  }
})

test('alterConfigs should handle unavailable API errors', async t => {
  const admin = createAdmin(t)
  mockUnavailableAPI(admin, 'AlterConfigs')
  try {
    await admin.alterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [{ name: 'cleanup.policy', value: 'compact' }]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API AlterConfigs.'), true)
  }
})

test('incrementalAlterConfigs should be able to set, append, subtract, and delete configs', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)
  await admin.incrementalAlterConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configs: [
          {
            name: 'cleanup.policy',
            configOperation: IncrementalAlterConfigOperationTypes.SET,
            value: 'compact'
          }
        ]
      }
    ],
    validateOnly: false
  })
  await scheduler.wait(1000)
  const setResult = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: ['cleanup.policy']
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })
  deepStrictEqual(setResult, [
    {
      resourceType: ConfigResourceTypes.TOPIC,
      resourceName: topicName,
      configs: [
        {
          name: 'cleanup.policy',
          value: 'compact',
          readOnly: false,
          configSource: ConfigSources.TOPIC_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LIST,
          documentation: null
        }
      ]
    }
  ])
  await admin.incrementalAlterConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configs: [
          {
            name: 'cleanup.policy',
            configOperation: IncrementalAlterConfigOperationTypes.APPEND,
            value: 'delete'
          }
        ]
      }
    ],
    validateOnly: false
  })
  await scheduler.wait(1000)
  const appendResult = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: ['cleanup.policy']
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })
  deepStrictEqual(appendResult, [
    {
      resourceType: ConfigResourceTypes.TOPIC,
      resourceName: topicName,
      configs: [
        {
          name: 'cleanup.policy',
          value: 'compact,delete',
          readOnly: false,
          configSource: ConfigSources.TOPIC_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LIST,
          documentation: null
        }
      ]
    }
  ])
  await admin.incrementalAlterConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configs: [
          {
            name: 'cleanup.policy',
            configOperation: IncrementalAlterConfigOperationTypes.SUBTRACT,
            value: 'compact'
          }
        ]
      }
    ],
    validateOnly: false
  })
  await scheduler.wait(1000)
  const subtractResult = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: ['cleanup.policy']
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })
  deepStrictEqual(subtractResult, [
    {
      resourceType: ConfigResourceTypes.TOPIC,
      resourceName: topicName,
      configs: [
        {
          name: 'cleanup.policy',
          value: 'delete',
          readOnly: false,
          configSource: ConfigSources.TOPIC_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LIST,
          documentation: null
        }
      ]
    }
  ])
  await admin.incrementalAlterConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configs: [
          {
            name: 'cleanup.policy',
            configOperation: IncrementalAlterConfigOperationTypes.DELETE
          }
        ]
      }
    ],
    validateOnly: false
  })
  await scheduler.wait(1000)
  const deleteResult = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: ['cleanup.policy']
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })
  deepStrictEqual(deleteResult, [
    {
      resourceType: ConfigResourceTypes.TOPIC,
      resourceName: topicName,
      configs: [
        {
          name: 'cleanup.policy',
          value: 'delete',
          readOnly: false,
          // The config has been deleted from topic config, so it falls back to default
          configSource: ConfigSources.DEFAULT_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LIST,
          documentation: null
        }
      ]
    }
  ])

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('incrementalAlterConfigs should properly update broker configs', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)

  await admin.incrementalAlterConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '1',
        configs: [
          {
            name: 'message.max.bytes',
            configOperation: IncrementalAlterConfigOperationTypes.SET,
            value: '2500000'
          }
        ]
      },
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '2',
        configs: [
          {
            name: 'message.max.bytes',
            configOperation: IncrementalAlterConfigOperationTypes.SET,
            value: '2500000'
          }
        ]
      },
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '3',
        configs: [
          {
            name: 'message.max.bytes',
            configOperation: IncrementalAlterConfigOperationTypes.SET,
            value: '2500000'
          }
        ]
      }
    ]
  })

  await scheduler.wait(1000)

  const result = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '1',
        configurationKeys: ['message.max.bytes']
      },
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '2',
        configurationKeys: ['message.max.bytes']
      },
      {
        resourceType: ConfigResourceTypes.BROKER,
        resourceName: '3',
        configurationKeys: ['message.max.bytes']
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })

  deepStrictEqual(result, [
    {
      resourceType: ConfigResourceTypes.BROKER,
      resourceName: '1',
      configs: [
        {
          name: 'message.max.bytes',
          value: '2500000',
          readOnly: false,
          configSource: ConfigSources.DYNAMIC_BROKER_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.INT,
          documentation: null
        }
      ]
    },
    {
      resourceType: ConfigResourceTypes.BROKER,
      resourceName: '2',
      configs: [
        {
          name: 'message.max.bytes',
          value: '2500000',
          readOnly: false,
          configSource: ConfigSources.DYNAMIC_BROKER_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.INT,
          documentation: null
        }
      ]
    },
    {
      resourceType: ConfigResourceTypes.BROKER,
      resourceName: '3',
      configs: [
        {
          name: 'message.max.bytes',
          value: '2500000',
          readOnly: false,
          configSource: ConfigSources.DYNAMIC_BROKER_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.INT,
          documentation: null
        }
      ]
    }
  ])

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('incrementalAlterConfigs should not update configs when validateOnly is true', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)
  await admin.incrementalAlterConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configs: [
          {
            name: 'cleanup.policy',
            configOperation: IncrementalAlterConfigOperationTypes.SET,
            value: 'compact'
          }
        ]
      }
    ],
    validateOnly: true
  })
  await scheduler.wait(200)
  const result = await admin.describeConfigs({
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configurationKeys: ['cleanup.policy']
      }
    ],
    includeSynonyms: false,
    includeDocumentation: false
  })
  deepStrictEqual(result, [
    {
      resourceType: ConfigResourceTypes.TOPIC,
      resourceName: topicName,
      configs: [
        {
          name: 'cleanup.policy',
          value: 'delete',
          readOnly: false,
          configSource: ConfigSources.DEFAULT_CONFIG,
          isSensitive: false,
          synonyms: [],
          configType: ConfigTypes.LIST,
          documentation: null
        }
      ]
    }
  ])

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('incrementalAlterConfigs should support diagnostic channels', async t => {
  const admin = createAdmin(t)
  const topicName = await createTopic(t, true)
  const options = {
    resources: [
      {
        resourceType: ConfigResourceTypes.TOPIC,
        resourceName: topicName,
        configs: [
          {
            name: 'cleanup.policy',
            configOperation: IncrementalAlterConfigOperationTypes.SET,
            value: 'compact'
          }
        ]
      }
    ],
    validateOnly: false
  }
  const verifyTracingChannel = createTracingChannelVerifier(
    adminConfigsChannel,
    'client',
    {
      start (context: ClientDiagnosticEvent) {
        deepStrictEqual(context, {
          client: admin,
          operation: 'incrementalAlterConfigs',
          options,
          operationId: mockedOperationId
        })
      },
      asyncStart (context: ClientDiagnosticEvent) {
        strictEqual(context.result, undefined)
      },
      error (context: ClientDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, data: ClientDiagnosticEvent) => data.operation === 'incrementalAlterConfigs'
  )
  await admin.incrementalAlterConfigs(options)
  verifyTracingChannel()

  // Clean up
  await admin.deleteTopics({ topics: [topicName] })
})

test('incrementalAlterConfigs should validate options in strict mode', async t => {
  const admin = createAdmin(t, { strict: true })

  // Test with missing resources
  try {
    await admin.incrementalAlterConfigs({} as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid additional property
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [
            {
              name: 'cleanup.policy',
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 'compact'
            }
          ]
        }
      ],
      invalidProperty: true
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with invalid type for resources
  try {
    await admin.incrementalAlterConfigs({ resources: 'not-an-array' } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid type for validateOnly
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [
            {
              name: 'cleanup.policy',
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 'compact'
            }
          ]
        }
      ],
      validateOnly: 'not-a-boolean'
    } as any)
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('validateOnly'), true)
  }

  // Test with empty resources array
  try {
    await admin.incrementalAlterConfigs({ resources: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid type for resources elements
  try {
    await admin.incrementalAlterConfigs({
      resources: ['not-an-object'] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resources'), true)
  }

  // Test with invalid additional property in resource object
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [
            {
              name: 'cleanup.policy',
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 'compact'
            }
          ],
          invalidProperty: true
        } as any
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with missing resourceType in resource object
  try {
    await admin.incrementalAlterConfigs({
      resources: [{ resourceName: 'test-topic', configs: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceType'), true)
  }

  // Test with missing resourceName in resource object
  try {
    await admin.incrementalAlterConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, configs: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceName'), true)
  }

  // Test with missing configs in resource object
  try {
    await admin.incrementalAlterConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic' }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configs'), true)
  }

  // Test with invalid type for resourceType
  try {
    await admin.incrementalAlterConfigs({
      resources: [{ resourceType: true, resourceName: 'test-topic', configs: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceType'), true)
  }

  // Test with invalid enum member for resourceType
  try {
    await admin.incrementalAlterConfigs({
      resources: [{ resourceType: 9999, resourceName: 'test-topic', configs: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceType'), true)
  }

  // Test with invalid type for resourceName
  try {
    await admin.incrementalAlterConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: 123, configs: [] }] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceName'), true)
  }

  // Test with invalid empty resourceName
  try {
    await admin.incrementalAlterConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: '', configs: [] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('resourceName'), true)
  }

  // Test with invalid type for configs
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        { resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic', configs: 'not-an-array' }
      ] as any
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configs'), true)
  }

  // Test with invalid empty configs array
  try {
    await admin.incrementalAlterConfigs({
      resources: [{ resourceType: ConfigResourceTypes.TOPIC, resourceName: 'test-topic', configs: [] }]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configs'), true)
  }

  // Test with invalid type for configs elements
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: ['not-an-object'] as any
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configs'), true)
  }

  // Test with invalid additional property in config object
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [
            {
              name: 'cleanup.policy',
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 'compact',
              invalidProperty: true
            } as any
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Test with missing name in config object
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [
            {
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 'compact'
            } as any
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with missing configOperation in config object
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [
            {
              name: 'cleanup.policy',
              value: 'compact'
            } as any
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configOperation'), true)
  }

  // Test with missing value in config object when operation is not DELETE
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [
            {
              name: 'cleanup.policy',
              configOperation: IncrementalAlterConfigOperationTypes.SET
            } as any
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('value'), true)
  }

  // Test with invalid type for name in config object
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [
            {
              name: 123,
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 'test'
            } as any
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid empty name in config object
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [
            {
              name: '',
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 'test'
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('name'), true)
  }

  // Test with invalid type for configOperation
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [
            {
              name: 'test.config',
              configOperation: 'not-a-number' as any,
              value: 'test'
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configOperation'), true)
  }

  // Test with invalid enum member for configOperation
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [
            {
              name: 'test.config',
              configOperation: 9999 as any,
              value: 'test'
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('configOperation'), true)
  }

  // Test with invalid type for value in config object when operation is not DELETE
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [
            {
              name: 'test.config',
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 123 as any
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('value'), true)
  }

  // Test with invalid value provided for DELETE operation
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test',
          configs: [
            {
              name: 'test.config',
              configOperation: IncrementalAlterConfigOperationTypes.DELETE,
              // @ts-expect-error - Intentionally passing invalid options
              value: 'should-not-be-provided'
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error.message.includes('value'), true)
  }
})

test('incrementalAlterConfigs should handle errors from Connection.get', async t => {
  const admin = createAdmin(t)
  mockConnectionPoolGet(admin[kConnections], 4)
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [
            {
              name: 'cleanup.policy',
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 'compact'
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Incrementally altering configs failed.'), true)
  }
})

test('incrementalAlterConfigs should handle errors from the API', async t => {
  const admin = createAdmin(t)
  mockAPI(admin[kConnections], incrementalAlterConfigsV1.api.key)
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [
            {
              name: 'cleanup.policy',
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 'compact'
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Incrementally altering configs failed.'), true)
  }
})

test('incrementalAlterConfigs should handle unavailable API errors', async t => {
  const admin = createAdmin(t)
  mockUnavailableAPI(admin, 'IncrementalAlterConfigs')
  try {
    await admin.incrementalAlterConfigs({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: 'test-topic',
          configs: [
            {
              name: 'cleanup.policy',
              configOperation: IncrementalAlterConfigOperationTypes.SET,
              value: 'compact'
            }
          ]
        }
      ]
    })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes('Unsupported API IncrementalAlterConfigs.'), true)
  }
})
