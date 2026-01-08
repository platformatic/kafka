import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { scheduler } from 'node:timers/promises'
import { ClientQuotaEntityTypes, ClientQuotaKeys } from '../../../src/apis/enumerations.ts'
import { kConnections } from '../../../src/clients/base/base.ts'
import {
  Admin,
  adminClientQuotasChannel,
  adminGroupsChannel,
  adminLogDirsChannel,
  adminTopicsChannel,
  alterClientQuotasV1,
  type Broker,
  type BrokerLogDirDescription,
  type Callback,
  type CallbackWithPromise,
  type ClientDiagnosticEvent,
  ClientQuotaMatchTypes,
  type ClusterPartitionMetadata,
  type Connection,
  Consumer,
  type CreatedTopic,
  type DescribeClientQuotasOptions,
  describeClientQuotasV0,
  describeGroupsV5,
  describeLogDirsV4,
  EMPTY_BUFFER,
  type GroupBase,
  instancesChannel,
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
            callback(new ResponseError(19, 7, { '/': 41 }, {}), undefined as unknown as ReturnType)
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
            callback(new ResponseError(20, 6, { '/': 41 }, {}), undefined as unknown as ReturnType)
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
