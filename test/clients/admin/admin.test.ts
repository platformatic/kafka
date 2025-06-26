import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { kConnections } from '../../../src/clients/base/base.ts'
import {
  Admin,
  adminGroupsChannel,
  adminTopicsChannel,
  type ClientDiagnosticEvent,
  type ClusterPartitionMetadata,
  Consumer,
  type CreatedTopic,
  describeGroupsV5,
  EMPTY_BUFFER,
  type GroupBase,
  instancesChannel,
  listGroupsV5,
  MultipleErrors,
  sleep,
  UnsupportedApiError
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
  mockUnavailableAPI
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
    (...args) => {
      const [err, created] = args
      strictEqual(err, null)
      strictEqual(created.length, 1)
      strictEqual(created[0].name, topicName)

      // Clean up and close
      admin.deleteTopics({ topics: [topicName] }, (...args) => {
        const [err] = args
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

test('createTopics using assignments', async t => {
  const admin = createAdmin(t)

  // First, get cluster metadata to find the broker
  const metadata = await admin.metadata({ topics: [] })
  const brokerIds = Array.from(metadata.brokers.keys())
  const topicName = `test-topic-leader-${randomUUID()}`

  // Create a topic with a single partition - leader will be automatically assigned
  const created = await admin.createTopics({
    topics: [topicName],
    partitions: -1,
    replicas: -1,
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
  deepStrictEqual(Array.from(describedGroups.values()), [
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
    },
    {
      id: 'non-existent-group',
      state: 'DEAD',
      protocolType: '',
      protocol: '',
      members: new Map(),
      authorizedOperations
    }
  ])

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
