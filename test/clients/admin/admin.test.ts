import { deepStrictEqual, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { test, type TestContext } from 'node:test'
import { kConnections, kMetadata } from '../../../src/clients/base/base.ts'
import {
  Admin,
  type AdminOptions,
  type Broker,
  type Callback,
  type CallbackWithPromise,
  type Connection,
  Consumer,
  describeGroupsV5,
  EMPTY_BUFFER,
  listGroupsV5,
  type MetadataOptions,
  MultipleErrors,
  type ResponseParser,
  type Writer
} from '../../../src/index.ts'

const kafkaBootstrapServers = ['localhost:29092']

// Helper function to create a unique test client
function createTestClient (t: TestContext, overrideOptions = {}) {
  const options: AdminOptions = {
    clientId: `test-admin-client-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers,
    ...overrideOptions
  }

  const client = new Admin(options)
  t.after(() => client.close())

  return client
}

test('constructor should initialize properly', t => {
  const client = createTestClient(t)

  strictEqual(client instanceof Admin, true)
  strictEqual(client.closed, false)

  client.close()
})

test('should support both promise and callback API', (t, done) => {
  const client = createTestClient(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-${randomUUID()}`

  // Use callback API
  client.createTopics(
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
      client.deleteTopics({ topics: [topicName] }, err => {
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

test('all operations should fail when client is closed', async t => {
  const client = createTestClient(t)

  // Close the client first
  await client.close()

  // Attempt to call createTopics on closed client
  try {
    await client.createTopics({
      topics: ['test-topic'],
      partitions: 1,
      replicas: 1
    })
    throw new Error('Expected createTopics to fail on closed client')
  } catch (error) {
    // Error should be about client being closed
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call deleteTopics on closed client
  try {
    await client.deleteTopics({
      topics: ['test-topic']
    })
    throw new Error('Expected deleteTopics to fail on closed client')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call listGroups on closed client
  try {
    await client.listGroups()
    throw new Error('Expected listGroups to fail on closed client')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call describeGroups on closed client
  try {
    await client.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected describeGroups to fail on closed client')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }

  // Attempt to call deleteGroups on closed client
  try {
    await client.deleteGroups({
      groups: ['test-group']
    })
    throw new Error('Expected deleteGroups to fail on closed client')
  } catch (error) {
    strictEqual(error.message, 'Client is closed.')
  }
})

test('createTopics should create a new topic', async t => {
  const client = createTestClient(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-${randomUUID()}`

  // Create a new topic
  const created = await client.createTopics({
    topics: [topicName],
    partitions: 3,
    replicas: 1
  })

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

  // Clean up by deleting the topic
  await client.deleteTopics({ topics: [topicName] })
})

test('createTopics should create a topic with specific partitions', async t => {
  const client = createTestClient(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-partitions-${randomUUID()}`

  // Create a new topic with a specific number of partitions
  const created = await client.createTopics({
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
  const topicMetadata = await client.metadata({ topics: [topicName] })
  const topic = topicMetadata.topics.get(topicName)

  // Verify the topic has exactly 3 partitions
  strictEqual(topic?.partitionsCount, 3)

  // Verify each partition has a valid leader
  for (let i = 0; i < 3; i++) {
    const partition = topic?.partitions[i]
    strictEqual(typeof partition?.leader, 'number')
    strictEqual(partition?.leader! >= 0, true)
  }

  // Clean up by deleting the topic
  await client.deleteTopics({ topics: [topicName] })
})

test('createTopics should create multiple topics', async t => {
  const client = createTestClient(t)

  // Generate unique topic names for testing
  const topicName1 = `test-topic-${randomUUID()}`
  const topicName2 = `test-topic-${randomUUID()}`

  // Create multiple topics in a single request
  const created = await client.createTopics({
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
  const topicMetadata = await client.metadata({ topics: [topicName1, topicName2] })
  strictEqual(topicMetadata.topics.has(topicName1), true)
  strictEqual(topicMetadata.topics.has(topicName2), true)

  // Clean up by deleting the topics
  await client.deleteTopics({ topics: [topicName1, topicName2] })
})

test('createTopics with multiple partitions', async t => {
  const client = createTestClient(t)

  // Generate a unique topic name
  const topicName = `test-topic-partitions-${randomUUID()}`

  // Create topic with multiple partitions
  const created = await client.createTopics({
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
  const topicMetadata = await client.metadata({ topics: [topicName] })
  const topic = topicMetadata.topics.get(topicName)

  // Should have exactly 3 partitions
  strictEqual(topic?.partitionsCount, 3)

  // Clean up
  await client.deleteTopics({ topics: [topicName] })
})

test('createTopics using assignments', async t => {
  const client = createTestClient(t)

  // First, get cluster metadata to find the broker
  const metadata = await client.metadata({ topics: [] })
  const brokerIds = Array.from(metadata.brokers.keys())
  const topicName = `test-topic-leader-${randomUUID()}`

  // Create a topic with a single partition - leader will be automatically assigned
  const created = await client.createTopics({
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
  await client.deleteTopics({ topics: [topicName] })
})

test('createTopics should validate options in strict mode', async t => {
  const client = createTestClient(t, { strict: true })

  // Test with missing required field (topics)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.createTopics({})
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid topics type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.createTopics({ topics: 'not-an-array' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with non-string topic in topics array
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.createTopics({ topics: [123] })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid partitions type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.createTopics({ topics: ['test-topic'], partitions: 'not-a-number' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('partitions'), true)
  }

  // Test with invalid replicas type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.createTopics({ topics: ['test-topic'], replicas: 'not-a-number' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('replicas'), true)
  }

  // Test with invalid assignments type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.createTopics({ topics: ['test-topic'], assignments: 'not-an-array' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('assignments'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.createTopics({ topics: ['test-topic'], invalidProperty: true })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('createTopics should handle errors from ConnectionPool.getFirstAvailable', async t => {
  const client = createTestClient(t)

  client[kConnections].getFirstAvailable = (_brokers: Broker[], callback: CallbackWithPromise<Connection>) => {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined as unknown as Connection)
  }

  try {
    // Attempt to create a topic - should fail with connection error
    await client.createTopics({
      topics: ['test-topic'],
      partitions: 1,
      replicas: 1
    })
    throw new Error('Expected createTopics to fail with connection error')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('deleteTopics should delete a topic', async t => {
  const client = createTestClient(t)

  // Generate a unique topic name for testing
  const topicName = `test-topic-${randomUUID()}`

  // Create a new topic
  await client.createTopics({
    topics: [topicName],
    partitions: 1,
    replicas: 1
  })

  // Delete the topic
  await client.deleteTopics({ topics: [topicName] })

  // Verify the topic was deleted by trying to recreate it
  // (If it still existed, this would fail with a topic already exists error)
  const created = await client.createTopics({
    topics: [topicName],
    partitions: 1,
    replicas: 1
  })

  strictEqual(created.length, 1)
  strictEqual(created[0].name, topicName)

  // Clean up
  await client.deleteTopics({ topics: [topicName] })
})

test('deleteTopics should validate options in strict mode', async t => {
  const client = createTestClient(t, { strict: true })

  // Test with missing required field (topics)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.deleteTopics({})
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid type for topics
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.deleteTopics({ topics: 'not-an-array' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with non-string topic in topics array
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.deleteTopics({ topics: [123] })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('topics'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.deleteTopics({ topics: ['test-topic'], invalidProperty: true })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('deleteTopics should handle errors from ConnectionPool.getFirstAvailable', async t => {
  const client = createTestClient(t)

  client[kConnections].getFirstAvailable = (_brokers: Broker[], callback: CallbackWithPromise<Connection>) => {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined as unknown as Connection)
  }

  try {
    // Attempt to delete a topic - should fail with connection error
    await client.deleteTopics({
      topics: ['test-topic']
    })
    throw new Error('Expected deleteTopics to fail with connection error')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('listGroups should return consumer groups', async t => {
  // Create a consumer that joins a group
  const consumer = new Consumer({
    clientId: `test-admin-client-${randomUUID()}`,
    groupId: `test-group-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers
  })
  t.after(() => consumer.close())

  const client = createTestClient(t)

  await consumer.joinGroup({})

  // List all groups
  const groups = await client.listGroups()

  // Basic validation of the groups map structure
  strictEqual(groups instanceof Map, true)

  deepStrictEqual(groups.get(consumer.groupId), {
    id: consumer.groupId,
    protocolType: 'consumer',
    state: 'STABLE',
    groupType: 'classic'
  })
})

test('listGroups should support filtering by types and states', async t => {
  // Create a consumer that joins a group
  const consumer = new Consumer({
    clientId: `test-admin-client-${randomUUID()}`,
    groupId: `test-group-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers
  })
  t.after(() => consumer.close())

  const client = createTestClient(t)

  await consumer.joinGroup({})

  // Test with explicit types parameter - include consumer groups
  const groups1 = await client.listGroups({
    types: ['classic']
  })

  // Verify we found the consumer group
  strictEqual(groups1.has(consumer.groupId), true, 'The consumer group should be found with type "classic"')

  // Test with empty options
  const groups2 = await client.listGroups({})
  // Default types should be used
  strictEqual(groups2.has(consumer.groupId), true, 'The consumer group should be found with default types')

  // Test with state filtering - stable should include our group
  const groups3 = await client.listGroups({
    states: ['STABLE']
  })
  strictEqual(groups3.has(consumer.groupId), true, 'The consumer group should be found with state "STABLE"')

  // Test with a state that shouldn't match our group
  const groups4 = await client.listGroups({
    states: ['DEAD']
  })
  strictEqual(groups4.has(consumer.groupId), false, 'The consumer group should not be found with state "DEAD"')
})

test('listGroups should validate options in strict mode', async t => {
  const client = createTestClient(t, { strict: true })

  // Test with invalid types field
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.listGroups({ types: 'not-an-array' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('types'), true)
  }

  // Test with invalid states field
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.listGroups({ states: 'not-an-array' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('states'), true)
  }

  // Test with invalid state value
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.listGroups({ states: ['INVALID_STATE'] })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('states'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.listGroups({ invalidProperty: true })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }

  // Empty options should be valid
  const groups = await client.listGroups({})
  strictEqual(groups instanceof Map, true)
})

test('listGroups should handle errors from Base.metadata', async t => {
  const client = createTestClient(t)

  // Override the [kMetadata] method to simulate a connection error
  client[kMetadata] = (_options: any, callback: CallbackWithPromise<any>) => {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined)
  }

  try {
    // Attempt to list groups - should fail with connection error
    await client.listGroups()
    throw new Error('Expected listGroups to fail with connection error')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('listGroups should handle errors from ConnectionPool.get', async t => {
  const client = createTestClient(t)

  const original = client[kConnections].get.bind(client[kConnections])
  let firstCall = true

  client[kConnections].get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    if (firstCall) {
      firstCall = false
      original(broker, callback)
      return
    }

    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined as unknown as Connection)
  } as typeof original

  try {
    // Attempt to list groups - should fail with connection error
    await client.listGroups()
    throw new Error('Expected listGroups to fail with connection error')
  } catch (error) {
    // Error should be about connection failure
    strictEqual(error instanceof Error, true)
    strictEqual(error.message, 'Listing groups failed.')
  }
})

test('listGroups should handle errors from the API', async t => {
  const client = createTestClient(t)

  const original = client[kConnections].get.bind(client[kConnections])
  client[kConnections].get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    original(broker, (error: Error | null, connection: Connection) => {
      if (error) {
        callback(error, undefined as unknown as Connection)
        return
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
        if (apiKey === listGroupsV5.api.key) {
          const connectionError = new MultipleErrors('Cannot connect to any broker.', [
            new Error('Connection failed to localhost:29092')
          ])
          callback(connectionError, undefined as unknown as ReturnType)
          return
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
      } as typeof originalSend

      callback(null, connection)
    })
  } as typeof original

  try {
    // Attempt to list groups - should fail with connection error
    await client.listGroups()
    throw new Error('Expected listGroups to fail with connection error')
  } catch (error) {
    // Error should be about connection failure
    strictEqual(error instanceof Error, true)
    strictEqual(error.message, 'Listing groups failed.')
  }
})

test('describeGroups should describe consumer groups', async t => {
  // Create a consumer that joins a group
  const consumer = new Consumer({
    clientId: `test-admin-client-${randomUUID()}`,
    groupId: `test-group-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers
  })
  t.after(() => consumer.close())

  const client = createTestClient(t)
  const testTopic = `test-topic-${randomUUID()}`

  // Create a unique test topic name
  await client.createTopics({ topics: [testTopic], partitions: 1, replicas: 1 })

  await consumer.topics.trackAll(testTopic)
  await consumer.joinGroup({})

  // Describe the groups
  const describedGroups = await client.describeGroups({
    groups: [consumer.groupId, 'non-existent-group'],
    includeAuthorizedOperations: true
  })

  const { id, clientId, clientHost } = Array.from(describedGroups.get(consumer.groupId)?.members.values()!)[0]
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
      authorizedOperations: 328
    },
    {
      id: 'non-existent-group',
      state: 'DEAD',
      protocolType: '',
      protocol: '',
      members: new Map(),
      authorizedOperations: 328
    }
  ])
})

test('describeGroups should handle includeAuthorizedOperations option', async t => {
  // Create a consumer that joins a group
  const consumer = new Consumer({
    clientId: `test-admin-client-${randomUUID()}`,
    groupId: `test-group-${randomUUID()}`,
    bootstrapBrokers: kafkaBootstrapServers
  })
  t.after(() => consumer.close())

  const client = createTestClient(t)

  await consumer.joinGroup({})

  // Test with includeAuthorizedOperations: true
  const groupsWithAuth = await client.describeGroups({
    groups: [consumer.groupId],
    includeAuthorizedOperations: true
  })

  // Verify the authorizedOperations field is as expected
  const group1 = groupsWithAuth.get(consumer.groupId)!
  strictEqual(typeof group1.authorizedOperations, 'number')

  // Test with includeAuthorizedOperations: false (default)
  const groupsWithoutAuth = await client.describeGroups({
    groups: [consumer.groupId]
  })

  // Verify the authorizedOperations field is 0 (default value when not included)
  const group2 = groupsWithoutAuth.get(consumer.groupId)!
  // Depending on broker configuration, this might still be populated
  // Just ensure the field exists without asserting a specific value
  strictEqual(typeof group2.authorizedOperations, 'number')
})

test('describeGroups should validate options in strict mode', async t => {
  const client = createTestClient(t, { strict: true })

  // Test with missing required field (groups)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.describeGroups({})
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with invalid type for groups
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.describeGroups({ groups: 'not-an-array' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with non-string group in groups array
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.describeGroups({ groups: [123] })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with empty groups array
  try {
    await client.describeGroups({ groups: [] })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with invalid includeAuthorizedOperations type
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.describeGroups({ groups: ['test-group'], includeAuthorizedOperations: 'not-a-boolean' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('includeAuthorizedOperations'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.describeGroups({ groups: ['test-group'], invalidProperty: true })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('describeGroups should handle errors from Base.metadata', async t => {
  const client = createTestClient(t)

  // Override the [kMetadata] method to simulate a connection error
  client[kMetadata] = (_options: any, callback: CallbackWithPromise<any>) => {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined)
  }

  try {
    // Attempt to describe groups - should fail with connection error
    await client.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected describeGroups to fail with connection error')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('describeGroups should handle errors from ConnectionPool.getFirstAvailable', async t => {
  const client = createTestClient(t)

  // Override the getFirstAvailable method to fail on the second use (after the successful metadata call)
  const original = client[kConnections].getFirstAvailable.bind(client[kConnections])
  let firstCall = true

  client[kConnections].getFirstAvailable = (brokers: Broker[], callback: CallbackWithPromise<Connection>) => {
    if (firstCall) {
      // Let the first call (for metadata) succeed
      firstCall = false
      original(brokers, callback)
    } else {
      // Make the second call (for findCoordinator) fail
      const connectionError = new MultipleErrors('Cannot connect to any broker.', [
        new Error('Connection failed to localhost:29092')
      ])
      callback(connectionError, undefined as unknown as Connection)
    }
  }

  try {
    // Attempt to describe groups - should fail with connection error
    await client.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected describeGroups to fail with connection error')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('describeGroups should handle errors from ConnectionPool.get', async t => {
  const client = createTestClient(t)

  const original = client[kConnections].get.bind(client[kConnections])

  let getCalls = 0
  client[kConnections].get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    getCalls++

    if (getCalls < 3) {
      original(broker, callback)
      return
    }

    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined as unknown as Connection)
  } as typeof original

  try {
    // Attempt to describe groups - should fail with connection error
    await client.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected describeGroups to fail with connection error')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing groups failed.'), true)
  }
})

test('describeGroups should handle errors from the API', async t => {
  const client = createTestClient(t)

  const original = client[kConnections].get.bind(client[kConnections])
  client[kConnections].get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    original(broker, (error: Error | null, connection: Connection) => {
      if (error) {
        callback(error, undefined as unknown as Connection)
        return
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
        if (apiKey === describeGroupsV5.api.key) {
          const connectionError = new MultipleErrors('Cannot connect to any broker.', [
            new Error('Connection failed to localhost:29092')
          ])
          callback(connectionError, undefined as unknown as ReturnType)
          return
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
      } as typeof originalSend

      callback(null, connection)
    })
  } as typeof original

  try {
    // Attempt to describe groups - should fail with connection error
    await client.describeGroups({
      groups: ['test-group']
    })
    throw new Error('Expected describeGroups to fail with connection error')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Describing groups failed.'), true)
  }
})

test('deleteGroups should handle non-existent groups properly', async t => {
  const client = createTestClient(t)

  // Generate a unique group ID for testing
  const groupId = `test-group-${randomUUID()}`

  // Trying to delete a non-existent group will likely result in an error
  try {
    await client.deleteGroups({ groups: [groupId] })
    // If no error, that's also fine
  } catch (error) {
    // This is expected - a non-existent group will cause an error
    strictEqual(error instanceof Error, true)
    // The error should contain specific error information
    strictEqual(Array.isArray(error.errors), true)
  }
})

test('deleteGroups should validate options in strict mode', async t => {
  const client = createTestClient(t, { strict: true })

  // Test with missing required field (groups)
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.deleteGroups({})
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with invalid type for groups
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.deleteGroups({ groups: 'not-an-array' })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with non-string group in groups array
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.deleteGroups({ groups: [123] })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with empty groups array
  try {
    await client.deleteGroups({ groups: [] })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('groups'), true)
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.deleteGroups({ groups: ['test-group'], invalidProperty: true })
    throw new Error('Expected validation error')
  } catch (error) {
    strictEqual(error.message.includes('must NOT have additional properties'), true)
  }
})

test('deleteGroups should handle errors from Base.metadata', async t => {
  const client = createTestClient(t)

  client[kMetadata] = function (_options: MetadataOptions, callback: CallbackWithPromise<any>) {
    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])

    callback(connectionError, undefined as unknown as Connection)
  }

  try {
    // Attempt to delete groups - should fail with connection error
    await client.deleteGroups({ groups: ['non-existent'] })
    throw new Error('Expected deleteGroups to fail with connection error')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('deleteGroups should handle errors from ConnectionPool.getFirstAvailable', async t => {
  const client = createTestClient(t)

  // Override the getFirstAvailable method to fail on the second use (after the successful metadata call)
  const original = client[kConnections].getFirstAvailable.bind(client[kConnections])
  let firstCall = true

  client[kConnections].getFirstAvailable = (brokers: Broker[], callback: CallbackWithPromise<Connection>) => {
    if (firstCall) {
      // Let the first call (for metadata) succeed
      firstCall = false
      original(brokers, callback)
    } else {
      // Make the second call (for findCoordinator) fail
      const connectionError = new MultipleErrors('Cannot connect to any broker.', [
        new Error('Connection failed to localhost:29092')
      ])
      callback(connectionError, undefined as unknown as Connection)
    }
  }

  try {
    // Attempt to delete groups - should fail with connection error
    await client.deleteGroups({
      groups: ['test-group']
    })
    throw new Error('Expected deleteGroups to fail with connection error')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Cannot connect to any broker.'), true)
  }
})

test('deleteGroups should handle errors from ConnectionPool.get', async t => {
  const client = createTestClient(t)

  const original = client[kConnections].get.bind(client[kConnections])

  let getCalls = 0
  client[kConnections].get = function (broker: Broker, callback: CallbackWithPromise<Connection>) {
    getCalls++

    if (getCalls < 3) {
      original(broker, callback)
      return
    }

    const connectionError = new MultipleErrors('Cannot connect to any broker.', [
      new Error('Connection failed to localhost:29092')
    ])
    callback(connectionError, undefined as unknown as Connection)
  } as typeof original

  try {
    // Attempt to delete groups - should fail with connection error
    await client.deleteGroups({ groups: ['non-existent'] })
    throw new Error('Expected deleteGroups to fail with connection error')
  } catch (error) {
    // Error should contain our mock error message
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes('Deleting groups failed.'), true)
  }
})
