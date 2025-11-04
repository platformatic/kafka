import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { test } from 'node:test'
import { kConnections, kGetApi } from '../../../src/clients/base/base.ts'
import {
  apiVersionsV3,
  Base,
  baseApisChannel,
  baseMetadataChannel,
  Connection,
  MultipleErrors,
  sleep,
  UnsupportedApiError,
  type ClientDiagnosticEvent,
  type ClusterMetadata
} from '../../../src/index.ts'
import {
  createBase,
  createTracingChannelVerifier,
  mockAPI,
  mockConnectionPoolGet,
  mockConnectionPoolGetFirstAvailable,
  mockedErrorMessage,
  mockedOperationId
} from '../../helpers.ts'

test('constructor should properly set getters', () => {
  const base = new Base({ clientId: 'clientId', bootstrapBrokers: ['localhost:9092'], strict: true })

  ok(typeof base.instanceId, 'number')
  deepStrictEqual(base.clientId, 'clientId')
  deepStrictEqual(base.closed, false)
  deepStrictEqual(base.type, 'base')
})

test('constructor should throw on invalid options when strict mode is enabled', () => {
  // Missing required clientId
  try {
    // @ts-expect-error - Intentionally passing invalid options
    // eslint-disable-next-line no-new
    new Base({
      bootstrapBrokers: ['localhost:9092'],
      strict: true
    })
    throw new Error('Should have thrown for missing clientId')
  } catch (error: any) {
    strictEqual(error.message.includes('clientId'), true)
  }

  // Missing required bootstrapBrokers
  try {
    // @ts-expect-error - Intentionally passing invalid options
    // eslint-disable-next-line no-new
    new Base({
      clientId: 'test-client',
      strict: true
    })
    throw new Error('Should have thrown for missing bootstrapBrokers')
  } catch (error: any) {
    strictEqual(error.message.includes('bootstrapBrokers'), true)
  }

  // Invalid timeout type
  try {
    // eslint-disable-next-line no-new
    new Base({
      clientId: 'test-client',
      bootstrapBrokers: ['localhost:9092'],
      // @ts-expect-error - Intentionally passing invalid option type
      timeout: 'not-a-number',
      strict: true
    })
    throw new Error('Should have thrown for invalid timeout type')
  } catch (error: any) {
    strictEqual(error.message.includes('timeout'), true)
  }

  // Invalid negative timeout value
  try {
    // eslint-disable-next-line no-new
    new Base({
      clientId: 'test-client',
      bootstrapBrokers: ['localhost:9092'],
      timeout: -1, // Negative value
      strict: true
    })
    throw new Error('Should have thrown for negative timeout')
  } catch (error: any) {
    strictEqual(error.message.includes('timeout'), true)
  }

  // Invalid broker format
  try {
    // eslint-disable-next-line no-new
    new Base({
      clientId: 'test-client',
      // @ts-expect-error - Intentionally passing invalid broker format
      bootstrapBrokers: [123], // Not a string or broker object
      strict: true
    })
    throw new Error('Should have thrown for invalid broker format')
  } catch (error: any) {
    strictEqual(error.message.includes('bootstrapBrokers'), true)
  }

  // Valid options should not throw
  const client = new Base({
    clientId: 'test-client',
    bootstrapBrokers: ['localhost:9092'],
    timeout: 1000,
    strict: true
  })

  strictEqual(client instanceof Base, true)
  client.close()
})

test('close should properly terminate client', async t => {
  const client = createBase(t)

  // Close the client
  await client.close()

  // Verify the client is closed
  strictEqual(client.closed, true)
})

test('isActive should return true when client is not closed', t => {
  const client = createBase(t)

  // Client should be ready when not closed
  strictEqual(client.isActive(), true)
})

test('isActive should return false when client is closed', async t => {
  const client = createBase(t)

  // Close the client
  await client.close()

  // Client should not be ready when closed
  strictEqual(client.isActive(), false)
})

test('isConnected should return false when client is closed', async t => {
  const client = createBase(t)

  // Close the client
  await client.close()

  // Client should not be live when closed
  strictEqual(client.isConnected(), false)
})

test('isConnected should delegate to connection pool when client is not closed', t => {
  const client = createBase(t)

  let isConnectedCalled = false
  client[kConnections].isConnected = function () {
    isConnectedCalled = true
    return true
  }

  const result = client.isConnected()

  strictEqual(isConnectedCalled, true)
  strictEqual(result, true)
})

test('listApis should return a list of available APIs', async t => {
  const client = createBase(t)

  const apis = await client.listApis()

  const produceApi = apis.find(api => api.name === 'Produce')!
  deepStrictEqual(produceApi.name, 'Produce')
  deepStrictEqual(produceApi.minVersion, 0)
  ok(produceApi.maxVersion > 0)
})

test('listApis should support diagnostic channels', async t => {
  const client = createBase(t)

  const verifyTracingChannel = createTracingChannelVerifier(baseApisChannel, 'client', {
    start (context: ClientDiagnosticEvent) {
      deepStrictEqual(context, { client, operation: 'listApis', operationId: mockedOperationId })
    },
    error (context: ClientDiagnosticEvent) {
      ok(typeof context === 'undefined')
    }
  })

  // Fetch metadata for the cluster
  const metadataPromise = client.listApis()

  await metadataPromise
  verifyTracingChannel()
})

test('listApis should support both callback and promise API', (t, done) => {
  const client = createBase(t)

  // Use callback API
  client.listApis((err, apis) => {
    strictEqual(err, null)
    ok(Array.isArray(apis))

    client
      .close()
      .then(() => done())
      .catch(done)
  })
})

test('listApis should handle errors from Connection.getFirstAvailable', async t => {
  const client = createBase(t)

  mockConnectionPoolGetFirstAvailable(client[kConnections])

  // Attempt to find coordinator with the mocked connection
  try {
    await client.listApis()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('listApis should handle errors from the API (apiVersions)', async t => {
  const client = createBase(t)

  mockAPI(client[kConnections], apiVersionsV3.api.key)

  // Attempt to commit with mocked error
  try {
    await client.listApis()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message, mockedErrorMessage)
  }
})

test('metadata should fetch cluster metadata', async t => {
  const client = createBase(t)

  // Fetch metadata for the cluster
  const metadata = await client.metadata({ topics: [] })

  // Basic validation of metadata structure
  strictEqual(typeof metadata.id, 'string')
  strictEqual(metadata.brokers instanceof Map, true)
  strictEqual(metadata.topics instanceof Map, true)
  strictEqual(typeof metadata.lastUpdate, 'number')

  // Verify that brokers contains at least one broker
  strictEqual(metadata.brokers.size > 0, true)

  // Verify first broker structure
  const firstBrokerId = metadata.brokers.keys().next().value!
  const firstBroker = metadata.brokers.get(firstBrokerId)!
  strictEqual(typeof firstBroker.host, 'string')
  strictEqual(typeof firstBroker.port, 'number')
})

test('metadata should fetch topic metadata', async t => {
  const client = createBase(t)

  // Create a unique test topic name
  const testTopic = `test-topic-${randomUUID()}`

  // Fetch metadata with autocreate
  const metadata = await client.metadata({
    topics: [testTopic],
    autocreateTopics: true
  })

  // Verify the topic was created and metadata contains information about it
  strictEqual(metadata.topics.has(testTopic), true)

  // Check topic metadata structure
  const topicMetadata = metadata.topics.get(testTopic)!
  strictEqual(typeof topicMetadata.id, 'string')
  strictEqual(Array.isArray(topicMetadata.partitions), true)
  strictEqual(typeof topicMetadata.partitionsCount, 'number')
  strictEqual(topicMetadata.partitionsCount > 0, true)

  // Verify partition structure
  const firstPartition = topicMetadata.partitions[0]
  strictEqual(typeof firstPartition.leader, 'number')
  strictEqual(typeof firstPartition.leaderEpoch, 'number')
  strictEqual(Array.isArray(firstPartition.replicas), true)
})

test('metadata should cache metadata according to metadataMaxAge', async t => {
  const client = createBase(t, {
    metadataMaxAge: 1000 // 1 second
  })

  // Create a unique test topic
  const testTopic = `test-topic-${randomUUID()}`

  // First metadata request should fetch from broker
  const metadata1 = await client.metadata({
    topics: [testTopic],
    autocreateTopics: true
  })

  // Record the last update timestamp
  const lastUpdate1 = metadata1.lastUpdate

  // Second immediate request should use cache
  const metadata2 = await client.metadata({
    topics: [testTopic]
  })

  // The timestamps should be the same if cache was used
  strictEqual(metadata2.lastUpdate, lastUpdate1)

  // Wait for cache to expire
  await sleep(1100)

  // Third request after delay should fetch from broker again
  const metadata3 = await client.metadata({
    topics: [testTopic]
  })

  // The timestamp should be updated
  strictEqual(metadata3.lastUpdate > lastUpdate1, true)
})

test('metadata should only fetch missing topics if requested to', async t => {
  const client = createBase(t, {
    metadataMaxAge: 2000 // 2 second
  })

  // Create two unique test topics
  const testTopic1 = `test-topic-${randomUUID()}`
  const testTopic2 = `test-topic-${randomUUID()}`

  // First metadata request should fetch from broker
  const metadata1 = await client.metadata({
    topics: [testTopic1],
    autocreateTopics: true
  })

  // Record the last update timestamp
  const lastUpdate1 = metadata1.lastUpdate

  // Second immediate request should use cache
  const metadata2 = await client.metadata({
    topics: [testTopic1]
  })

  // The timestamps should be the same if cache was used
  strictEqual(metadata2.lastUpdate, lastUpdate1)

  // Third request for a different topic should fetch from broker
  const metadata3 = await client.metadata({
    topics: [testTopic2],
    autocreateTopics: true
  })

  const lastUpdate2 = metadata3.lastUpdate

  strictEqual(metadata3.lastUpdate, lastUpdate2)
  ok(metadata3.lastUpdate > lastUpdate1)

  // Fourth request for the first topic should still use cache
  const metadata4 = await client.metadata({
    topics: [testTopic1, testTopic2]
  })

  strictEqual(metadata4.lastUpdate, lastUpdate2)
  strictEqual(metadata4.topics.get(testTopic1)!.lastUpdate, lastUpdate1)
  strictEqual(metadata4.topics.get(testTopic2)!.lastUpdate, lastUpdate2)

  // Wait for cache to expire
  await sleep(2500)

  // Third request after delay should fetch from broker again
  const metadata5 = await client.metadata({
    topics: [testTopic1, testTopic2]
  })

  // The timestamp should be updated
  strictEqual(metadata5.lastUpdate > lastUpdate2, true)
})

test('metadata should support force update option', async t => {
  const client = createBase(t, {
    metadataMaxAge: 60000 // 1 minute
  })

  // Create a unique test topic
  const testTopic = `test-topic-${randomUUID()}`

  // First metadata request
  const metadata1 = await client.metadata({
    topics: [testTopic],
    autocreateTopics: true
  })

  // Record the last update timestamp
  const lastUpdate1 = metadata1.lastUpdate

  // Wait some time to avoid receiving the same timestamp
  await sleep(500)

  // Force update
  const metadata2 = await client.metadata({
    topics: [testTopic],
    forceUpdate: true
  })

  // The timestamp should be updated despite cache not being expired
  strictEqual(metadata2.lastUpdate > lastUpdate1, true)
})

test('metadata should support diagnostic channels', async t => {
  const client = createBase(t)

  const verifyTracingChannel = createTracingChannelVerifier(baseMetadataChannel, 'client', {
    start (context: ClientDiagnosticEvent) {
      deepStrictEqual(context, { client, operation: 'metadata', operationId: mockedOperationId })
    },
    error (context: ClientDiagnosticEvent) {
      ok(typeof context === 'undefined')
    }
  })

  // Fetch metadata for the cluster
  const metadataPromise = client.metadata({ topics: [] })

  await metadataPromise
  verifyTracingChannel()
})

test('metadata should support both callback and promise API', (t, done) => {
  const client = createBase(t)

  // Create a unique test topic
  const testTopic = `test-topic-${randomUUID()}`

  // Use callback API
  client.metadata({ topics: [testTopic], autocreateTopics: true }, (err, metadata) => {
    strictEqual(err, null)
    strictEqual(metadata.topics.has(testTopic), true)

    client
      .close()
      .then(() => done())
      .catch(done)
  })
})

test('metadata should emit events', async t => {
  const client = createBase(t)

  // Create a promise that resolves when metadata event is emitted
  const metadataPromise = new Promise<ClusterMetadata>(resolve => {
    client.on('client:metadata', resolve)
  })

  // Trigger metadata fetch
  client.metadata({ topics: [] })

  // Wait for the event
  const metadata = await metadataPromise

  // Verify metadata structure
  strictEqual(typeof metadata.id, 'string')
  strictEqual(metadata.brokers instanceof Map, true)
  strictEqual(metadata.topics instanceof Map, true)
})

test('metadata should return validation error in strict mode', async t => {
  const client = createBase(t, {
    strict: true
  })

  // Test with missing required field
  try {
    // @ts-expect-error - Intentionally passing invalid options
    await client.metadata({})

    // Should not reach here
    throw new Error('Expected metadata to fail with validation error')
  } catch (error: any) {
    // Verify the specific error message
    strictEqual(error.message, "/options must have required property 'topics'.")
  }

  // Test with invalid topics type
  try {
    // @ts-expect-error - Intentionally passing invalid topics type
    await client.metadata({ topics: 'not-an-array' })

    // Should not reach here
    throw new Error('Expected metadata to fail with validation error')
  } catch (error: any) {
    strictEqual(error instanceof Error, true)
    strictEqual(error.message, '/options/topics must be array.')
  }

  // Test with invalid additional property
  try {
    // @ts-expect-error - Intentionally passing invalid property
    await client.metadata({ topics: [], invalidProperty: true })

    // Should not reach here
    throw new Error('Expected metadata to fail with validation error')
  } catch (error: any) {
    strictEqual(error instanceof Error, true)
    strictEqual(error.message, '/options must NOT have additional properties.')
  }

  // Valid options should still work
  const metadata = await client.metadata({ topics: [] })
  strictEqual(typeof metadata.id, 'string')
})

test('metadata should handle connection failures to non-existent broker', async t => {
  // Create a client with a non-existent broker
  const client = createBase(t, {
    bootstrapBrokers: [{ host: '192.0.2.1', port: 9092 }], // RFC 5737 reserved IP for documentation - guaranteed to not exist
    connectTimeout: 1000, // 1 second timeout
    retries: 2 // Minimal retries to make test faster
  })

  // Expect the metadata call to fail with a network error
  try {
    await client.metadata({ topics: [] })

    // If we get here, the call unexpectedly succeeded
    throw new Error('Expected metadata call to fail with connection error')
  } catch (error: any) {
    // Should be a MultipleErrors or AggregateError instance since we use performWithRetry
    strictEqual(['MultipleErrors', 'AggregateError'].includes(error.name), true)

    // Error message should indicate failure
    strictEqual(error.message.includes('failed'), true)

    // Should contain nested errors
    strictEqual(Array.isArray(error.errors), true)
    strictEqual(error.errors.length > 0, true)

    // At least one error should be a network error
    const hasNetworkError = error.errors.some(
      (err: any) =>
        err.message.includes('ECONNREFUSED') ||
        err.message.includes('ETIMEDOUT') ||
        err.message.includes('getaddrinfo') ||
        err.message.includes('connect')
    )

    strictEqual(hasNetworkError, true)
  }
})

test('connectToBrokers should connect to all brokers by default', async t => {
  const otherClient = createBase(t)
  const metadata = await otherClient.metadata({ topics: [] })

  const client = createBase(t)
  const connections = await client.connectToBrokers()

  for (const [nodeId, nodeInfo] of metadata.brokers) {
    const connection = connections.get(nodeId)

    ok(connection instanceof Connection)
    strictEqual(connection.host, nodeInfo.host)
    strictEqual(connection.port, nodeInfo.port)
  }
})

test('connectToBrokers should connect to only select brokers and ignore invalid brokers', async t => {
  const otherClient = createBase(t)
  const metadata = await otherClient.metadata({ topics: [] })
  const firstBroker = metadata.brokers.keys().next().value!

  const client = createBase(t)
  const connections = await client.connectToBrokers([firstBroker, Math.random()])

  deepStrictEqual(Array.from(connections.keys()), [firstBroker])
})

test('connectToBrokers should handle errors from Connection.getFirstAvailable', async t => {
  const client = createBase(t)

  mockConnectionPoolGetFirstAvailable(client[kConnections])

  // Attempt to find coordinator with the mocked connection
  try {
    await client.connectToBrokers()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('connectToBrokers should handle errors from Connection.get', async t => {
  const client = createBase(t)

  mockConnectionPoolGet(client[kConnections], 3)

  // Attempt to find coordinator with the mocked connection
  try {
    await client.connectToBrokers()
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.errors[0].message.includes(mockedErrorMessage), true)
  }
})

test('listApis should handle errors from Connection.getFirstAvailable', async t => {
  const client = createBase(t)

  mockConnectionPoolGetFirstAvailable(client[kConnections], 2)

  // Attempt to find coordinator with the mocked connection
  try {
    await client.metadata({ topics: [] })
    throw new Error('Expected error not thrown')
  } catch (error) {
    strictEqual(error instanceof MultipleErrors, true)
    strictEqual(error.message.includes(mockedErrorMessage), true)
  }
})

test('emitWithDebug should emit events directly when section is null', t => {
  const client = createBase(t)

  // Create a flag to check if event was emitted
  let eventEmitted = false
  const testPayload = { test: 'data' }

  // Listen for the raw event (without section prefix)
  client.on('test-event', payload => {
    eventEmitted = true
    deepStrictEqual(payload, testPayload)
  })

  // Emit event with null section
  const result = client.emitWithDebug(null, 'test-event', testPayload)

  // Verify the event was emitted correctly
  strictEqual(eventEmitted, true)
  strictEqual(result, true)
})

test('emitWithDebug should emit events with section prefix when section is provided', t => {
  const client = createBase(t)

  // Create a flag to check if event was emitted
  let eventEmitted = false
  const testPayload = { test: 'data' }

  // Listen for the prefixed event
  client.on('test-section:test-event', payload => {
    eventEmitted = true
    deepStrictEqual(payload, testPayload)
  })

  // Event without prefix should not be triggered
  client.on('test-event', () => {
    throw new Error('Wrong event emitted')
  })

  // Emit event with a section
  const result = client.emitWithDebug('test-section', 'test-event', testPayload)

  // Verify the event was emitted correctly
  strictEqual(eventEmitted, true)
  strictEqual(result, true)
})

test('operations can be aborted without a retry', async t => {
  // Create a client with a non-existent broker
  const client = createBase(t, {
    bootstrapBrokers: ['192.0.2.1'], // RFC 5737 reserved IP for documentation - guaranteed to not exist
    connectTimeout: 1000, // 1 second timeout
    retries: 0
  })

  // Expect the metadata call to fail with a network error
  try {
    await client.metadata({ topics: [] })

    // If we get here, the call unexpectedly succeeded
    throw new Error('Expected metadata call to fail with connection error')
  } catch (error: any) {
    strictEqual(error.message, mockedErrorMessage)
  }
})

test('kGetApi should fail on unsupported API', (t, done) => {
  const client = createBase(t)

  client[kGetApi]('foo', (error, api) => {
    ok(typeof api === 'undefined')
    strictEqual(error instanceof UnsupportedApiError, true)
    strictEqual(error!.message, 'Unsupported API foo.')

    done()
  })
})

test('kGetApi should fail on unsupported API version', (t, done) => {
  const client = createBase(t)

  mockAPI(client[kConnections], apiVersionsV3.api.key, null, {
    errorCode: 0,
    throttleTimeMs: 0,
    apiKeys: [
      { name: 'Metadata', minVersion: 0, maxVersion: 11 },
      { name: 'Produce', minVersion: 30, maxVersion: 40 }
    ]
  })

  client[kGetApi]('Produce', (error, api) => {
    ok(typeof api === 'undefined')
    strictEqual(error instanceof UnsupportedApiError, true)
    strictEqual((error as UnsupportedApiError).message, 'No usable implementation found for API Produce.')
    strictEqual((error as UnsupportedApiError).minVersion, 30)
    strictEqual((error as UnsupportedApiError).maxVersion, 40)

    done()
  })
})

test('kPerformWithRetry should not leak timers', async t => {
  const client = createBase(t, { retries: 1, retryDelay: 10000 })
  const promise = client.metadata({ topics: [`test-topic-${randomUUID()}`] }).catch(e => e)

  // Wait for the first request to fail
  await once(client, 'client:performWithRetry:retry')

  // Forcefully close the client to ensure all operations are aborted
  await client.close()

  // If the timeout was already resolved, the test runner would complain
  const error = await promise

  ok(error instanceof MultipleErrors)
  strictEqual(error.errors.length, 2)
  ok(error.errors[1].message.startsWith('Client closed while retrying'))
})

test('initialization should not fail when maxInflights is specifically set to undefined', async t => {
  const client = createBase(t, { maxInflights: undefined })

  // Create a promise that resolves when metadata event is emitted
  const metadataPromise = new Promise<ClusterMetadata>(resolve => {
    client.on('client:metadata', resolve)
  })

  // Trigger metadata fetch
  client.metadata({ topics: [] })

  // Wait for the event
  const metadata = await metadataPromise

  // Verify metadata structure
  strictEqual(typeof metadata.id, 'string')
  strictEqual(metadata.brokers instanceof Map, true)
  strictEqual(metadata.topics instanceof Map, true)
})
