import { deepStrictEqual, ok, rejects } from 'node:assert'
import { type AddressInfo, createServer as createNetworkServer, type Server, type Socket } from 'node:net'
import { mock, test, type TestContext } from 'node:test'
import {
  type Broker,
  type CallbackWithPromise,
  Connection,
  ConnectionPool,
  type ConnectionPoolDiagnosticEvent,
  connectionsPoolGetsChannel,
  ConnectionStatuses,
  instancesChannel
} from '../../src/index.ts'
import {
  createCreationChannelVerifier,
  createTracingChannelVerifier,
  mockedErrorMessage,
  mockedOperationId,
  mockMethod
} from '../helpers.ts'

function createServer (t: TestContext): Promise<{ server: Server; port: number }> {
  const server = createNetworkServer()
  const { promise, resolve, reject } = Promise.withResolvers<{ server: Server; port: number }>()
  const sockets: Socket[] = []

  server.once('listening', () => resolve({ server, port: (server.address() as AddressInfo).port }))
  server.once('error', reject)
  server.on('connection', socket => {
    sockets.push(socket)
  })

  t.after((_, cb) => {
    for (const socket of sockets) {
      socket.end()
    }
    server.close(cb)
  })

  server.listen(0)
  return promise
}

test('constructor should support diagnostic channels', () => {
  const created = createCreationChannelVerifier(instancesChannel.name)
  const pool = new ConnectionPool('test-client')
  ok(typeof pool.instanceId === 'number')
  deepStrictEqual(created(), { type: 'connection-pool', instance: pool })
})

test('get should return a connection for a broker', async t => {
  const { port } = await createServer(t)

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  const connection = await connectionPool.get(broker)
  ok(connection instanceof Connection)
  deepStrictEqual(connection.status, ConnectionStatuses.CONNECTED)
  await connection.close()
})

test('get should return the same connection for the same broker', async t => {
  const { port } = await createServer(t)

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  const connection1 = await connectionPool.get(broker)
  const connection2 = await connectionPool.get(broker)

  deepStrictEqual(connection1, connection2)
})

test('get should handle connecting status and return same connection', async t => {
  const { port } = await createServer(t)

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  // First call will start connecting
  const connectionPromise1 = connectionPool.get(broker)

  // Second call should return the same pending connection
  const connectionPromise2 = connectionPool.get(broker)

  // Both promises should resolve to the same connection
  const connection1 = await connectionPromise1
  const connection2 = await connectionPromise2

  deepStrictEqual(connection1.socket, connection2.socket)

  await connection1.close()
})

test('get should handle connecting error and return same error', (t, done) => {
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port: 100 }

  // First call will start connecting
  const errors: (Error | null)[] = []

  function callback (error: Error | null) {
    errors.push(error)

    if (errors.length === 2) {
      deepStrictEqual(errors[0], errors[1])
      done()
    }
  }

  connectionPool.get(broker, callback)
  connectionPool.get(broker, callback)
})

test('get should handle connection drain events', async t => {
  const { port } = await createServer(t)

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  // Create a spy for the drain event
  const drainSpy = mock.fn()
  connectionPool.on('drain', drainSpy)

  const connection = await connectionPool.get(broker)

  // Simulate a drain event
  connection.emit('drain')

  // Verify the pool emitted the drain event
  deepStrictEqual(drainSpy.mock.calls.length, 1)

  await connection.close()
})

test('get should handle connection disconnect event', async t => {
  const { port } = await createServer(t)

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  // Create a spy for the disconnect event
  const disconnectSpy = mock.fn()
  connectionPool.on('disconnect', disconnectSpy)

  const connection = await connectionPool.get(broker)

  // Simulate a close event
  await connection.close()

  // Verify the pool emitted the disconnect event
  deepStrictEqual(disconnectSpy.mock.calls.length, 1)

  // Try to get the connection again - should create a new one
  const newConnection = await connectionPool.get(broker)
  ok(newConnection !== connection)

  await newConnection.close()
})

test('get should handle errors and remove connection', async t => {
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port: 100 }

  // Create a spy for the failed event
  const failedSpy = mock.fn()
  connectionPool.on('failed', failedSpy)

  // First call should fail and the connection should be removed
  await rejects(() => connectionPool.get(broker) as Promise<unknown>, {
    code: 'PLT_KFK_NETWORK'
  })

  // Verify the failed event was emitted
  deepStrictEqual(failedSpy.mock.calls.length, 1)

  // Second call should create a new connection (which will fail again)
  await rejects(() => connectionPool.get(broker) as Promise<unknown>, {
    code: 'PLT_KFK_NETWORK'
  })

  // Verify the failed event was emitted again
  deepStrictEqual(failedSpy.mock.calls.length, 2)
})

test('get should support diagnostic channels', async t => {
  const { port } = await createServer(t)
  const broker = { host: 'localhost', port }

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const verifyTracingChannel = createTracingChannelVerifier(connectionsPoolGetsChannel, ['connectionPool', 'result'], {
    start (context: ConnectionPoolDiagnosticEvent) {
      deepStrictEqual(context, { operationId: mockedOperationId, connectionPool, operation: 'get', broker })
    },
    asyncStart (context: ConnectionPoolDiagnosticEvent) {
      deepStrictEqual(context, {
        operationId: mockedOperationId,
        connectionPool,
        operation: 'get',
        broker,
        result: connection
      })
    },
    error (context: ConnectionPoolDiagnosticEvent) {
      ok(typeof context === 'undefined')
    }
  })

  const connection = await connectionPool.get(broker)

  ok(connection instanceof Connection)
  deepStrictEqual(connection.status, ConnectionStatuses.CONNECTED)
  await connection.close()

  verifyTracingChannel()
})

test('get fail when the pool is closed', async t => {
  const { port } = await createServer(t)
  const broker = { host: 'localhost', port }

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())
  await connectionPool.close()

  await rejects(() => connectionPool.get(broker) as Promise<unknown>, { message: 'Connection pool is closed.' })
})

test('getFirstAvailable should try multiple brokers until one succeeds', async t => {
  const { port } = await createServer(t)

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const brokers = [
    { host: 'localhost', port: 100 },
    { host: 'localhost', port: 200 },
    { host: 'localhost', port }
  ]

  let connectionAttempt = 0
  mockMethod(
    connectionPool,
    'get',
    current => current <= 3,
    null,
    null,
    (original, ...args) => {
      connectionAttempt++
      original(...args)
      return true
    }
  )

  const connection = await connectionPool.getFirstAvailable(brokers)

  ok(connection instanceof Connection)
  deepStrictEqual(connectionAttempt, 3)

  await connection.close()
})

test('getFirstAvailable should support diagnostic channels', async t => {
  const { port } = await createServer(t)

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const brokers = [
    { host: 'localhost', port: 100 },
    { host: 'localhost', port: 200 },
    { host: 'localhost', port }
  ]

  const verifyTracingChannel = createTracingChannelVerifier(
    connectionsPoolGetsChannel,
    ['connectionPool', 'result'],
    {
      start (context: ConnectionPoolDiagnosticEvent) {
        deepStrictEqual(context, {
          operationId: mockedOperationId,
          connectionPool,
          operation: 'getFirstAvailable',
          brokers
        })
      },
      asyncStart (context: ConnectionPoolDiagnosticEvent) {
        deepStrictEqual(context, {
          operationId: mockedOperationId,
          connectionPool,
          operation: 'getFirstAvailable',
          brokers,
          result: connection
        })
      },
      error (context: ConnectionPoolDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, context: ConnectionPoolDiagnosticEvent) => {
      return context.operation === 'getFirstAvailable'
    }
  )

  let connectionAttempt = 0
  mockMethod(
    connectionPool,
    'get',
    current => current <= 3,
    null,
    null,
    (original, ...args) => {
      connectionAttempt++
      original(...args)
      return true
    }
  )

  const connection = await connectionPool.getFirstAvailable(brokers)

  ok(connection instanceof Connection)
  deepStrictEqual(connectionAttempt, 3)

  await connection.close()

  verifyTracingChannel()
})

test('getFirstAvailable should fail if all brokers fail', async t => {
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const brokers = [
    { host: 'localhost', port: 100 },
    { host: 'localhost', port: 200 },
    { host: 'localhost', port: 300 }
  ]

  await rejects(() => connectionPool.getFirstAvailable(brokers) as Promise<unknown>, {
    code: 'PLT_KFK_MULTIPLE',
    message: mockedErrorMessage
  })
})

test('getFirstAvailable with callback parameter', async t => {
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const brokers = [
    { host: 'localhost', port: 100 },
    { host: 'localhost', port: 200 }
  ]

  // Mock get method to simulate failure
  mock.method(connectionPool, 'get', (_: Broker, callback: CallbackWithPromise<Connection>) => {
    if (callback) callback(new Error('Connection failed'), undefined as unknown as Connection)
    return undefined
  })

  // Test with callback
  let callbackCalled = false
  connectionPool.getFirstAvailable(brokers, err => {
    callbackCalled = true
    ok(err instanceof Error)
  })

  // Give time for the callback to be processed
  await new Promise(resolve => setTimeout(resolve, 10))

  ok(callbackCalled, 'Callback should have been called')
})

test('close should close all connections', async t => {
  const server1 = await createServer(t)
  const server2 = await createServer(t)

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker1 = { host: 'localhost', port: server1.port }
  const broker2 = { host: 'localhost', port: server2.port }

  const connection1 = await connectionPool.get(broker1)
  const connection2 = await connectionPool.get(broker2)

  await connectionPool.close()

  ok(connection1.socket.closed)
  ok(connection2.socket.closed)
})

test('close should handle empty pool', async () => {
  const connectionPool = new ConnectionPool('test-client')
  await connectionPool.close()
})

test('emits connecting and connect events', async t => {
  const { port } = await createServer(t)

  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  // Create spies for events
  const connectStartSpy = mock.fn()
  const connectSpy = mock.fn()
  const failedSpy = mock.fn()

  connectionPool.on('connecting', connectStartSpy)
  connectionPool.on('connect', connectSpy)
  connectionPool.on('failed', failedSpy)

  const connection = await connectionPool.get(broker)

  // Verify events were emitted in correct order
  deepStrictEqual(connectStartSpy.mock.calls.length, 1)
  deepStrictEqual(connectSpy.mock.calls.length, 1)
  deepStrictEqual(failedSpy.mock.calls.length, 0)

  await connection.close()
})

test('emits failed event on connection failure', async t => {
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port: 100 }

  // Create spy for failed event
  const failedSpy = mock.fn()
  connectionPool.on('failed', failedSpy)

  await rejects(() => connectionPool.get(broker) as Promise<unknown>, {
    code: 'PLT_KFK_NETWORK'
  })

  // Verify failed event was emitted
  deepStrictEqual(failedSpy.mock.calls.length, 1)
})

test('isActive should return false when no connections exist', () => {
  const connectionPool = new ConnectionPool('test-client')

  // Pool should not be ready when empty
  deepStrictEqual(connectionPool.isActive(), false)
})

test('isActive should return true when connections exist', async t => {
  const { port } = await createServer(t)
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  // Before connection
  deepStrictEqual(connectionPool.isActive(), false)

  // Get a connection
  const connection = await connectionPool.get(broker)

  // After connection
  deepStrictEqual(connectionPool.isActive(), true)

  await connection.close()
})

test('isActive should return false after closing all connections', async t => {
  const { port } = await createServer(t)
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  // Get a connection
  await connectionPool.get(broker)
  deepStrictEqual(connectionPool.isActive(), true)

  // Close all connections
  await connectionPool.close()

  // After closing
  deepStrictEqual(connectionPool.isActive(), false)
})

test('isConnected should return false when no connections exist', () => {
  const connectionPool = new ConnectionPool('test-client')

  // Pool should not be live when empty
  deepStrictEqual(connectionPool.isConnected(), false)
})

test('isConnected should return true when all connections are live', async t => {
  const { port } = await createServer(t)
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  // Before connection
  deepStrictEqual(connectionPool.isConnected(), false)

  // Get a connection
  const connection = await connectionPool.get(broker)

  // After connection
  deepStrictEqual(connectionPool.isConnected(), true)

  await connection.close()
})

test('isConnected should return true when connection is connecting', async t => {
  const { port } = await createServer(t)
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  // Before connection
  deepStrictEqual(connectionPool.isConnected(), false)

  // Get a connection but do not wait for it to connect
  const connectionPromise = connectionPool.get(broker)
  deepStrictEqual(connectionPool.isConnected(), false)

  // Complete the connection
  await connectionPromise
  deepStrictEqual(connectionPool.isConnected(), true)
})

test('isConnected should handle connection removal after close', async t => {
  const server1 = await createServer(t)
  const server2 = await createServer(t)
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker1 = { host: 'localhost', port: server1.port }
  const broker2 = { host: 'localhost', port: server2.port }

  // Get two connections
  const connection1 = await connectionPool.get(broker1)
  const connection2 = await connectionPool.get(broker2)

  // Both connections are live
  deepStrictEqual(connectionPool.isConnected(), true)

  // Close one connection - it will be removed from the pool automatically
  await connection1.close()

  // Pool should still be live because the closed connection is removed,
  // and the remaining connection is live
  deepStrictEqual(connectionPool.isConnected(), true)

  await connection2.close()

  deepStrictEqual(connectionPool.isConnected(), false)
})

test('isConnected should return false after closing the pool', async t => {
  const { port } = await createServer(t)
  const connectionPool = new ConnectionPool('test-client')
  t.after(() => connectionPool.close())

  const broker = { host: 'localhost', port }

  // Get a connection
  await connectionPool.get(broker)
  deepStrictEqual(connectionPool.isConnected(), true)

  // Close the pool
  await connectionPool.close()

  // After closing
  deepStrictEqual(connectionPool.isConnected(), false)
})
