import { deepStrictEqual, ok, rejects, strictEqual } from 'node:assert'
import { type AddressInfo, createServer as createNetworkServer, type Server, Socket } from 'node:net'
import test, { type TestContext } from 'node:test'
import {
  Connection,
  type ConnectionDiagnosticEvent,
  connectionsApiChannel,
  connectionsConnectsChannel,
  ConnectionStatuses,
  instancesChannel,
  NetworkError,
  type Reader,
  UnexpectedCorrelationIdError,
  Writer
} from '../../src/index.ts'
import { createCreationChannelVerifier, createTracingChannelVerifier, mockedOperationId } from '../helpers.ts'

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

test('Connection constructor', () => {
  const created = createCreationChannelVerifier(instancesChannel.name)
  const connection = new Connection('test-client')

  deepStrictEqual(connection.status, ConnectionStatuses.NONE)
  deepStrictEqual(created(), { type: 'connection', instance: connection })
})

test('Connection.connect should establish a connection', async t => {
  const { port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  await connection.connect('localhost', port)

  deepStrictEqual(connection.status, ConnectionStatuses.CONNECTED)
  ok(connection.socket instanceof Socket)
})

test('Connection.connect should support diagnostic channels', async t => {
  const { port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  const verifyTracingChannel = createTracingChannelVerifier(connectionsConnectsChannel.name, ['connection'], {
    start (context: ConnectionDiagnosticEvent) {
      deepStrictEqual(context, {
        operationId: mockedOperationId,
        connection,
        operation: 'connect',
        host: 'localhost',
        port
      })
    },
    asyncStart (context: ConnectionDiagnosticEvent) {
      deepStrictEqual(context, {
        operationId: mockedOperationId,
        connection,
        operation: 'connect',
        host: 'localhost',
        port
      })
    },
    error (context: ConnectionDiagnosticEvent) {
      ok(typeof context === 'undefined')
    }
  })

  await connection.connect('localhost', port)

  deepStrictEqual(connection.status, ConnectionStatuses.CONNECTED)
  ok(connection.socket instanceof Socket)

  verifyTracingChannel()
})

test('Connection.connect should support diagnostic channel', async t => {
  const { port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  await connection.connect('localhost', port)

  deepStrictEqual(connection.status, ConnectionStatuses.CONNECTED)
  ok(connection.socket instanceof Socket)
})

test('Connection.connect should return immediately if already connected', async t => {
  const { port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  await connection.connect('localhost', port)

  const startTime = Date.now()
  await connection.connect('localhost', port)
  const elapsedTime = Date.now() - startTime

  // Should return immediately without waiting
  ok(elapsedTime < 50, `Expected connect to return immediately, but took ${elapsedTime}ms`)
})

test('Connection.connect should handle connection timeout', async t => {
  const connection = new Connection('test-client', { connectTimeout: 100 })
  t.after(() => connection.close())

  // This IP is not routable due to RFC 5737
  await rejects(() => connection.connect('192.0.2.1', 9092) as Promise<unknown>, {
    code: 'PLT_KFK_NETWORK',
    message: 'Connection to 192.0.2.1:9092 timed out.'
  })
})

test('Connection.connect should handle connection error', async t => {
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  await rejects(() => connection.connect('localhost', 100) as Promise<unknown>, {
    code: 'PLT_KFK_NETWORK',
    message: 'Connection to localhost:100 failed.'
  })
})

test('Connection.connect should support diagnostic channels when erroring', async t => {
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  const verifyTracingChannel = createTracingChannelVerifier(connectionsConnectsChannel.name as string, ['connection'], {
    start (context: ConnectionDiagnosticEvent) {
      deepStrictEqual(context, {
        operationId: mockedOperationId,
        connection,
        operation: 'connect',
        host: 'localhost',
        port: 100
      })
    },
    asyncStart (context: ConnectionDiagnosticEvent) {
      deepStrictEqual((context.error as Error).message, 'Connection to localhost:100 failed.')
    },
    error (context: ConnectionDiagnosticEvent) {
      deepStrictEqual((context.error as Error).message, 'Connection to localhost:100 failed.')
    }
  })

  await rejects(() => connection.connect('localhost', 100) as Promise<unknown>, {
    code: 'PLT_KFK_NETWORK',
    message: 'Connection to localhost:100 failed.'
  })

  deepStrictEqual(connection.status, ConnectionStatuses.ERROR)
  ok(connection.socket instanceof Socket)

  verifyTracingChannel()
})

test('Connection.ready should resolve when connection is ready', async t => {
  const { port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  const connectPromise = connection.connect('localhost', port)
  const readyPromise = connection.ready()

  await readyPromise
  await connectPromise
  deepStrictEqual(connection.status, ConnectionStatuses.CONNECTED)
  ok(connection.socket instanceof Socket)
})

test('Connection.ready should reject when connection errors', async t => {
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  const readyPromise = rejects(() => connection.connect('localhost', 100) as Promise<unknown>, {
    code: 'PLT_KFK_NETWORK',
    message: 'Connection to localhost:100 failed.'
  })

  await rejects(() => connection.connect('localhost', 100) as Promise<unknown>, {
    code: 'PLT_KFK_NETWORK',
    message: 'Connection to localhost:100 failed.'
  })

  await readyPromise
})

test('Connection.close should close the connection', async t => {
  const { port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  await connection.connect('localhost', port)

  // Calling it multiple times should not be a problem
  const promise = connection.close()
  await connection.close()
  await promise

  ok(connection.socket.closed)
})

test('Connection.send should enqueue request and process response', async t => {
  const { server, port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  const verifyTracingChannel = createTracingChannelVerifier(
    connectionsApiChannel.name,
    ['connection'],
    {
      start (context: ConnectionDiagnosticEvent) {
        deepStrictEqual(context, {
          operationId: mockedOperationId,
          connection,
          operation: 'send',
          apiKey: 1,
          apiVersion: 1,
          correlationId: 1
        })
      },
      asyncStart (context: ConnectionDiagnosticEvent) {
        deepStrictEqual(context, {
          operationId: mockedOperationId,
          connection,
          operation: 'send',
          apiKey: 1,
          apiVersion: 1,
          correlationId: 1,
          result: {
            result: 123
          }
        })
      },
      error (context: ConnectionDiagnosticEvent) {
        ok(typeof context === 'undefined')
      }
    },
    (_label: string, context: ConnectionDiagnosticEvent) => {
      return context.operation === 'send'
    }
  )

  // Create a mock server that responds to requests
  server.on('connection', socket => {
    // Handle data coming from client
    socket.on('data', data => {
      // Read the correlation ID from the request
      const correlationId = data.readInt32BE(4 + 2 + 2) // Skip size, apiKey, apiVersion
      deepStrictEqual(data.readUInt8(data.length - 3), 0) // Check tagged fields
      deepStrictEqual(data.readInt32BE(data.length - 4), 42) // Check payload

      // Write first part of the response
      // Create a mock response
      const response = Buffer.alloc(4 + 4 + 1) // size + correlationId + taggedFields
      response.writeInt32BE(9, 0) // size (9 bytes payload)
      response.writeInt32BE(correlationId, 4) // same correlationId
      response.writeUInt8(0, 8) // tagged fields

      socket.write(response)

      // Wait few milliseconds to simulate async processing, then write the rest of the response
      setTimeout(() => {
        // Create a mock result
        const result = Buffer.alloc(4) // 4 bytes for the result
        result.writeInt32BE(123) // mock result

        // Send back the response
        socket.end(result)
      }, 500)
    })
  })

  await connection.connect('localhost', port)

  // Create mock payload function
  function payloadFn () {
    const writer = Writer.create()
    writer.appendInt32(42) // Some mock data
    return writer
  }

  // Create mock parser function
  function parser (_apiKey: number, _apiVersion: number, _correlationId: number, reader: Reader) {
    return { result: reader.readInt32() }
  }

  // Send a request
  await new Promise<void>((resolve, reject) => {
    connection.send(
      1, // apiKey
      1, // apiVersion
      payloadFn,
      parser,
      true, // hasRequestHeaderTaggedFields
      true, // hasResponseHeaderTaggedFields
      (err, data) => {
        if (err) {
          reject(err)
          return
        }

        // Verify response
        try {
          deepStrictEqual(data, { result: 123 })
          resolve()
        } catch (e) {
          reject(e)
        }
      }
    )
  })

  verifyTracingChannel()
})

test('Connection.send should handle requests with no response', async t => {
  const { server, port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  // Setup a simple echo server
  server.on('connection', socket => {
    socket.on('data', () => {
      // Server does nothing with the data
    })
  })

  await connection.connect('localhost', port)

  // Create payload function that indicates no response is expected
  function payloadFn () {
    const writer = Writer.create()
    writer.appendInt32(42)
    writer.context.noResponse = true
    return writer
  }

  // Using noResponseCallback
  await new Promise<void>((resolve, reject) => {
    connection.send(
      0, // apiKey
      0, // apiVersion
      payloadFn,
      function () {
        return {}
      }, // Dummy parser
      false, // hasRequestHeaderTaggedFields
      false, // hasResponseHeaderTaggedFields
      (err, canWrite) => {
        if (err) {
          reject(err)
          return
        }

        try {
          strictEqual(canWrite, true)
          resolve()
        } catch (e) {
          reject(e)
        }
      }
    )
  })
})

test('Connection should handle socket drain events', async t => {
  const { server, port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  const writesPerRequest = 7

  server.on('connection', socket => {
    socket.resume()
  })

  await connection.connect('localhost', port)

  // Override socket's write method to track writes
  const originalWrite = connection.socket.write
  let writeCallCount = 0

  connection.socket.write = function write (...args: any[]) {
    writeCallCount++

    // Simulate backpressure
    if (writeCallCount <= writesPerRequest * 2) {
      originalWrite.call(connection.socket, ...(args as unknown as Parameters<typeof originalWrite>))
      return false
    }

    return originalWrite.call(connection.socket, ...(args as unknown as Parameters<typeof originalWrite>))
  }

  // Create payload
  function payloadFn () {
    const writer = Writer.create()
    writer.appendInt32(42)
    writer.context.noResponse = true
    return writer
  }

  connection.send(
    0, // apiKey
    0, // apiVersion
    payloadFn,
    function () {
      return {}
    },
    false,
    false,
    () => {}
  )

  connection.send(
    0, // apiKey
    0, // apiVersion
    payloadFn,
    function () {
      return {}
    },
    false,
    false,
    () => {}
  )

  connection.send(
    0, // apiKey
    0, // apiVersion
    payloadFn,
    function () {
      return {}
    },
    false,
    false,
    () => {}
  )

  connection.send(
    0, // apiKey
    0, // apiVersion
    payloadFn,
    function () {
      return {}
    },
    false,
    false,
    () => {}
  )

  // Each request has 7 buffers (4 for size, 2 for apiKey, 1 for apiVersion)

  deepStrictEqual(writeCallCount, writesPerRequest, 'Only one request should be sent immediately')

  connection.socket.emit('drain')
  deepStrictEqual(writeCallCount, 2 * writesPerRequest, 'The second request should be sent after drain')

  connection.socket.emit('drain')
  deepStrictEqual(writeCallCount, 4 * writesPerRequest, 'All other request should be sent after the second drain')
})

test('Connection should handle unexpected correlation IDs', async t => {
  const { server, port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  // Mock server that sends response with unexpected correlation ID
  server.on('connection', socket => {
    socket.on('data', () => {
      // Create response with unexpected correlation ID
      const response = Buffer.alloc(4 + 4 + 4) // size + correlationId + result
      response.writeInt32BE(8, 0) // size (8 bytes payload)
      response.writeInt32BE(99999, 4) // Unexpected correlationId
      response.writeInt32BE(123, 8) // mock result
      socket.end(response)
    })
  })

  await connection.connect('localhost', port)

  // Listen for error
  const errorPromise = new Promise<Error>(resolve => {
    connection.once('error', err => {
      resolve(err)
    })
  })

  // Create payload
  function payloadFn () {
    const writer = Writer.create()
    writer.appendInt32(42)
    return writer
  }

  // Send a request
  connection.send(
    0, // apiKey
    0, // apiVersion
    payloadFn,
    function () {
      return {}
    },
    false,
    false,
    () => {} // Dummy callback
  )

  // Wait for error
  const error = await errorPromise
  ok(error instanceof UnexpectedCorrelationIdError)
  strictEqual(error.message, 'Received unexpected response with correlation_id=99999')
})

test('Connection should handle socket errors', async t => {
  const { port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  await connection.connect('localhost', port)

  // Listen for error
  const errorPromise = new Promise<Error>(resolve => {
    connection.once('error', err => {
      resolve(err)
    })
  })

  // Simulate a socket error
  connection.socket.emit('error', new Error('Socket error'))

  // Verify error
  const error = await errorPromise
  ok(error instanceof NetworkError)
  strictEqual(error.message, 'Connection error')
  ok(error.cause instanceof Error)
  strictEqual(error.cause.message, 'Socket error')
})

test('Connection should handle close with in-flight and after-drain requests', async t => {
  const { server, port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  // Create a mock server that never responds
  const { promise: receivePromise, resolve } = Promise.withResolvers()
  server.on('connection', socket => {
    socket.once('data', resolve)
  })

  await connection.connect('localhost', port)

  // Create payload
  function payloadFn () {
    const writer = Writer.create()
    writer.appendInt32(42)
    return writer
  }

  let writeCallCount = 0
  const originalWrite = connection.socket.write
  connection.socket.write = function write (...args: any[]) {
    writeCallCount++

    // Simulate backpressure
    if (writeCallCount > 0) {
      originalWrite.call(connection.socket, ...(args as unknown as Parameters<typeof originalWrite>))
      return false
    }

    return originalWrite.call(connection.socket, ...(args as unknown as Parameters<typeof originalWrite>))
  }

  // Verify request gets error
  const requestPromise1 = new Promise((resolve, reject) => {
    connection.send(
      0, // apiKey
      0, // apiVersion
      payloadFn,
      function () {
        return {}
      },
      false,
      false,
      err => {
        if (err) {
          reject(err)
        }

        resolve(null)
      }
    )
  })

  const requestPromise2 = new Promise((resolve, reject) => {
    connection.send(
      0, // apiKey
      0, // apiVersion
      payloadFn,
      function () {
        return {}
      },
      false,
      false,
      err => {
        if (err) {
          reject(err)
        }

        resolve(null)
      }
    )
  })

  await receivePromise
  await connection.close()

  await rejects(() => requestPromise1, {
    code: 'PLT_KFK_NETWORK',
    message: 'Connection closed'
  })

  await rejects(() => requestPromise2, {
    code: 'PLT_KFK_NETWORK',
    message: 'Connection closed'
  })
})

test('Connection should handle response parsing errors', async t => {
  const { server, port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  const verifyTracingChannel = createTracingChannelVerifier(
    connectionsApiChannel.name,
    ['connection'],
    {
      error (context: ConnectionDiagnosticEvent) {
        deepStrictEqual((context.error as Error).message, 'Parser error')
      }
    },
    (_label: string, context: ConnectionDiagnosticEvent) => {
      return context.operation === 'send'
    }
  )

  // Mock server that sends valid response
  server.on('connection', socket => {
    socket.on('data', data => {
      // Read the correlation ID from the request
      const correlationId = data.readInt32BE(4 + 2 + 2) // Skip size, apiKey, apiVersion

      // Create a response
      const response = Buffer.alloc(4 + 4 + 4) // size + correlationId + result
      response.writeInt32BE(8, 0) // size (8 bytes payload)
      response.writeInt32BE(correlationId, 4) // same correlationId
      response.writeInt32BE(123, 8) // mock result
      socket.end(response)
    })
  })

  await connection.connect('localhost', port)

  // Create payload
  function payloadFn () {
    const writer = Writer.create()
    writer.appendInt32(42)
    return writer
  }

  // Create parser that throws an error
  function parser () {
    throw new Error('Parser error')
  }

  // Send a request
  await new Promise<void>((resolve, reject) => {
    connection.send(
      0, // apiKey
      0, // apiVersion
      payloadFn,
      parser,
      false,
      false,
      err => {
        try {
          ok(err instanceof Error)
          strictEqual(err.message, 'Parser error')
          resolve()
        } catch (e) {
          reject(e)
        }
      }
    )
  })

  verifyTracingChannel()
})

test('Connection should handle response with tagged fields', async t => {
  const { server, port } = await createServer(t)
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  // Mock server that sends response with tagged fields
  server.on('connection', socket => {
    socket.on('data', data => {
      // Read the correlation ID from the request
      const correlationId = data.readInt32BE(4 + 2 + 2) // Skip size, apiKey, apiVersion

      // Create a response with tagged fields
      const response = Buffer.alloc(4 + 4 + 1 + 4) // size + correlationId + tag + result
      response.writeInt32BE(9, 0) // size (9 bytes payload)
      response.writeInt32BE(correlationId, 4) // same correlationId
      response.writeUInt8(0, 8) // Tagged fields count (0)
      response.writeInt32BE(123, 9) // mock result
      socket.end(response)
    })
  })

  await connection.connect('localhost', port)

  // Create payload
  function payloadFn () {
    const writer = Writer.create()
    writer.appendInt32(42)
    return writer
  }

  // Create parser
  function parser (_apiKey: number, _apiVersion: number, _correlationId: number, reader: Reader) {
    return { result: reader.readInt32() }
  }

  // Send a request with tagged fields in response
  await new Promise<void>((resolve, reject) => {
    connection.send(
      0, // apiKey
      0, // apiVersion
      payloadFn,
      parser,
      false, // hasRequestHeaderTaggedFields
      true, // hasResponseHeaderTaggedFields - Important!
      (err, data) => {
        if (err) {
          reject(err)
          return
        }

        try {
          deepStrictEqual(data, { result: 123 })
          resolve()
        } catch (e) {
          reject(e)
        }
      }
    )
  })
})

test('Connection should handle send when not connected', async t => {
  const connection = new Connection('test-client')
  t.after(() => connection.close())

  // Create payload
  function payloadFn () {
    const writer = Writer.create()
    writer.appendInt32(42)
    return writer
  }

  // Send a request when not connected
  await new Promise<void>((resolve, reject) => {
    connection.send(
      0, // apiKey
      0, // apiVersion
      payloadFn,
      function () {
        return {}
      },
      false,
      false,
      err => {
        try {
          ok(err instanceof NetworkError)
          strictEqual(err.message, 'Connection closed')
          resolve()
        } catch (e) {
          reject(e)
        }
      }
    )
  })
})
