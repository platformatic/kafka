import EventEmitter from 'node:events'
import {
  createPromisifiedCallback,
  kCallbackPromise,
  runConcurrentCallbacks,
  type CallbackWithPromise
} from '../apis/callbacks.ts'
import { type Callback } from '../apis/definitions.ts'
import { connectionsPoolGetsChannel, createDiagnosticContext, notifyCreation } from '../diagnostic.ts'
import { MultipleErrors } from '../errors.ts'
import { Connection, ConnectionStatuses, type Broker, type ConnectionOptions } from './connection.ts'

let currentInstance = 0

export class ConnectionPool extends EventEmitter {
  #instanceId: number
  #clientId: string
  // @ts-ignore This is used just for debugging
  #ownerId: number | undefined
  #connections: Map<string, Connection>
  #connectionOptions: ConnectionOptions

  constructor (clientId: string, connectionOptions: ConnectionOptions = {}) {
    super()
    this.#instanceId = currentInstance++
    this.#clientId = clientId
    this.#ownerId = connectionOptions.ownerId
    this.#connections = new Map()
    this.#connectionOptions = connectionOptions

    notifyCreation('connection-pool', this)
  }

  get instanceId (): number {
    return this.#instanceId
  }

  get (broker: Broker, callback: CallbackWithPromise<Connection>): void
  get (broker: Broker): Promise<Connection>
  get (broker: Broker, callback?: CallbackWithPromise<Connection>): void | Promise<Connection> {
    if (!callback) {
      callback = createPromisifiedCallback<Connection>()
    }

    connectionsPoolGetsChannel.traceCallback(
      this.#get,
      1,
      createDiagnosticContext({ connectionPool: this, broker, operation: 'get' }),
      this,
      broker,
      callback
    )

    return callback[kCallbackPromise]
  }

  getFirstAvailable (brokers: Broker[], callback: CallbackWithPromise<Connection>): void
  getFirstAvailable (brokers: Broker[]): Promise<Connection>
  getFirstAvailable (brokers: Broker[], callback?: CallbackWithPromise<Connection>): void | Promise<Connection> {
    if (!callback) {
      callback = createPromisifiedCallback<Connection>()
    }

    connectionsPoolGetsChannel.traceCallback(
      this.#getFirstAvailable,
      3,
      createDiagnosticContext({ connectionPool: this, brokers, operation: 'getFirstAvailable' }),
      this,
      brokers,
      0,
      [],
      callback
    )

    return callback[kCallbackPromise]
  }

  close (callback: CallbackWithPromise<void>): void
  close (): Promise<void>
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this.#connections.size === 0) {
      callback(null, undefined)
      return callback[kCallbackPromise]
    }

    runConcurrentCallbacks(
      'Closing connections failed.',
      this.#connections,
      ([key, connection]: [string, Connection], cb: Callback<void>) => {
        connection.close(cb)
        this.#connections.delete(key)
      },
      // @ts-ignore
      (...args) => callback(args[0], undefined)
    )

    return callback[kCallbackPromise]
  }

  #get (broker: Broker, callback: Callback<Connection>): void {
    const key = `${broker.host}:${broker.port}`
    const existing = this.#connections.get(key)

    if (existing) {
      if (existing.status !== ConnectionStatuses.CONNECTED) {
        existing.ready((...args) => {
          const error = args[0]
          if (error) {
            callback(error)
            return
          }

          callback(null, existing)
        })
      } else {
        callback(null, existing)
      }

      return
    }

    const connection = new Connection(this.#clientId, this.#connectionOptions)
    this.#connections.set(key, connection)

    const eventPayload = { broker, connection }

    this.emit('connecting', eventPayload)

    connection.connect(broker.host, broker.port, (...args) => {
      const error = args[0]
      if (error) {
        this.#connections.delete(key)
        this.emit('failed', eventPayload)

        callback(error)
        return
      }

      this.emit('connect', eventPayload)
      callback(null, connection)
    })

    connection.on('sasl:handshake', mechanisms => {
      this.emit('sasl:handshake', { ...eventPayload, mechanisms })
    })

    connection.on('sasl:authentication', authentication => {
      this.emit('sasl:authentication', { ...eventPayload, authentication })
    })

    // Remove stale connections from the pool
    connection.once('close', () => {
      this.emit('disconnect', eventPayload)
      this.#connections.delete(key)
    })

    connection.once('error', () => {
      this.#connections.delete(key)
    })

    connection.on('drain', () => {
      this.emit('drain', eventPayload)
    })
  }

  #getFirstAvailable (
    brokers: Broker[],
    current: number = 0,
    errors: Error[] = [],
    callback: CallbackWithPromise<Connection>
  ): void {
    this.get(brokers[current], (...args) => {
      const [error, connection] = args
      if (error) {
        errors.push(error)

        if (current === brokers.length - 1) {
          callback(new MultipleErrors('Cannot connect to any broker.', errors))
          return
        }

        this.#getFirstAvailable(brokers, current + 1, errors, callback)
        return
      }

      callback(null, connection)
    })
  }
}
