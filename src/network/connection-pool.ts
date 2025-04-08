import EventEmitter from 'node:events'
import { type Callback } from '../apis/definitions.ts'
import {
  createPromisifiedCallback,
  kCallbackPromise,
  runConcurrentCallbacks,
  type CallbackWithPromise
} from '../clients/callbacks.ts'
import { MultipleErrors } from '../errors.ts'
import { Connection, ConnectionStatuses, type Broker, type ConnectionOptions } from './connection.ts'

export class ConnectionPool extends EventEmitter {
  #clientId: string
  // @ts-ignore This is used just for debugging
  #ownerId: number | undefined
  #connections: Map<string, Connection>
  #connectionOptions: ConnectionOptions

  constructor (clientId: string, connectionOptions: ConnectionOptions = {}) {
    super()

    this.#clientId = clientId
    this.#ownerId = connectionOptions.ownerId
    this.#connections = new Map()
    this.#connectionOptions = connectionOptions
  }

  get (broker: Broker, callback: CallbackWithPromise<Connection>): void
  get (broker: Broker): Promise<Connection>
  get (broker: Broker, callback?: CallbackWithPromise<Connection>): void | Promise<Connection> {
    const key = `${broker.host}:${broker.port}`
    const existing = this.#connections.get(key)

    if (!callback) {
      callback = createPromisifiedCallback<Connection>()
    }

    if (existing) {
      if (existing.status !== ConnectionStatuses.CONNECTED) {
        existing.ready(error => {
          if (error) {
            callback(error, undefined as unknown as Connection)
            return
          }

          callback(null, existing)
        })
      } else {
        callback(null, existing)
      }

      return callback[kCallbackPromise]
    }

    const connection = new Connection(this.#clientId, this.#connectionOptions)
    this.#connections.set(key, connection)

    const eventPayload = { broker, connection }

    this.emit('connecting', eventPayload)

    connection.connect(broker.host, broker.port, error => {
      if (error) {
        this.#connections.delete(key)
        this.emit('failed', eventPayload)

        callback(error, undefined as unknown as Connection)
        return
      }

      this.emit('connect', eventPayload)
      callback(null, connection)
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

    return callback[kCallbackPromise]
  }

  getFirstAvailable (
    brokers: Broker[],
    callback?: CallbackWithPromise<Connection>,
    current: number = 0,
    errors: Error[] = []
  ): void | Promise<Connection> {
    if (!callback) {
      callback = createPromisifiedCallback<Connection>()
    }

    this.#getFirstAvailable(brokers, current, errors, callback)

    return callback[kCallbackPromise]
  }

  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (this.#connections.size === 0) {
      callback(null)
      return callback[kCallbackPromise]
    }

    runConcurrentCallbacks<void>(
      'Closing connections failed.',
      this.#connections,
      ([key, connection]: [string, Connection], cb: Callback<void>) => {
        connection.close(cb)
        this.#connections.delete(key)
      },
      error => callback(error)
    )

    return callback[kCallbackPromise]
  }

  #getFirstAvailable (
    brokers: Broker[],
    current: number = 0,
    errors: Error[] = [],
    callback: CallbackWithPromise<Connection>
  ): void {
    this.get(brokers[current], (error, connection) => {
      if (error) {
        errors.push(error)

        if (current === brokers.length - 1) {
          callback(new MultipleErrors('Cannot connect to any broker.', errors), undefined as unknown as Connection)
          return
        }

        this.#getFirstAvailable(brokers, current + 1, errors, callback)
        return
      }

      callback(null, connection)
    })
  }
}
