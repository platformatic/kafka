import fastq from 'fastq'
import EventEmitter from 'node:events'
import { createConnection, type NetConnectOpts, type Socket } from 'node:net'
import { connect as createTLSConnection, type ConnectionOptions as TLSConnectionOptions } from 'node:tls'
import { type Callback, type ResponseParser } from '../apis/definitions.ts'
import { type CallbackWithPromise, createPromisifiedCallback, kCallbackPromise } from '../clients/callbacks.ts'
import { connectionsApiChannel, connectionsConnectsChannel, createDiagnosticContext, notifyCreation } from '../diagnostic.ts'
import { NetworkError, TimeoutError, UnexpectedCorrelationIdError } from '../errors.ts'
import { protocolAPIsById } from '../protocol/apis.ts'
import { EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE, INT32_SIZE } from '../protocol/definitions.ts'
import { DynamicBuffer } from '../protocol/dynamic-buffer.ts'
import { Reader } from '../protocol/reader.ts'
import { Writer } from '../protocol/writer.ts'
import { loggers } from '../utils.ts'

export interface Broker {
  host: string
  port: number
}

export interface ConnectionOptions {
  connectTimeout?: number
  maxInflights?: number
  tls?: TLSConnectionOptions
  ownerId?: number
}

export interface Request {
  correlationId: number
  apiKey: number
  apiVersion: number
  hasRequestHeaderTaggedFields: boolean
  hasResponseHeaderTaggedFields: boolean
  payload: () => Writer
  parser: ResponseParser<unknown>
  callback: Callback<any>
  diagnostic: Record<string, unknown>
}

export const ConnectionStatuses = {
  NONE: 'none',
  CONNECTING: 'connecting',
  CONNECTED: 'connected',
  CLOSED: 'closed',
  CLOSING: 'closing',
  ERROR: 'error'
} as const

export type ConnectionStatus = keyof typeof ConnectionStatuses
export type ConnectionStatusValue = (typeof ConnectionStatuses)[keyof typeof ConnectionStatuses]

export const defaultOptions: ConnectionOptions = {
  connectTimeout: 5000,
  maxInflights: 5
}

let currentInstance = 0
const kNoResponse = Symbol('plt.kafka.noResponse')

/* c8 ignore next */
export function noResponseCallback (..._: any[]): void {}
noResponseCallback[kNoResponse] = true

export class Connection extends EventEmitter {
  #options: ConnectionOptions
  #status: ConnectionStatusValue
  #instanceId: number
  #clientId: string | undefined
  // @ts-ignore This is used just for debugging
  #ownerId: number | undefined
  #correlationId: number
  #nextMessage: number
  #afterDrainRequests: Request[]
  #requestsQueue: fastq.queue<(callback: CallbackWithPromise<any>) => void>
  #inflightRequests: Map<number, Request>
  #responseBuffer: DynamicBuffer
  #responseReader: Reader
  #socket!: Socket
  #socketMustBeDrained: boolean

  constructor (clientId?: string, options: ConnectionOptions = {}) {
    super()
    this.setMaxListeners(0)

    this.#instanceId = currentInstance++
    this.#options = Object.assign({}, defaultOptions, options)
    this.#status = ConnectionStatuses.NONE
    this.#clientId = clientId
    this.#ownerId = options.ownerId
    this.#correlationId = 0
    this.#nextMessage = 0
    this.#afterDrainRequests = []
    this.#requestsQueue = fastq((op, cb) => op(cb), this.#options.maxInflights!)
    this.#inflightRequests = new Map()
    this.#responseBuffer = new DynamicBuffer()
    this.#responseReader = new Reader(this.#responseBuffer)
    this.#socketMustBeDrained = false

    notifyCreation('connection', this)
  }

  /* c8 ignore next 3 */
  get instanceId (): number {
    return this.#instanceId
  }

  get status (): ConnectionStatusValue {
    return this.#status
  }

  get socket (): Socket {
    return this.#socket
  }

  connect (host: string, port: number, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    const diagnosticContext = createDiagnosticContext({ connection: this, operation: 'connect', host, port })

    connectionsConnectsChannel.start.publish(diagnosticContext)

    try {
      if (this.#status === ConnectionStatuses.CONNECTED) {
        callback(null)
        return callback[kCallbackPromise]
      }

      this.ready(callback)

      if (this.#status === ConnectionStatuses.CONNECTING) {
        return callback[kCallbackPromise]
      }

      this.#status = ConnectionStatuses.CONNECTING

      const connectionOptions: Partial<NetConnectOpts> = {
        timeout: this.#options.connectTimeout
      }

      const connectionTimeoutHandler = () => {
        const error = new TimeoutError(`Connection to ${host}:${port} timed out.`)
        diagnosticContext.error = error
        this.#socket.destroy()

        this.#status = ConnectionStatuses.ERROR

        connectionsConnectsChannel.error.publish(diagnosticContext)
        connectionsConnectsChannel.asyncStart.publish(diagnosticContext)
        this.emit('timeout', error)
        this.emit('error', error)
        connectionsConnectsChannel.asyncEnd.publish(diagnosticContext)
      }

      const connectionErrorHandler = (e: Error) => {
        const error = new NetworkError(`Connection to ${host}:${port} failed.`, { cause: e })
        diagnosticContext.error = error

        this.#status = ConnectionStatuses.ERROR

        connectionsConnectsChannel.error.publish(diagnosticContext)
        connectionsConnectsChannel.asyncStart.publish(diagnosticContext)
        this.emit('error', error)
        connectionsConnectsChannel.asyncEnd.publish(diagnosticContext)
      }

      this.emit('connecting')
      /* c8 ignore next 3 */
      this.#socket = this.#options.tls
        ? createTLSConnection(port, host, { ...this.#options.tls, ...connectionOptions })
        : createConnection({ ...connectionOptions, port, host })
      this.#socket.setNoDelay(true)

      this.#socket.once('connect', () => {
        this.#socket.removeListener('timeout', connectionTimeoutHandler)
        this.#socket.removeListener('error', connectionErrorHandler)

        this.#socket.on('error', this.#onError.bind(this))
        this.#socket.on('data', this.#onData.bind(this))
        this.#socket.on('drain', this.#onDrain.bind(this))
        this.#socket.on('close', this.#onClose.bind(this))

        this.#socket.setTimeout(0)

        this.#status = ConnectionStatuses.CONNECTED

        connectionsConnectsChannel.asyncStart.publish(diagnosticContext)
        this.emit('connect')
        connectionsConnectsChannel.asyncEnd.publish(diagnosticContext)
      })

      this.#socket.once('timeout', connectionTimeoutHandler)
      this.#socket.once('error', connectionErrorHandler)
      /* c8 ignore next 5 */
    } catch (error) {
      connectionsConnectsChannel.error.publish({ ...diagnosticContext, error })

      throw error
    } finally {
      connectionsConnectsChannel.end.publish(diagnosticContext)
    }

    return callback[kCallbackPromise]
  }

  ready (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    const onConnect = () => {
      this.removeListener('error', onError)

      callback!(null)
    }

    const onError = (error: Error) => {
      this.removeListener('connect', onConnect)

      callback!(error)
    }

    this.once('connect', onConnect)
    this.once('error', onError)
    this.emit('ready')

    return callback[kCallbackPromise]
  }

  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    if (
      this.#status === ConnectionStatuses.CLOSED ||
      this.#status === ConnectionStatuses.ERROR ||
      this.#status === ConnectionStatuses.NONE
    ) {
      callback(null)
      return callback[kCallbackPromise]
    } else if (this.#status === ConnectionStatuses.CLOSING) {
      this.once('close', () => {
        callback(null)
      })

      return callback[kCallbackPromise]
    }

    // Ignore all disconnection errors
    this.#socket.removeAllListeners('error')
    this.#socket.once('error', () => {})

    this.#socket.once('close', () => {
      this.#status = ConnectionStatuses.CLOSED
      this.emit('close')
      callback(null)
    })

    this.#status = ConnectionStatuses.CLOSING
    this.emit('closing')
    this.#socket.end()

    return callback[kCallbackPromise]
  }

  send<ReturnType>(
    apiKey: number,
    apiVersion: number,
    payload: () => Writer,
    responseParser: ResponseParser<ReturnType>,
    hasRequestHeaderTaggedFields: boolean,
    hasResponseHeaderTaggedFields: boolean,
    callback: Callback<ReturnType>
  ) {
    this.#requestsQueue.push(fastQueueCallback => {
      const correlationId = ++this.#correlationId

      const request: Request = {
        correlationId,
        apiKey,
        apiVersion,
        hasRequestHeaderTaggedFields,
        hasResponseHeaderTaggedFields,
        parser: responseParser,
        payload,
        callback: fastQueueCallback,
        diagnostic: createDiagnosticContext({
          connection: this,
          operation: 'send',
          apiKey,
          apiVersion,
          correlationId
        })
      }

      if (this.#socketMustBeDrained) {
        this.#afterDrainRequests.push(request)
        return false
      }

      return this.#sendRequest(request)
    }, callback)
  }

  /*
    Request => Size [Request Header v2] [payload]
    Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER
      request_api_key => INT16
      request_api_version => INT16
      correlation_id => INT32
      client_id => NULLABLE_STRING
  */
  #sendRequest (request: Request): boolean {
    connectionsApiChannel.start.publish(request.diagnostic)

    try {
      if (this.#status !== ConnectionStatuses.CONNECTED) {
        request.callback(new NetworkError('Connection closed'), undefined)
        return false
      }

      let canWrite = true

      const { correlationId, apiKey, apiVersion, payload: payloadFn, hasRequestHeaderTaggedFields } = request

      const writer = Writer.create()
        .appendInt16(apiKey)
        .appendInt16(apiVersion)
        .appendInt32(correlationId)
        .appendString(this.#clientId, false)

      if (hasRequestHeaderTaggedFields) {
        writer.appendTaggedFields()
      }

      const payload = payloadFn()
      writer.appendFrom(payload)
      writer.prependLength()

      // Write the header
      this.#socket.cork()

      if (!payload.context.noResponse) {
        this.#inflightRequests.set(correlationId, request)
      }

      /* c8 ignore next */
      loggers.protocol({ apiKey: protocolAPIsById[apiKey], correlationId, request }, 'Sending request.')

      for (const buf of writer.buffers) {
        if (!this.#socket.write(buf)) {
          canWrite = false
        }
      }

      if (!canWrite) {
        this.#socketMustBeDrained = true
      }

      this.#socket.uncork()

      if (payload.context.noResponse) {
        request.callback(null, canWrite)
      }

      // debugDump(Date.now() % 100000, 'send', { owner: this.#ownerId, apiKey: protocolAPIsById[apiKey], correlationId })

      return canWrite
      /* c8 ignore next 5 */
    } catch (error) {
      connectionsApiChannel.error.publish({ ...request.diagnostic, error })
      connectionsApiChannel.end.publish(request.diagnostic)
      throw error
    } finally {
      connectionsApiChannel.end.publish(request.diagnostic)
    }
  }

  /*
    Response Header v1 => correlation_id TAG_BUFFER
      correlation_id => INT32
  */
  #onData (chunk: Buffer): void {
    this.#responseBuffer.append(chunk)

    // There is at least one message size to add
    // Note that here the initial position is always 0
    while (this.#responseBuffer.length > INT32_SIZE) {
      if (this.#nextMessage < 1) {
        this.#nextMessage = this.#responseReader.readInt32()
      }

      // Less data than the message size, wait for more data
      if (this.#nextMessage > this.#responseBuffer.length - INT32_SIZE) {
        break
      }

      // Read the correlationId and get the handler
      const correlationId = this.#responseReader.readInt32()
      const request = this.#inflightRequests.get(correlationId)

      if (!request) {
        this.emit(
          'error',
          new UnexpectedCorrelationIdError(`Received unexpected response with correlation_id=${correlationId}`, {
            raw: this.#responseReader.buffer.slice(0, this.#nextMessage + INT32_SIZE)
          })
        )

        return
      }

      this.#inflightRequests.delete(correlationId)

      const { apiKey, apiVersion, hasResponseHeaderTaggedFields, parser, callback } = request

      let deserialized: any
      let responseError: Error | null = null

      try {
        // Due to inconsistency in the wire protocol, the tag buffer in the header might have to be handled by the APIs
        // For example: https://github.com/apache/kafka/blob/84caaa6e9da06435411510a81fa321d4f99c351f/clients/src/main/resources/common/message/ApiVersionsResponse.json#L24
        if (hasResponseHeaderTaggedFields) {
          this.#responseReader.skip(EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE)
        }

        deserialized = parser(
          correlationId,
          apiKey,
          apiVersion,
          new Reader(
            this.#responseReader.buffer.subarray(this.#responseReader.position, this.#nextMessage + INT32_SIZE)
          )
        )
      } catch (error) {
        responseError = error

        // debugDump(Date.now() % 100000, 'received error', {
        //   owner: this.#ownerId,
        //   apiKey: protocolAPIsById[apiKey],
        //   error
        // })
      } finally {
        this.#responseBuffer.consume(this.#nextMessage + INT32_SIZE)
        this.#responseReader.position = 0
        this.#nextMessage = -1
      }

      // debugDump(Date.now() % 100000, 'receive', {
      //   owner: this.#ownerId,
      //   apiKey: protocolAPIsById[apiKey],
      //   correlationId
      // })

      /* c8 ignore next */
      loggers.protocol({ apiKey: protocolAPIsById[apiKey], correlationId, request }, 'Received response.')

      if (responseError) {
        request.diagnostic.error = responseError
        connectionsApiChannel.error.publish(request.diagnostic)
      } else {
        request.diagnostic.result = deserialized
      }

      connectionsApiChannel.asyncStart.publish(request.diagnostic)
      callback(responseError, deserialized)
      connectionsApiChannel.asyncStart.publish(request.diagnostic)
    }
  }

  #onDrain (): void {
    // First of all, send all the requests that were waiting for the socket to drain
    while (this.#afterDrainRequests.length) {
      const request = this.#afterDrainRequests.shift()

      // If no more request or  after sending the request the socket is blocked again, abort
      if (!request || !this.#sendRequest(request)) {
        return
      }
    }

    // Start getting requests again
    this.#socketMustBeDrained = false
    this.emit('drain')
  }

  #onClose (): void {
    this.#status = ConnectionStatuses.CLOSED
    this.emit('close')

    const error = new NetworkError('Connection closed')

    for (const request of this.#afterDrainRequests) {
      const payload = request.payload()

      if (!payload.context.noResponse) {
        request.callback(error, undefined)
      }
    }

    for (const inflight of this.#inflightRequests.values()) {
      inflight.callback(error, undefined)
    }
  }

  #onError (error: Error): void {
    this.emit('error', new NetworkError('Connection error', { cause: error }))
  }
}
