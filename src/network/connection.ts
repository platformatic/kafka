import { DynamicBuffer } from '@platformatic/dynamic-buffer'
import fastq from 'fastq'
import { createConnection, type NetConnectOpts, type Socket } from 'node:net'
import { connect as createTLSConnection, type ConnectionOptions as TLSConnectionOptions } from 'node:tls'
import { type CallbackWithPromise, createPromisifiedCallback, kCallbackPromise } from '../apis/callbacks.ts'
import { type Callback, type ResponseParser } from '../apis/definitions.ts'
import { allowedSASLMechanisms, SASLMechanisms, type SASLMechanismValue } from '../apis/enumerations.ts'
import { saslAuthenticateV2, saslHandshakeV1 } from '../apis/index.ts'
import { type SaslAuthenticateResponse, type SASLAuthenticationAPI } from '../apis/security/sasl-authenticate-v2.ts'
import {
  connectionsApiChannel,
  connectionsConnectsChannel,
  createDiagnosticContext,
  type DiagnosticContext,
  notifyCreation
} from '../diagnostic.ts'
import {
  AuthenticationError,
  type MultipleErrors,
  NetworkError,
  type ProtocolError,
  TimeoutError,
  UnexpectedCorrelationIdError,
  UserError
} from '../errors.ts'
import { TypedEventEmitter, type TypedEvents } from '../events.ts'
import { protocolAPIsById } from '../protocol/apis.ts'
import { EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE, INT32_SIZE } from '../protocol/definitions.ts'
import { saslOAuthBearer, saslPlain, saslScramSha } from '../protocol/index.ts'
import { Reader } from '../protocol/reader.ts'
import { defaultCrypto, type ScramAlgorithm } from '../protocol/sasl/scram-sha.ts'
import { Writer } from '../protocol/writer.ts'
import { loggers } from '../utils.ts'

export interface ConnectionEvents extends TypedEvents {
  connecting: () => void
  timeout: (error: TimeoutError) => void
  error: (error: Error) => void
  connect: () => void
  ready: () => void
  close: () => void
  closing: () => void
  'sasl:handshake': (mechanisms: string[]) => void
  'sasl:authentication': (authBytes?: Buffer) => void
  'sasl:authentication:extended': (authBytes?: Buffer) => void
  drain: () => void
}

export type SASLCredentialProvider<T = string> = () => T | Promise<T>
export interface Broker {
  host: string
  port: number
}

export type SASLCustomAuthenticator = (
  mechanism: SASLMechanismValue,
  connection: Connection,
  authenticate: SASLAuthenticationAPI,
  usernameProvider: string | SASLCredentialProvider | undefined,
  passwordProvider: string | SASLCredentialProvider | undefined,
  tokenProvider: string | SASLCredentialProvider | undefined,
  callback: CallbackWithPromise<SaslAuthenticateResponse>
) => void

export interface SASLOptions {
  mechanism: SASLMechanismValue
  username?: string | SASLCredentialProvider
  password?: string | SASLCredentialProvider
  token?: string | SASLCredentialProvider
  oauthBearerExtensions?: Record<string, string> | SASLCredentialProvider<Record<string, string>>
  authenticate?: SASLCustomAuthenticator
  authBytesValidator?: (authBytes: Buffer, callback: CallbackWithPromise<Buffer>) => void
}

export interface ConnectionOptions {
  connectTimeout?: number
  requestTimeout?: number
  maxInflights?: number
  tls?: TLSConnectionOptions
  ssl?: TLSConnectionOptions // Alias for tls
  tlsServerName?: string | boolean
  sasl?: SASLOptions
  ownerId?: number
  handleBackPressure?: boolean
}

export interface Request {
  correlationId: number
  apiKey: number
  apiVersion: number
  hasResponseHeaderTaggedFields: boolean
  noResponse: boolean
  payload: Buffer
  parser: ResponseParser<unknown>
  callback: Callback<any>
  diagnostic: Record<string, unknown>
  timeoutHandle: NodeJS.Timeout | null
  timedOut: boolean
}

export const ConnectionStatuses = {
  NONE: 'none',
  CONNECTING: 'connecting',
  AUTHENTICATING: 'authenticating',
  REAUTHENTICATING: 'reauthenticating',
  CONNECTED: 'connected',
  CLOSED: 'closed',
  CLOSING: 'closing',
  ERROR: 'error'
} as const

export type ConnectionStatus = keyof typeof ConnectionStatuses
export type ConnectionStatusValue = (typeof ConnectionStatuses)[keyof typeof ConnectionStatuses]

export const defaultOptions = {
  connectTimeout: 5000,
  requestTimeout: 30000,
  maxInflights: 5
}

let currentInstance = 0

export class Connection extends TypedEventEmitter<ConnectionEvents> {
  #host: string | undefined
  #port: number | undefined
  #options: ConnectionOptions
  #status: ConnectionStatusValue
  #instanceId: number
  #clientId: string | undefined
  // @ts-ignore This is used just for debugging
  #ownerId: number | undefined
  #handleBackPressure: boolean
  #correlationId: number
  #nextMessage: number
  #afterDrainRequests: Request[]
  #requestsQueue: fastq.queue<(callback: CallbackWithPromise<any>) => void>
  #inflightRequests: Map<number, Request>
  #responseBuffer: DynamicBuffer
  #responseReader: Reader
  #socket!: Socket
  #socketMustBeDrained: boolean
  #reauthenticationTimeout!: NodeJS.Timeout

  constructor (clientId?: string, options: ConnectionOptions = {}) {
    super()
    this.setMaxListeners(0)

    this.#instanceId = currentInstance++
    this.#options = Object.assign({}, defaultOptions, options)
    this.#options.tls ??= this.#options.ssl
    this.#status = ConnectionStatuses.NONE
    this.#clientId = clientId
    this.#ownerId = options.ownerId
    this.#handleBackPressure = options.handleBackPressure ?? false
    this.#correlationId = 0
    this.#nextMessage = 0
    this.#afterDrainRequests = []
    this.#requestsQueue = fastq((op, cb) => op(cb), this.#options.maxInflights! ?? defaultOptions.maxInflights!)
    this.#inflightRequests = new Map()
    this.#responseBuffer = new DynamicBuffer()
    this.#responseReader = new Reader(this.#responseBuffer)
    this.#socketMustBeDrained = false

    notifyCreation('connection', this)
  }

  /* c8 ignore next 3 - Simple getter */
  get host (): string | undefined {
    return this.#status === ConnectionStatuses.CONNECTING || ConnectionStatuses.CONNECTED ? this.#host : undefined
  }

  /* c8 ignore next 3 - Simple getter */
  get port (): number | undefined {
    return this.#status === ConnectionStatuses.CONNECTING || ConnectionStatuses.CONNECTED ? this.#port : undefined
  }

  get instanceId (): number {
    return this.#instanceId
  }

  get status (): ConnectionStatusValue {
    return this.#status
  }

  get socket (): Socket {
    return this.#socket
  }

  isConnected (): boolean {
    return this.#status === ConnectionStatuses.CONNECTED
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

      const connectionOptions: Partial<NetConnectOpts & TLSConnectionOptions> = {
        timeout: this.#options.connectTimeout
      }

      if (this.#options.tlsServerName) {
        connectionOptions.servername =
          typeof this.#options.tlsServerName === 'string' ? this.#options.tlsServerName : host
      }

      const connectingSocketTimeoutHandler = () => {
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

      const connectingSocketErrorHandler = (error: Error) => {
        this.#onConnectionError(host, port, diagnosticContext, error)
      }

      this.emit('connecting')

      this.#host = host
      this.#port = port
      /* c8 ignore next 3 - TLS connection is not tested but we rely on Node.js tests */
      this.#socket = this.#options.tls
        ? createTLSConnection(port, host, { ...this.#options.tls, ...connectionOptions })
        : createConnection({ ...connectionOptions, port, host })
      this.#socket.setNoDelay(true)

      this.#socket.once(this.#options.tls ? 'secureConnect' : 'connect', () => {
        this.#socket.removeListener('timeout', connectingSocketTimeoutHandler)
        this.#socket.removeListener('error', connectingSocketErrorHandler)

        this.#socket.on('error', this.#onError.bind(this))
        this.#socket.on('data', this.#onData.bind(this))
        if (this.#handleBackPressure) {
          this.#socket.on('drain', this.#onDrain.bind(this))
        }
        this.#socket.on('close', this.#onClose.bind(this))

        this.#socket.setTimeout(0)

        if (this.#options.sasl) {
          this.#authenticate(host, port, diagnosticContext)
        } else {
          this.#onConnectionSucceed(diagnosticContext)
        }
      })

      this.#socket.once('timeout', connectingSocketTimeoutHandler)
      this.#socket.once('error', connectingSocketErrorHandler)
    } catch (error) {
      this.#status = ConnectionStatuses.ERROR

      diagnosticContext.error = error
      connectionsConnectsChannel.error.publish(diagnosticContext)

      throw error
    } finally {
      connectionsConnectsChannel.end.publish(diagnosticContext)
    }

    return callback[kCallbackPromise]
  }

  ready (callback: CallbackWithPromise<void>): void
  ready (): Promise<void>
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

  close (callback: CallbackWithPromise<void>): void
  close (): Promise<void>
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback()
    }

    clearInterval(this.#reauthenticationTimeout)

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

  send<ReturnType> (
    apiKey: number,
    apiVersion: number,
    createPayload: () => Writer,
    responseParser: ResponseParser<ReturnType>,
    hasRequestHeaderTaggedFields: boolean,
    hasResponseHeaderTaggedFields: boolean,
    callback: Callback<ReturnType>
  ) {
    const correlationId = ++this.#correlationId

    const diagnostic = createDiagnosticContext({
      connection: this,
      operation: 'send',
      apiKey,
      apiVersion,
      correlationId
    })

    const writer = Writer.create()
    writer.appendInt16(apiKey).appendInt16(apiVersion).appendInt32(correlationId).appendString(this.#clientId, false)

    if (hasRequestHeaderTaggedFields) {
      writer.appendTaggedFields()
    }

    let payload: Writer
    try {
      payload = createPayload()
    } catch (err) {
      diagnostic.error = err as Error
      connectionsApiChannel.error.publish(diagnostic)
      return callback(err)
    }

    writer.appendFrom(payload).prependLength()

    const request: Request = {
      correlationId,
      apiKey,
      apiVersion,
      parser: responseParser,
      payload: writer.buffer,
      callback: null as unknown as Callback<any>, // Will be set later
      hasResponseHeaderTaggedFields,
      noResponse: payload.context.noResponse ?? false,
      diagnostic,
      timeoutHandle: null,
      timedOut: false
    }

    this.#requestsQueue.push(
      fastQueueCallback => {
        request.callback = fastQueueCallback
        if (!request.noResponse) {
          request.timeoutHandle = setTimeout(this.#onRequestTimeout.bind(this, request), this.#options.requestTimeout)
        }

        if (this.#socketMustBeDrained) {
          this.#afterDrainRequests.push(request)
          return false
        }

        return this.#sendRequest(request)
      },
      this.#onResponse.bind(this, request, callback)
    )
  }

  reauthenticate (): void {
    if (!this.#options.sasl) {
      return
    }

    const host = this.#host!
    const port = this.#port!
    const diagnosticContext = createDiagnosticContext({ connection: this, operation: 'reauthenticate', host, port })

    this.#status = ConnectionStatuses.REAUTHENTICATING
    clearTimeout(this.#reauthenticationTimeout)
    this.#authenticate(host, port, diagnosticContext)
  }

  #onResponse (request: Request, callback: Callback<any>, error: Error | null, payload?: any): void {
    clearTimeout(request.timeoutHandle!)
    request.timeoutHandle = null
    callback(error, payload)
  }

  #onRequestTimeout (request: Request): void {
    request.timedOut = true
    request.callback(new TimeoutError('Request timed out'), null)
  }

  #authenticate (host: string, port: number, diagnosticContext: DiagnosticContext): void {
    if (this.#status === ConnectionStatuses.CONNECTING) {
      this.#status = ConnectionStatuses.AUTHENTICATING
    }

    const { mechanism, username, password, token, oauthBearerExtensions, authenticate } = this.#options.sasl!

    if (!allowedSASLMechanisms.includes(mechanism)) {
      this.#onConnectionError(
        host,
        port,
        diagnosticContext,
        new UserError(`SASL mechanism ${mechanism} not supported.`)
      )

      return
    }

    saslHandshakeV1.api(this, mechanism, (error, response) => {
      if (error) {
        this.#onConnectionError(
          host,
          port,
          diagnosticContext,
          new AuthenticationError('Cannot find a suitable SASL mechanism.', { cause: error })
        )
        return
      }

      this.emit('sasl:handshake', response!.mechanisms)
      const callback = this.#onSaslAuthenticate.bind(this, host, port, diagnosticContext)

      if (authenticate) {
        authenticate(mechanism, this, saslAuthenticateV2.api, username, password, token, callback)
      } else if (mechanism === SASLMechanisms.PLAIN) {
        saslPlain.authenticate(saslAuthenticateV2.api, this, username!, password!, callback)
      } else if (mechanism === SASLMechanisms.OAUTHBEARER) {
        saslOAuthBearer.authenticate(saslAuthenticateV2.api, this, token!, oauthBearerExtensions!, callback)
      } else if (mechanism === SASLMechanisms.GSSAPI) {
        callback(new UserError('No custom SASL/GSSAPI authenticator provided.'))
      } else {
        saslScramSha.authenticate(
          saslAuthenticateV2.api,
          this,
          mechanism.substring(6) as ScramAlgorithm,
          username!,
          password!,
          defaultCrypto,
          callback
        )
      }
    })
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
      if (
        this.#status !== ConnectionStatuses.CONNECTED &&
        this.#status !== ConnectionStatuses.AUTHENTICATING &&
        this.#status !== ConnectionStatuses.REAUTHENTICATING
      ) {
        request.callback(new NetworkError('Connection closed'), undefined)
        return false
      }

      if (!request.noResponse) {
        this.#inflightRequests.set(request.correlationId, request)
      }

      let canWrite = this.#socket.write(request.payload)

      if (!this.#handleBackPressure) {
        canWrite = true
      }

      if (!canWrite) {
        this.#socketMustBeDrained = true
      }

      if (request.noResponse) {
        request.callback(null, canWrite)
      }

      loggers.protocol('Sending request.', {
        apiKey: protocolAPIsById[request.apiKey],
        correlationId: request.correlationId,
        request
      })

      return canWrite
      /* c8 ignore next 8 - Hard to test */
    } catch (err) {
      request.diagnostic.error = err as Error
      connectionsApiChannel.error.publish(request.diagnostic)
      throw err
    } finally {
      connectionsApiChannel.end.publish(request.diagnostic)
    }
  }

  #onConnectionSucceed (diagnosticContext: DiagnosticContext): void {
    this.#status = ConnectionStatuses.CONNECTED

    connectionsConnectsChannel.asyncStart.publish(diagnosticContext)
    this.emit('connect')
    connectionsConnectsChannel.asyncEnd.publish(diagnosticContext)
  }

  #onConnectionError (host: string, port: number, diagnosticContext: DiagnosticContext, cause: Error): void {
    const error = new NetworkError(`Connection to ${host}:${port} failed.`, { cause })
    this.#status = ConnectionStatuses.ERROR
    clearTimeout(this.#reauthenticationTimeout)

    diagnosticContext.error = error
    connectionsConnectsChannel.error.publish(diagnosticContext)
    connectionsConnectsChannel.asyncStart.publish(diagnosticContext)
    this.emit('error', error)
    connectionsConnectsChannel.asyncEnd.publish(diagnosticContext)

    this.#socket.end()
  }

  #onSaslAuthenticate (
    host: string,
    port: number,
    diagnosticContext: DiagnosticContext,
    error: Error | null,
    response?: SaslAuthenticateResponse
  ): void {
    if (error) {
      const protocolError = (error as MultipleErrors).errors?.[0] as ProtocolError

      if (protocolError?.apiId === 'SASL_AUTHENTICATION_FAILED') {
        error = new AuthenticationError('SASL authentication failed.', { cause: error })
      }

      this.#onConnectionError(host, port, diagnosticContext, error)
      return
    }

    if (this.#options.sasl!.authBytesValidator) {
      this.#options.sasl!.authBytesValidator(
        response!.authBytes,
        this.#onSaslAuthenticationValidation.bind(this, host, port, diagnosticContext, response!.sessionLifetimeMs)
      )
    } else {
      this.#onSaslAuthenticationValidation(
        host,
        port,
        diagnosticContext,
        response!.sessionLifetimeMs,
        null,
        response!.authBytes
      )
    }
  }

  #onSaslAuthenticationValidation (
    host: string,
    port: number,
    diagnosticContext: DiagnosticContext,
    sessionLifetimeMs: bigint,
    error: Error | null,
    authBytes?: Buffer
  ): void {
    if (error) {
      this.#onConnectionError(
        host,
        port,
        diagnosticContext,
        new AuthenticationError('SASL authentication failed.', { cause: error })
      )
      return
    }

    if (sessionLifetimeMs > 0) {
      this.#reauthenticationTimeout = setTimeout(this.reauthenticate.bind(this), Number(sessionLifetimeMs) * 0.8)
    }

    if (this.#status === ConnectionStatuses.CONNECTED || this.#status === ConnectionStatuses.REAUTHENTICATING) {
      this.#status = ConnectionStatuses.CONNECTED
      this.emit('sasl:authentication:extended', authBytes)
    } else {
      this.emit('sasl:authentication', authBytes)
      this.#onConnectionSucceed(diagnosticContext)
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
        this.#socket.destroy(
          new UnexpectedCorrelationIdError(`Received unexpected response with correlation_id=${correlationId}`, {
            raw: this.#responseReader.buffer.slice(0, this.#nextMessage + INT32_SIZE)
          })
        )

        return
      }

      this.#inflightRequests.delete(correlationId)

      const { apiKey, apiVersion, hasResponseHeaderTaggedFields, parser, callback, timedOut } = request

      /* c8 ignore next 3 - Hard to test */
      if (timedOut) {
        return
      }

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

      loggers.protocol('Received response.', { apiKey: protocolAPIsById[apiKey], correlationId, request, deserialized })

      if (responseError) {
        request.diagnostic.error = responseError
        connectionsApiChannel.error.publish(request.diagnostic)
      } else {
        request.diagnostic.result = deserialized
      }

      connectionsApiChannel.asyncStart.publish(request.diagnostic)
      callback(responseError, deserialized)
      connectionsApiChannel.asyncEnd.publish(request.diagnostic)
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
      if (!request.noResponse) {
        request.callback(error, undefined)
      }
    }

    for (const inflight of this.#inflightRequests.values()) {
      inflight.callback(error, undefined)
    }
  }

  #onError (error: Error): void {
    clearTimeout(this.#reauthenticationTimeout)
    this.emit('error', new NetworkError('Connection error', { cause: error }))
  }
}
