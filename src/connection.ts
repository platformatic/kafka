import BufferList from 'bl'
import EventEmitter, { once } from 'node:events'
import { createConnection, Socket } from 'node:net'
import { type ResponseParser, type SendCallback } from './apis/index.ts'
import { NetworkError, UnexpectedCorrelationIdError } from './errors.ts'
import { EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE, INT32_SIZE } from './protocol/definitions.ts'
import { Reader } from './protocol/reader.ts'
import { Writer } from './protocol/writer.ts'

export interface Request {
  correlationId: number
  apiKey: number
  apiVersion: number
  hasTaggedFields: boolean
  payload: () => Writer
}

export interface Response {
  apiKey: number
  apiVersion: number
  hasTaggedFields: boolean
  parser: ResponseParser<unknown>
  callback: SendCallback<any>
}

export class Connection extends EventEmitter {
  #clientId: string | undefined
  #correlationId: number
  #nextMessage: number
  #afterDrainRequests: Request[]
  #inflightRequests: Map<number, Response>
  #responseBuffer: BufferList
  #responseReader: Reader
  #socket!: Socket
  #socketMustBeDrained: boolean

  constructor (clientId?: string) {
    super()

    this.#clientId = clientId
    this.#correlationId = 0
    this.#nextMessage = 0
    this.#afterDrainRequests = []
    this.#inflightRequests = new Map()
    this.#responseBuffer = new BufferList()
    this.#responseReader = new Reader(this.#responseBuffer)
    this.#socketMustBeDrained = false
  }

  get socket (): Socket {
    return this.#socket
  }

  async start (host: string, port: number): Promise<void> {
    const { promise, resolve, reject } = Promise.withResolvers<void>()

    this.#socket = createConnection(port, host)

    this.#socket.on('connect', () => {
      this.#socket.removeListener('error', reject)
      this.#socket.on('error', this.#onError.bind(this))

      this.emit('connect')
      resolve()
    })

    this.#socket.on('error', (e: Error) => {
      this.emit('error', e)
      reject(e)
    })

    this.#socket.on('data', this.#onData.bind(this))
    this.#socket.on('drain', this.#onDrain.bind(this))
    this.#socket.on('close', this.#onClose.bind(this))

    return promise
  }

  async close (): Promise<void> {
    const promise = once(this.#socket, 'close')
    this.#socket.end()

    await promise
  }

  send<T>(
    apiKey: number,
    apiVersion: number,
    payload: () => Writer,
    responseParser: ResponseParser<T>,
    hasRequestHeaderTaggedFields: boolean,
    hasResponseHeaderTaggedFields: boolean,
    cb: SendCallback<T>
  ): boolean {
    const correlationId = ++this.#correlationId
    this.#inflightRequests.set(correlationId, {
      apiKey,
      apiVersion,
      hasTaggedFields: hasResponseHeaderTaggedFields,
      parser: responseParser,
      callback: cb
    })

    const request = {
      correlationId,
      apiKey,
      apiVersion,
      hasTaggedFields: hasRequestHeaderTaggedFields,
      payload
    }

    if (this.#socketMustBeDrained) {
      this.#afterDrainRequests.push(request)
      return false
    }

    return this.#sendRequest(request)
  }

  /*
    Request => Size [Request Header v2] [payload]
    Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER
      request_api_key => INT16
      request_api_version => INT16
      correlation_id => INT32
      client_id => NULLABLE_STRING
  */
  #sendRequest ({ correlationId, apiKey, apiVersion, payload: payloadFn, hasTaggedFields }: Request): boolean {
    let canWrite = true

    const writer = Writer.create()
      .appendInt16(apiKey)
      .appendInt16(apiVersion)
      .appendInt32(correlationId)
      .appendString(this.#clientId, false)

    if (hasTaggedFields) {
      writer.appendTaggedFields()
    }

    writer.append(payloadFn().bufferList)
    writer.prependLength()

    // Write the header
    this.#socket.cork()

    for (const buf of writer.buffers) {
      if (!this.#socket.write(buf)) {
        canWrite = false
      }
    }

    if (!canWrite) {
      this.#socketMustBeDrained = true
    }

    this.#socket.uncork()
    return canWrite
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
      const handler = this.#inflightRequests.get(correlationId)

      if (!handler) {
        this.emit(
          'error',
          new UnexpectedCorrelationIdError(`Received unexpected response with correlation_id=${correlationId}`, {
            raw: this.#responseReader.buffer.slice(0, this.#nextMessage + INT32_SIZE)
          })
        )

        return
      }

      const { apiKey, apiVersion, hasTaggedFields, parser, callback } = handler

      try {
        // Due to inconsistency in the wire protocol, the tag buffer in the header might have to be handled by the APIs
        // For example: https://github.com/apache/kafka/blob/84caaa6e9da06435411510a81fa321d4f99c351f/clients/src/main/resources/common/message/ApiVersionsResponse.json#L24
        if (hasTaggedFields) {
          this.#responseReader.skip(EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE)
        }

        const deserialized = parser(
          correlationId,
          apiKey,
          apiVersion,
          this.#responseReader.buffer.shallowSlice(this.#responseReader.position, this.#nextMessage + INT32_SIZE)
        )

        callback(null, deserialized)
      } catch (e) {
        callback(e)
      } finally {
        this.#responseBuffer.consume(this.#nextMessage + INT32_SIZE)
        this.#responseReader.position = 0
        this.#nextMessage = -1
      }
    }
  }

  #onDrain (): void {
    // First of all, send all the requests that were waiting for the socket to drain
    while (this.#afterDrainRequests.length) {
      const request = this.#afterDrainRequests.shift()!

      // If after sending the request the socket is blocked again, abort
      if (!this.#sendRequest(request)) {
        return
      }
    }

    // Start getting requests again
    this.#socketMustBeDrained = false
    this.emit('drain')
  }

  #onClose (): void {
    this.emit('close')

    const error = new NetworkError('Connection closed')
    for (const inflight of this.#inflightRequests.values()) {
      inflight.callback(error)
    }
  }

  #onError (error: Error): void {
    this.emit('error', new NetworkError('Connection error', { cause: error }))
  }
}
