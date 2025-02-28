import BufferList from 'bl'
import EventEmitter, { once } from 'node:events'
import { createConnection, Socket } from 'node:net'
import { type ResponseParser, type SendCallback } from './apis/index.ts'
import { KafkaError } from './error.ts'
import {
  EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE,
  INT16_SIZE,
  INT32_SIZE,
  sizeOfCollection
} from './protocol/definitions.ts'
import { Reader } from './protocol/reader.ts'
import { Writer } from './protocol/writer.ts'

export class Connection extends EventEmitter {
  #socket!: Socket
  #clientId: string | undefined
  #correlationId: number
  #pending: Map<number, [boolean, ResponseParser<unknown>, SendCallback<any>]>
  #responseBuffer: BufferList
  #responseReader: Reader
  #nextMessage: number

  constructor (clientId?: string) {
    super()

    this.#clientId = clientId
    this.#responseBuffer = new BufferList()
    this.#responseReader = new Reader(this.#responseBuffer)
    this.#nextMessage = 0
    this.#correlationId = 0
    this.#pending = new Map()
  }

  async start (host: string, port: number): Promise<void> {
    const { promise, resolve, reject } = Promise.withResolvers<void>()

    this.#socket = createConnection(port, host)

    this.#socket.on('data', this.#onData.bind(this))

    this.#socket.on('connect', () => {
      this.#socket.removeListener('error', reject)
      resolve()
    })

    this.#socket.on('error', reject)

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
    payload: BufferList,
    responseParser: ResponseParser<T>,
    hasResponseHeaderTaggedFields: boolean
  ): Promise<T> {
    const correlationId = ++this.#correlationId

    const { promise, resolve, reject } = Promise.withResolvers<T>()

    this.#pending.set(correlationId, [
      hasResponseHeaderTaggedFields,
      responseParser,
      function responseCallback (error: Error | null | undefined, payload?: T) {
        if (error) {
          reject(error)
        } else {
          resolve(payload as T)
        }
      }
    ])

    this.#sendRequest(correlationId, apiKey, apiVersion, payload)
    return promise
  }

  /*
    Response Header v1 => correlation_id TAG_BUFFER
      correlation_id => INT32
  */
  #onData (chunk: Buffer): void {
    this.#responseBuffer.append(chunk)

    if (this.#nextMessage < 1) {
      this.#nextMessage = this.#responseReader.peekInt32()
    }

    while (this.#responseBuffer.length - 4 >= this.#nextMessage) {
      this.#responseReader.skip(INT32_SIZE)

      // Read the correlationId
      const correlationId = this.#responseReader.readInt32()

      const handler = this.#pending.get(correlationId)

      if (!handler) {
        this.emit(
          'error',
          new KafkaError(`Received unexpected response with correlation_id=${correlationId}`, {
            raw: this.#responseBuffer.slice()
          })
        )

        return
      }

      const [hasResponseHeaderTaggedFields, parse, callback] = handler

      try {
        // Due to inconsistency in the wire protocol, the tag buffer in the header might have to be handled by the APIs
        // For example: https://github.com/apache/kafka/blob/84caaa6e9da06435411510a81fa321d4f99c351f/clients/src/main/resources/common/message/ApiVersionsResponse.json#L24
        if (hasResponseHeaderTaggedFields) {
          this.#responseReader.skip(EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE)
        }

        const deserialized = parse(
          this.#responseBuffer.shallowSlice(this.#responseReader.position, this.#nextMessage + 4)
        )

        callback(null, deserialized)
      } catch (e) {
        callback(e)
      } finally {
        this.#responseBuffer.consume(this.#nextMessage + 4)
        this.#responseReader.position = 0
        this.#nextMessage = -1
      }
    }
  }

  /*
    Request => Size [Request Header v2] [payload]
    Request Header v2 => request_api_key request_api_version correlation_id client_id TAG_BUFFER
      request_api_key => INT16
      request_api_version => INT16
      correlation_id => INT32
      client_id => NULLABLE_STRING

    Buffer allocations: 1
  */
  #sendRequest (correlationId: number, apiKey: number, apiVersion: number, payload: BufferList) {
    const writer = Writer.create(
      INT32_SIZE + // message_length
        INT16_SIZE + // request_api_key
        INT16_SIZE + // request_api_version
        INT32_SIZE + // correlation_id
        sizeOfCollection(this.#clientId) + // client_id
        EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE // tagged_fields
    )

    writer
      .writeInt32(writer.buffer.length - 4 + payload.length)
      .writeInt16(apiKey)
      .writeInt16(apiVersion)
      .writeInt32(correlationId)
      .writeString(this.#clientId)
      .writeTaggedFields()

    // Write the header
    this.#socket.write(writer.finalize())

    // Write the payload, if any
    if (payload.length) {
      // @ts-expect-error _bufs is not exposed
      for (const buf of payload._bufs) {
        this.#socket.write(buf)
      }
    }
  }
}
