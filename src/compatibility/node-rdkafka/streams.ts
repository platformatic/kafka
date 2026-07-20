import { Readable, type ReadableOptions, Writable, type WritableOptions } from 'node:stream'
import { KafkaConsumer } from './consumer.ts'
import { Producer } from './producer.ts'
import { type ConsumerGlobalConfig, type ConsumerTopicConfig, type Message, type Metadata, type ProducerGlobalConfig, type ProducerTopicConfig } from './types.ts'

export interface ReadStreamOptions extends ReadableOptions {
  topics: string | RegExp | (string | RegExp)[] | ((metadata: Metadata) => string | RegExp | (string | RegExp)[])
  autoClose?: boolean
  streamAsBatch?: boolean
  fetchSize?: number
  waitInterval?: number
  initOauthBearerToken?: string
  connectOptions?: { topic?: string; allTopics?: boolean; timeout?: number }
}

export interface WriteStreamOptions extends WritableOptions {
  topic?: string
  autoClose?: boolean
  encoding?: BufferEncoding
  pollInterval?: number
  connectOptions?: { topic?: string; allTopics?: boolean; timeout?: number }
}

export class ConsumerStream extends Readable {
  readonly consumer: KafkaConsumer
  readonly #options: ReadStreamOptions
  #started = false
  #inFlight = false
  #pending: Message[] = []
  #waitTimer: NodeJS.Timeout | null = null
  #disconnecting = false
  #disconnected = false
  #disconnectCallbacks: Array<(error: Error | null) => void> = []

  constructor (consumer: KafkaConsumer, options: ReadStreamOptions | number) {
    const normalized = typeof options === 'number' ? { topics: [] as string[], waitInterval: options } : options
    super({ ...normalized, objectMode: normalized.objectMode ?? true })
    this.consumer = consumer
    this.#options = normalized
    if (normalized.initOauthBearerToken) {
      consumer.setOauthBearerToken(normalized.initOauthBearerToken)
    }
    this.connect(normalized.connectOptions)
    consumer.on('event.error', error => this.destroy(error))
    consumer.on('unsubscribed', () => this.push(null))
  }

  connect (options: { topic?: string; allTopics?: boolean; timeout?: number } = {}): void {
    this.consumer.connect(options, (error, metadata) => {
      if (error || !metadata) {
        this.destroy(error ?? new Error('Metadata unavailable'))
        return
      }
      let topics = this.#options.topics
      if (typeof topics === 'function') {
        topics = topics(metadata)
      }
      this.consumer.subscribe(Array.isArray(topics) ? topics : [topics])
      this.#started = true
      this.#pull()
    })
  }

  refreshOauthBearerToken (token: string): void {
    this.consumer.setOauthBearerToken(token)
  }

  close (callback?: () => void): void {
    this.#started = false
    this.#clearWaitTimer()
    this.#disconnect(() => {
      this.push(null)
      this.destroy()
      callback?.()
    })
  }

  override _destroy (error: Error | null, callback: (error?: Error | null) => void): void {
    this.#started = false
    this.#clearWaitTimer()
    if (this.#options.autoClose === false) {
      callback(error)
      return
    }
    this.#disconnect(disconnectError => callback(error ?? disconnectError))
  }

  override _read (): void {
    this.#pull()
  }

  #pull (): void {
    while (this.#pending.length > 0) {
      const message = this.#pending.shift()!
      const chunk = this.#options.objectMode === false ? message.value ?? Buffer.alloc(0) : message
      if (!this.push(chunk)) {
        return
      }
    }
    if (!this.#started || this.#inFlight || this.#waitTimer || this.destroyed) {
      return
    }
    this.#inFlight = true
    this.consumer.consume(this.#options.fetchSize ?? 1, (error, messages) => {
      this.#inFlight = false
      if (!this.#started || this.destroyed) {
        return
      }
      if (error) {
        if (this.listenerCount('error') === 0) {
          this.destroy(error)
        } else {
          this.emit('error', error)
          if (this.#options.waitInterval && this.#options.waitInterval > 0) {
            this.#schedulePull(this.#options.waitInterval)
          }
        }
        return
      }
      if (this.#options.streamAsBatch) {
        if (messages && messages.length > 0 && !this.push(messages)) {
          return
        }
      } else {
        this.#pending.push(...messages ?? [])
      }
      if ((!messages || messages.length === 0) && this.#options.waitInterval && this.#options.waitInterval > 0) {
        this.#schedulePull(this.#options.waitInterval)
        return
      }
      this.#pull()
    })
  }

  #schedulePull (waitInterval: number): void {
    this.#waitTimer = setTimeout(() => {
      this.#waitTimer = null
      this.#pull()
    }, waitInterval)
    this.#waitTimer.unref()
  }

  #clearWaitTimer (): void {
    if (this.#waitTimer) {
      clearTimeout(this.#waitTimer)
      this.#waitTimer = null
    }
  }

  #disconnect (callback: (error: Error | null) => void): void {
    if (this.#disconnected) {
      queueMicrotask(() => callback(null))
      return
    }
    this.#disconnectCallbacks.push(callback)
    if (this.#disconnecting) {
      return
    }
    this.#disconnecting = true
    this.consumer.disconnect(error => {
      this.#disconnecting = false
      this.#disconnected = true
      const callbacks = this.#disconnectCallbacks.splice(0)
      for (const pendingCallback of callbacks) {
        pendingCallback(error ?? null)
      }
    })
  }
}

export class ProducerStream extends Writable {
  readonly producer: Producer
  readonly #options: WriteStreamOptions
  #explicitClose = false
  #disconnecting = false
  #disconnected = false
  #disconnectCallbacks: Array<(error: Error | null) => void> = []
  #retryTimer: NodeJS.Timeout | null = null
  #readyListener: (() => void) | null = null
  #writeCallback: ((error?: Error | null) => void) | null = null
  #closeEmitted = false
  #closeCallbacks: Array<() => void> = []

  constructor (producer: Producer, options: WriteStreamOptions) {
    if (options.objectMode !== true && !options.topic) {
      throw new TypeError('ProducerStreams not using objectMode must provide a topic to produce to.')
    }
    super(options)
    this.producer = producer
    this.#options = options
    this.producer.setPollInterval(options.pollInterval ?? 100)
    this.producer.once('connection.failure', error => this.destroy(error))
    this.once('close', () => {
      this.#closeEmitted = true
      const callbacks = this.#closeCallbacks.splice(0)
      for (const callback of callbacks) {
        callback()
      }
    })
    this.producer.connect(options.connectOptions)
  }

  connect (options: { topic?: string; allTopics?: boolean; timeout?: number } = {}): void {
    this.producer.connect(options)
  }

  close (callback?: () => void): void {
    this.#explicitClose = true
    if (callback) {
      if (this.#closeEmitted) {
        queueMicrotask(callback)
      } else {
        this.#closeCallbacks.push(callback)
      }
    }
    this.destroy()
  }

  override _destroy (error: Error | null, callback: (error?: Error | null) => void): void {
    this.#cancelWrite(error)
    if (!this.#explicitClose && this.#options.autoClose === false) {
      callback(error)
      return
    }
    this.#disconnect(disconnectError => callback(error ?? disconnectError))
  }

  override _write (chunk: Buffer | Message, _encoding: BufferEncoding, callback: (error?: Error | null) => void): void {
    this.#writeCallback = callback
    this.#write(chunk, _encoding)
  }

  #write (chunk: Buffer | Message, encoding: BufferEncoding): void {
    if (this.destroyed) {
      this.#cancelWrite(null)
      return
    }
    if (!this.producer.isConnected()) {
      this.#readyListener = () => {
        this.#readyListener = null
        this.#write(chunk, encoding)
      }
      this.producer.once('ready', this.#readyListener)
      return
    }
    try {
      if (Buffer.isBuffer(chunk)) {
        this.producer.produce(this.#options.topic!, null, chunk, null)
      } else {
        this.producer.produce(chunk.topic, chunk.partition, chunk.value, chunk.key, chunk.timestamp, chunk.opaque, chunk.headers)
      }
      this.#completeWrite()
    } catch (error) {
      if (isQueueFull(error)) {
        this.producer.poll()
        this.#retryTimer = setTimeout(() => {
          this.#retryTimer = null
          this.#write(chunk, encoding)
        }, 10)
        this.#retryTimer.unref()
        return
      }
      this.#completeWrite(error as Error)
    }
  }

  #completeWrite (error?: Error | null): void {
    const callback = this.#writeCallback
    this.#writeCallback = null
    callback?.(error)
  }

  #cancelWrite (error: Error | null): void {
    if (this.#retryTimer) {
      clearTimeout(this.#retryTimer)
      this.#retryTimer = null
    }
    if (this.#readyListener) {
      this.producer.removeListener('ready', this.#readyListener)
      this.#readyListener = null
    }
    this.#completeWrite(error)
  }

  #disconnect (callback: (error: Error | null) => void): void {
    if (this.#disconnected) {
      queueMicrotask(() => callback(null))
      return
    }
    this.#disconnectCallbacks.push(callback)
    if (this.#disconnecting) {
      return
    }
    this.#disconnecting = true
    this.producer.disconnect(error => {
      this.#disconnecting = false
      this.#disconnected = true
      const callbacks = this.#disconnectCallbacks.splice(0)
      for (const pendingCallback of callbacks) {
        pendingCallback(error ?? null)
      }
    })
  }
}

function isQueueFull (error: unknown): error is Error & { code: number } {
  return typeof error === 'object' && error !== null && (error as { code?: unknown }).code === -184
}

export function createReadStream (config: ConsumerGlobalConfig, topicConfig: ConsumerTopicConfig, options: ReadStreamOptions | number): ConsumerStream {
  return new ConsumerStream(new KafkaConsumer(config, topicConfig), options)
}

export function createWriteStream (config: ProducerGlobalConfig, topicConfig: ProducerTopicConfig, options: WriteStreamOptions): ProducerStream {
  return new ProducerStream(new Producer(config, topicConfig), options)
}
