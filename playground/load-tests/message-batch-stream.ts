import { Duplex } from 'node:stream'

type CallbackFunction = (error?: Error | null) => void
type MessageWithTopicAndPartition = { topic: string; partition: number }

export interface MessageBatchOptions {
  /** Maximum number of messages to accumulate before flushing */
  batchSize: number
  /** Time in milliseconds to wait before flushing incomplete batches */
  timeoutMilliseconds: number
}

/**
 * Replicates MQT's KafkaMessageBatchStream: a Duplex stream that batches Kafka messages
 * based on size and timeout constraints.
 *
 * - Accumulates messages across all partitions up to batchSize
 * - Groups messages by topic:partition when flushing
 * - Implements backpressure
 * - Auto-flushes on timeout for partial batches
 */
export class MessageBatchStream<TMessage extends MessageWithTopicAndPartition> extends Duplex {
  private readonly batchSize: number
  private readonly timeout: number

  private messages: TMessage[]
  private existingTimeout: NodeJS.Timeout | undefined
  private pendingBatches: TMessage[][]

  constructor (options: MessageBatchOptions) {
    super({ objectMode: true })
    this.batchSize = options.batchSize
    this.timeout = options.timeoutMilliseconds
    this.messages = []
    this.pendingBatches = []
  }

  override _read (): void {
    this.flushPendingBatches()
  }

  override _write (message: TMessage, _encoding: BufferEncoding, callback: CallbackFunction): void {
    try {
      this.messages.push(message)

      if (this.messages.length >= this.batchSize) {
        this.flushMessages()
      } else {
        this.existingTimeout ??= setTimeout(() => this.flushMessages(), this.timeout)
      }
    } finally {
      callback()
    }
  }

  override _final (callback: CallbackFunction): void {
    clearTimeout(this.existingTimeout)
    this.existingTimeout = undefined
    // Remaining messages are not committed, next consumer will process them
    this.messages = []
    this.pendingBatches = []
    this.push(null)
    callback()
  }

  private flushMessages (): void {
    clearTimeout(this.existingTimeout)
    this.existingTimeout = undefined

    if (this.messages.length === 0) return

    const messageBatch = this.messages.splice(0, this.messages.length)

    // Group by topic:partition
    const messagesByTopicPartition: Record<string, TMessage[]> = {}
    for (const message of messageBatch) {
      const key = `${message.topic}:${message.partition}`
      if (!messagesByTopicPartition[key]) messagesByTopicPartition[key] = []
      messagesByTopicPartition[key].push(message)
    }

    for (const messagesForKey of Object.values(messagesByTopicPartition)) {
      this.pendingBatches.push(messagesForKey)
    }

    this.flushPendingBatches()
  }

  private flushPendingBatches (): void {
    while (this.pendingBatches.length > 0) {
      const canContinue = this.push(this.pendingBatches[0]!)
      if (!canContinue) return

      this.pendingBatches.shift()
    }
  }
}
