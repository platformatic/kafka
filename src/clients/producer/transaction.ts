import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../../apis/callbacks.ts'
import { type Callback } from '../../apis/definitions.ts'
import { createDiagnosticContext, producerTransactionsChannel } from '../../diagnostic.ts'
import { UserError } from '../../errors.ts'
import { type Message, type MessageConsumerMetadata, type MessageRecord } from '../../protocol/records.ts'
import {
  kInstance,
  kTransaction,
  kTransactionAddOffsets,
  kTransactionAddPartitions,
  kTransactionCancel,
  kTransactionCommitOffset,
  kTransactionEnd,
  kTransactionFindCoordinator,
  kTransactionPrepare
} from '../../symbols.ts'
import { kOptions } from '../base/base.ts'
import { type ClusterMetadata } from '../base/types.ts'
import { type Consumer } from '../consumer/consumer.ts'
import { type Producer } from './producer.ts'
import { type ProduceOptions, type ProduceResult, type SendOptions } from './types.ts'

let currentInstance = 0

export class Transaction<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> {
  id: string
  #completed: boolean

  #producer: Producer<Key, Value, HeaderKey, HeaderValue>
  #partitions: Set<string>
  #consumerGroups: Set<string>;

  [kInstance]: number

  constructor (producer: Producer<Key, Value, HeaderKey, HeaderValue>) {
    this[kInstance] = currentInstance++
    this.id = producer[kOptions].transactionalId!
    this.#producer = producer
    this.#partitions = new Set()
    this.#consumerGroups = new Set()
    this.#completed = false
  }

  get completed (): boolean {
    return this.#completed
  }

  [kTransactionPrepare] (
    transactionalId: string | undefined,
    options: ProduceOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: Callback<Transaction<Key, Value, HeaderKey, HeaderValue>>
  ): void {
    this.#producer[kTransactionFindCoordinator](error => {
      if (error) {
        callback(error, null as unknown as Transaction<Key, Value, HeaderKey, HeaderValue>)
        return
      }

      // If the idempotent producer was already initialized for this transactional id, nothing else is required
      if (transactionalId === this.id) {
        callback(null, this)
        return
      }

      this.#producer.initIdempotentProducer(options, error => {
        if (error) {
          callback(error, null as unknown as Transaction<Key, Value, HeaderKey, HeaderValue>)
          return
        }

        callback(null, this)
      })
    })
  }

  send (options: SendOptions<Key, Value, HeaderKey, HeaderValue>, callback: CallbackWithPromise<ProduceResult>): void
  send (options: SendOptions<Key, Value, HeaderKey, HeaderValue>): Promise<ProduceResult>
  send (
    options: SendOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<ProduceResult>
  ): void | Promise<ProduceResult> {
    if (!callback) {
      callback = createPromisifiedCallback<ProduceResult>()
    }

    if (this.#completed) {
      callback(
        new UserError('Cannot produce to an already completed transaction.'),
        undefined as unknown as ProduceResult
      )
      return callback[kCallbackPromise]
    }

    this.#producer.send({ ...options, [kTransaction]: this[kInstance] }, callback)
    return callback[kCallbackPromise]
  }

  addPartitions (metadata: ClusterMetadata, messages: MessageRecord[], callback: CallbackWithPromise<void>): void
  addPartitions (metadata: ClusterMetadata, messages: MessageRecord[]): Promise<void>
  addPartitions (
    metadata: ClusterMetadata,
    messages: MessageRecord[],
    callback?: CallbackWithPromise<void>
  ): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    // Scan all messages to find the partitions we need to add to the transaction
    const newAdditions = new Set<string>()
    const topicPartitions = new Map<string, Set<number>>()
    for (const message of messages) {
      message.partition! %= metadata.topics.get(message.topic)!.partitionsCount

      const { topic, partition } = message
      const key = `${topic}:${partition}`

      if (!this.#partitions.has(key)) {
        newAdditions.add(key)

        let partitionsSet = topicPartitions.get(topic)
        if (!partitionsSet) {
          partitionsSet = new Set<number>()
          topicPartitions.set(topic, partitionsSet)
        }

        partitionsSet.add(partition!)
      }
    }

    // Nothing to add
    if (topicPartitions.size === 0) {
      callback(null)
      return
    }

    if (this.#completed) {
      callback(new UserError('Cannot add partitions to an already completed transaction.'))
      return callback[kCallbackPromise]
    }

    producerTransactionsChannel.traceCallback(
      this.#producer[kTransactionAddPartitions],
      2,
      createDiagnosticContext({ client: this.#producer, transaction: this, operation: 'addPartitions' }),
      this.#producer,
      this[kInstance],
      topicPartitions,
      error => {
        if (!error) {
          for (const key of newAdditions) {
            this.#partitions.add(key)
          }
        }

        callback(error)
      }
    )

    return callback[kCallbackPromise]
  }

  addConsumer<MessageKey = Buffer, MessageValue = Buffer, MessageHeaderKey = Buffer, MessageHeaderValue = Buffer> (
    consumer: Consumer<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>,
    callback: CallbackWithPromise<void>
  ): void
  addConsumer<MessageKey = Buffer, MessageValue = Buffer, MessageHeaderKey = Buffer, MessageHeaderValue = Buffer> (
    consumer: Consumer<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>
  ): Promise<void>
  addConsumer<MessageKey = Buffer, MessageValue = Buffer, MessageHeaderKey = Buffer, MessageHeaderValue = Buffer> (
    consumer: Consumer<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>,
    callback?: CallbackWithPromise<void>
  ): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    const groupId = consumer.groupId

    if (this.#consumerGroups.has(groupId)) {
      callback(null)
      return
    }

    if (this.#completed) {
      callback(new UserError('Cannot add a consumer to an already completed transaction.'))
      return callback[kCallbackPromise]
    }

    producerTransactionsChannel.traceCallback(
      this.#producer[kTransactionAddOffsets],
      2,
      createDiagnosticContext({
        client: this.#producer,
        transaction: this,
        operation: 'addOffsetsToTransaction',
        groupId
      }),
      this.#producer,
      this[kInstance],
      groupId,
      error => {
        if (!error) {
          this.#consumerGroups.add(groupId)
        }

        callback(error)
      }
    )

    return callback[kCallbackPromise]
  }

  addOffset<MessageKey = Buffer, MessageValue = Buffer, MessageHeaderKey = Buffer, MessageHeaderValue = Buffer> (
    message: Message<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>,
    callback: CallbackWithPromise<void>
  ): void
  addOffset<MessageKey = Buffer, MessageValue = Buffer, MessageHeaderKey = Buffer, MessageHeaderValue = Buffer> (
    message: Message<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>
  ): Promise<void>
  addOffset<MessageKey = Buffer, MessageValue = Buffer, MessageHeaderKey = Buffer, MessageHeaderValue = Buffer> (
    message: Message<MessageKey, MessageValue, MessageHeaderKey, MessageHeaderValue>,
    callback?: CallbackWithPromise<void>
  ): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this.#completed) {
      callback(new UserError('Cannot add an offset to an already completed transaction.'))
      return callback[kCallbackPromise]
    }

    if (!this.#consumerGroups.has((message.metadata?.consumer as MessageConsumerMetadata)?.groupId)) {
      callback(
        new UserError(
          'Cannot add an offset to a transaction when the consumer group of the message has not been added to the transaction.'
        )
      )
      return callback[kCallbackPromise]
    }

    producerTransactionsChannel.traceCallback(
      this.#producer[kTransactionCommitOffset],
      2,
      createDiagnosticContext({ client: this.#producer, transaction: this, operation: 'commitOffset', message }),
      this.#producer,
      this[kInstance],
      message,
      callback
    )

    return callback[kCallbackPromise]
  }

  commit (callback: CallbackWithPromise<void>): void
  commit (): Promise<void>
  commit (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this.#completed) {
      callback(new UserError('Cannot commit an already completed transaction.'))
      return callback[kCallbackPromise]
    }

    producerTransactionsChannel.traceCallback(
      this.#producer[kTransactionEnd],
      2,
      createDiagnosticContext({ client: this.#producer, transaction: this, operation: 'commit' }),
      this.#producer,
      this[kInstance],
      true,
      this.#updateCompletionCallback.bind(this, callback)
    )

    return callback[kCallbackPromise]
  }

  abort (callback: CallbackWithPromise<void>): void
  abort (): Promise<void>
  abort (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this.#completed) {
      callback(new UserError('Cannot abort an already completed transaction.'))
      return callback[kCallbackPromise]
    }

    producerTransactionsChannel.traceCallback(
      this.#producer[kTransactionEnd],
      2,
      createDiagnosticContext({ client: this.#producer, transaction: this, operation: 'abort' }),
      this.#producer,
      this[kInstance],
      false,
      this.#updateCompletionCallback.bind(this, callback)
    )

    return callback[kCallbackPromise]
  }

  cancel (callback: CallbackWithPromise<void>): void
  cancel (): Promise<void>
  cancel (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    this.#producer[kTransactionCancel](this[kInstance], this.#updateCompletionCallback.bind(this, callback))

    return callback[kCallbackPromise]
  }

  #updateCompletionCallback (callback: CallbackWithPromise<void>, error: Error | null): void {
    if (!error) {
      this.#completed = true
    }

    callback(error)
  }
}
