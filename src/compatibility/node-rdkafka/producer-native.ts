import { randomUUID } from 'node:crypto'
import { Consumer } from '../../clients/consumer/consumer.ts'
import type { ConsumerOptions, ListOffsetsOptions, OffsetsWithTimestamps } from '../../clients/consumer/types.ts'
import { kGetTransactionalIdForInitialization, Producer } from '../../clients/producer/producer.ts'
import type { ProducerOptions } from '../../clients/producer/types.ts'
import type { CallbackWithPromise } from '../../apis/callbacks.ts'

export class NodeRdkafkaProducerBridge extends Producer<Buffer | string | null, Buffer | null, string, Buffer | string> {
  readonly #transactionalId: string | undefined
  readonly #offsetRequesterOptions: ConsumerOptions<Buffer, Buffer | null, Buffer, Buffer>

  constructor (
    options: ProducerOptions<Buffer | string | null, Buffer | null, string, Buffer | string>,
    offsetRequesterOptions: Omit<ConsumerOptions<Buffer, Buffer | null, Buffer, Buffer>, 'groupId'>
  ) {
    super(options)
    this.#transactionalId = options.transactionalId
    this.#offsetRequesterOptions = { ...offsetRequesterOptions, groupId: `node-rdkafka-offsets-${randomUUID()}` }
  }

  [kGetTransactionalIdForInitialization] (): string | undefined {
    return this.#transactionalId ?? super[kGetTransactionalIdForInitialization]()
  }

  listOffsetsWithTimestamps (options: ListOffsetsOptions, callback: CallbackWithPromise<OffsetsWithTimestamps>): void {
    const requester = new Consumer(this.#offsetRequesterOptions)
    requester.listOffsetsWithTimestamps(options, (error, offsets) => {
      requester.close(closeError => callback(error ?? closeError, offsets))
    })
  }
}
