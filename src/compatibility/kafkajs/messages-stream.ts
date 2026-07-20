import type { FetchResponse, FetchResponsePartition } from '../../apis/consumer/fetch-v17.ts'
import type { Callback } from '../../apis/definitions.ts'
import { ListOffsetTimestamps } from '../../apis/enumerations.ts'
import {
  kDecorateMessage,
  kHandleFetchPartitionProgress,
  kHandleOffsetOutOfRange,
  MessagesStream
} from '../../clients/consumer/messages-stream.ts'
import {
  MessagesStreamFallbackModes,
  type ConsumeOptions,
  type MessagesStreamFallbackModeValue
} from '../../clients/consumer/types.ts'
import type { Consumer } from '../../clients/consumer/consumer.ts'
import type { GenericError } from '../../errors.ts'
import { protocolErrors, UserError } from '../../errors.ts'
import type { Message } from '../../protocol/records.ts'
import { partitionKey } from '../../clients/consumer/utils.ts'
import {
  kKafkaJSFetchBatch,
  kKafkaJSFetchProgress,
  type KafkaJSFallbackModes,
  type KafkaJSFetchBatchMetadata,
  type KafkaJSFetchProgress
} from './symbols.ts'

export type KafkaJSNativeMessage = Message<Buffer, Buffer, Buffer, Buffer> & {
  [kKafkaJSFetchBatch]?: KafkaJSFetchBatchMetadata
}

export type KafkaJSFetchProgressMarker = {
  [kKafkaJSFetchProgress]: KafkaJSFetchProgress
}

export class KafkaJSMessagesStream extends MessagesStream<Buffer, Buffer, Buffer, Buffer> {
  readonly #fallbackModes: KafkaJSFallbackModes
  readonly #fetchBatches = new WeakMap<FetchResponsePartition, KafkaJSFetchBatchMetadata>()

  constructor (
    consumer: Consumer<Buffer, Buffer, Buffer, Buffer>,
    options: ConsumeOptions<Buffer, Buffer, Buffer, Buffer>,
    fallbackModes: KafkaJSFallbackModes
  ) {
    super(consumer, options)
    this.#fallbackModes = fallbackModes
  }

  [kDecorateMessage] (
    message: KafkaJSNativeMessage,
    partitionResponse: FetchResponsePartition,
    seekVersion: number,
    membershipVersion: number
  ): void {
    let batch = this.#fetchBatches.get(partitionResponse)
    if (!batch) {
      batch = { highWatermark: partitionResponse.highWatermark, lastOffset: message.offset, seekVersion, membershipVersion }
      this.#fetchBatches.set(partitionResponse, batch)
    } else {
      batch.lastOffset = message.offset
    }
    Object.defineProperty(message, kKafkaJSFetchBatch, { value: batch })
  }

  [kHandleFetchPartitionProgress] (
    topic: string,
    partition: number,
    partitionResponse: FetchResponsePartition,
    lastOffset: bigint,
    seekVersion: number,
    membershipVersion: number
  ): boolean {
    return this.push({
      [kKafkaJSFetchProgress]: {
        topic,
        partition,
        highWatermark: partitionResponse.highWatermark,
        lastOffset,
        seekVersion,
        membershipVersion
      }
    } satisfies KafkaJSFetchProgressMarker)
  }

  [kHandleOffsetOutOfRange] (
    error: GenericError,
    topicIds: Map<string, string>,
    callback: Callback<boolean>
  ): void {
    if (!error.findBy?.('apiId', 'OFFSET_OUT_OF_RANGE')) {
      callback(null, false)
      return
    }

    const response = error.response as FetchResponse | undefined
    if (!response || response.errorCode !== 0) {
      callback(null, false)
      return
    }

    const recoveredOffsets: [string, bigint][] = []
    const partitionsToRefresh = new Map<MessagesStreamFallbackModeValue, Map<string, number[]>>()

    for (const topicResponse of response.responses) {
      const topic = topicIds.get(topicResponse.topicId)
      if (!topic) {
        callback(null, false)
        return
      }

      const fallbackMode = this.#fallbackModes[topic] ?? MessagesStreamFallbackModes.LATEST
      if (fallbackMode === MessagesStreamFallbackModes.FAIL) {
        callback(null, false)
        return
      }

      for (const partitionResponse of topicResponse.partitions) {
        if (partitionResponse.errorCode === 0) {
          continue
        }
        if (partitionResponse.errorCode !== protocolErrors.OFFSET_OUT_OF_RANGE.code) {
          callback(null, false)
          return
        }

        const offset =
          fallbackMode === MessagesStreamFallbackModes.EARLIEST
            ? partitionResponse.logStartOffset
            : partitionResponse.highWatermark
        if (offset >= 0n) {
          recoveredOffsets.push([partitionKey(topic, partitionResponse.partitionIndex), offset])
          continue
        }

        let topics = partitionsToRefresh.get(fallbackMode)
        if (!topics) {
          topics = new Map()
          partitionsToRefresh.set(fallbackMode, topics)
        }
        const partitions = topics.get(topic) ?? []
        partitions.push(partitionResponse.partitionIndex)
        topics.set(topic, partitions)
      }
    }

    const pending = Array.from(partitionsToRefresh)
    const refresh = (): void => {
      const request = pending.shift()
      if (!request) {
        callback(null, this.#applyRecoveredOffsets(recoveredOffsets))
        return
      }
      const [fallbackMode, topics] = request
      this.consumer.listOffsets(
        {
          topics: Array.from(topics.keys()),
          partitions: Object.fromEntries(topics),
          timestamp:
            fallbackMode === MessagesStreamFallbackModes.EARLIEST
              ? ListOffsetTimestamps.EARLIEST
              : ListOffsetTimestamps.LATEST
        },
        (refreshError, offsets) => {
          if (refreshError) {
            callback(refreshError)
            return
          }
          for (const [topic, partitions] of topics) {
            const topicOffsets = offsets!.get(topic)
            for (const partition of partitions) {
              const offset = topicOffsets?.[partition]
              if (typeof offset !== 'bigint' || offset < 0n) {
                callback(new UserError(`Cannot recover offset out of range for topic ${topic} partition ${partition}.`))
                return
              }
              recoveredOffsets.push([partitionKey(topic, partition), offset])
            }
          }
          refresh()
        }
      )
    }
    refresh()
  }

  #applyRecoveredOffsets (recoveredOffsets: [string, bigint][]): boolean {
    for (const [key, offset] of recoveredOffsets) {
      this.offsetsToFetch.set(key, offset)
      this.offsetsCommitted.set(key, offset)
    }
    if (recoveredOffsets.length > 0) {
      this.emit('offsets')
    }
    return recoveredOffsets.length > 0
  }
}
