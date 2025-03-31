import { Readable } from 'node:stream'
import { type FetchRequestTopic, type FetchResponse } from '../../apis/consumer/fetch.ts'
import { ListOffsetTimestamps } from '../../apis/enumerations.ts'
import { UserError } from '../../errors.ts'
import { type Message } from '../../protocol/records.ts'
import { type ClusterMetadata } from '../base/types.ts'
import { createPromisifiedCallback, kCallbackPromise, type CallbackWithPromise } from '../callbacks.ts'
import { type Deserializer } from '../serde.ts'
import { type Consumer } from './consumer.ts'
import {
  MessagesStreamFallbackModes,
  MessagesStreamModes,
  type CommitOptionsPartition,
  type ConsumeOptions,
  type GroupAssignment,
  type Offsets
} from './types.ts'

// Don't move this function as being in the same file will enable V8 to remove.
// For futher info, ask Matteo.
export function noopDeserializer (data?: Buffer): Buffer | undefined {
  return data
}

// TODO(ShogunPanda): Document the impact of HighWaterMark
export class MessagesStream<Key, Value, HeaderKey, HeaderValue> extends Readable {
  #consumer: Consumer<Key, Value, HeaderKey, HeaderValue>
  #mode: string
  #fallbackMode: string
  #options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>
  #topics: string[]
  #offsetsToFetch: Map<string, bigint>
  #offsetsToCommit: Map<string, CommitOptionsPartition>
  #inflightNodes: Set<number>
  #keyDeserializer: Deserializer<Key>
  #valueDeserializer: Deserializer<Value>
  #headerKeyDeserializer: Deserializer<HeaderKey>
  #headerValueDeserializer: Deserializer<HeaderValue>
  #autocommitEnabled: boolean
  #autocommitInterval: NodeJS.Timeout | null = null
  #shouldClose: boolean
  #nullPushed: boolean

  constructor (
    consumer: Consumer<Key, Value, HeaderKey, HeaderValue>,
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>
  ) {
    super({ objectMode: true, highWaterMark: options.highWaterMark ?? 1024 })

    const { autocommit, mode, fallbackMode, offsets, deserializers, ..._options } = options

    if (offsets && mode !== MessagesStreamModes.MANUAL) {
      throw new UserError('Cannot specify offsets when the stream mode is not MANUAL.')
    }

    if (!offsets && mode === MessagesStreamModes.MANUAL) {
      throw new UserError('Must specify offsets when the stream mode is MANUAL.')
    }

    this.#consumer = consumer
    this.#mode = mode ?? MessagesStreamModes.LATEST
    this.#fallbackMode = fallbackMode ?? MessagesStreamFallbackModes.LATEST
    this.#offsetsToCommit = new Map()
    this.#topics = structuredClone(options.topics)
    this.#inflightNodes = new Set()
    this.#keyDeserializer = deserializers?.key ?? (noopDeserializer as Deserializer<Key>)
    this.#valueDeserializer = deserializers?.value ?? (noopDeserializer as Deserializer<Value>)
    this.#headerKeyDeserializer = deserializers?.headerKey ?? (noopDeserializer as Deserializer<HeaderKey>)
    this.#headerValueDeserializer = deserializers?.headerValue ?? (noopDeserializer as Deserializer<HeaderValue>)
    this.#autocommitEnabled = !!options.autocommit
    this.#shouldClose = false
    this.#nullPushed = false

    // Restore offsets
    this.#offsetsToFetch = new Map()
    if (offsets) {
      for (const { topic, partition, offset } of offsets) {
        this.#offsetsToFetch.set(`${topic}:${partition}`, offset)
      }
    }

    // Clone the rest of the options so the user can never mutate them
    this.#options = structuredClone(_options)

    // Start the autocommit interval
    if (typeof autocommit === 'number' && autocommit > 0) {
      this.#autocommitInterval = setInterval(this.#autocommit.bind(this), this.#options.autocommit as number)
    }
  }

  close (callback: CallbackWithPromise<void>): void
  close (): Promise<void>
  close (callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this.#shouldClose) {
      callback(null)
      return callback[kCallbackPromise]
    }

    this.#shouldClose = true

    this.once('error', (error: Error) => {
      callback(error)
    })

    this.once('close', () => {
      callback(null)
    })

    return callback[kCallbackPromise]
  }

  /*
    TypeScript support - Extracted from node @types/node/stream.d.ts

    * Event emitter
    * The defined events on documents including:
    * 1. close
    * 2. data
    * 3. end
    * 4. error
    * 5. pause
    * 6. readable
    * 7. resume
  */
  addListener (event: 'data', listener: (message: Message<Key, Value, HeaderKey, HeaderValue>) => void): this
  addListener (event: 'close', listener: () => void): this
  addListener (event: 'end', listener: () => void): this
  addListener (event: 'error', listener: (err: Error) => void): this
  addListener (event: 'pause', listener: () => void): this
  addListener (event: 'readable', listener: () => void): this
  addListener (event: 'resume', listener: () => void): this
  addListener (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.addListener(event, listener)
  }

  on (event: 'data', listener: (message: Message<Key, Value, HeaderKey, HeaderValue>) => void): this
  on (event: 'close', listener: () => void): this
  on (event: 'end', listener: () => void): this
  on (event: 'error', listener: (err: Error) => void): this
  on (event: 'pause', listener: () => void): this
  on (event: 'readable', listener: () => void): this
  on (event: 'resume', listener: () => void): this
  on (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.on(event, listener)
  }

  once (event: 'data', listener: (message: Message<Key, Value, HeaderKey, HeaderValue>) => void): this
  once (event: 'close', listener: () => void): this
  once (event: 'end', listener: () => void): this
  once (event: 'error', listener: (err: Error) => void): this
  once (event: 'pause', listener: () => void): this
  once (event: 'readable', listener: () => void): this
  once (event: 'resume', listener: () => void): this
  once (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.once(event, listener)
  }

  prependListener (event: 'data', listener: (message: Message<Key, Value, HeaderKey, HeaderValue>) => void): this
  prependListener (event: 'close', listener: () => void): this
  prependListener (event: 'end', listener: () => void): this
  prependListener (event: 'error', listener: (err: Error) => void): this
  prependListener (event: 'pause', listener: () => void): this
  prependListener (event: 'readable', listener: () => void): this
  prependListener (event: 'resume', listener: () => void): this
  prependListener (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.prependListener(event, listener)
  }

  prependOnceListener (event: 'data', listener: (message: Message<Key, Value, HeaderKey, HeaderValue>) => void): this
  prependOnceListener (event: 'close', listener: () => void): this
  prependOnceListener (event: 'end', listener: () => void): this
  prependOnceListener (event: 'error', listener: (err: Error) => void): this
  prependOnceListener (event: 'pause', listener: () => void): this
  prependOnceListener (event: 'readable', listener: () => void): this
  prependOnceListener (event: 'resume', listener: () => void): this
  prependOnceListener (event: string | symbol, listener: (...args: any[]) => void): this {
    return super.prependOnceListener(event, listener)
  }

  [Symbol.asyncIterator] (): NodeJS.AsyncIterator<Message<Key, Value, HeaderKey, HeaderValue>> {
    return super[Symbol.asyncIterator]()
  }

  _construct (callback: (error?: Error) => void) {
    // List topic offsets
    this.#consumer.listOffsets(
      {
        topics: this.#topics,
        timestamp:
          this.#mode === MessagesStreamModes.EARLIEST ||
          (this.#mode !== MessagesStreamModes.LATEST && this.#fallbackMode === MessagesStreamFallbackModes.EARLIEST)
            ? ListOffsetTimestamps.EARLIEST
            : ListOffsetTimestamps.LATEST
      },
      (error, offsets) => {
        if (error) {
          callback(error)
          return
        }

        if (this.#mode !== MessagesStreamModes.COMMITTED) {
          this.#restoreOffsets(offsets, new Map(), callback)
          return
        }

        // Now restore group offsets
        const topics: GroupAssignment[] = []
        for (const topic of this.#topics) {
          topics.push(this.#assignmentsForTopic(topic)!)
        }

        this.#consumer.listCommittedOffsets({ topics }, (error, commits) => {
          if (error) {
            callback(error)
            return
          }

          this.#restoreOffsets(offsets, commits, callback)
        })
      }
    )
  }

  _destroy (error: Error | null, callback: (error?: Error | null) => void): void {
    if (this.#autocommitInterval) {
      clearInterval(this.#autocommitInterval)
    }

    callback(error)
  }

  _read () {
    this.#fetch()
  }

  #fetch () {
    if (this.#shouldClose || this.closed || this.destroyed) {
      this.#handlePostCloseCallback()
      return
    }

    this.#consumer.metadata({ topics: this.#consumer.topics.current }, (error, metadata) => {
      if (error) {
        this.destroy(error)
        return
      }

      if (this.#shouldClose || this.closed || this.destroyed) {
        this.#handlePostCloseCallback()
        return
      }

      const requests = new Map<number, FetchRequestTopic[]>()
      const topicIds = new Map<string, string>()

      // Group topic-partitions by the destination broker
      for (const topic of this.#topics) {
        const partitions = this.#assignmentsForTopic(topic)!.partitions

        for (const partition of partitions) {
          const leader = metadata.topics.get(topic)!.partitions[partition].leader

          if (this.#inflightNodes.has(leader)) {
            continue
          }

          let leaderRequests = requests.get(leader)
          if (!leaderRequests) {
            leaderRequests = []
            requests.set(leader, leaderRequests)
          }

          const topicId = metadata.topics.get(topic)!.id
          topicIds.set(topicId, topic)

          leaderRequests.push({
            topicId,
            partitions: [
              {
                partition,
                fetchOffset: this.#offsetsToFetch.get(`${topic}:${partition}`) ?? 0n,
                partitionMaxBytes: this.#options.maxBytes!,
                currentLeaderEpoch: -1,
                lastFetchedEpoch: -1
              }
            ]
          })
        }
      }

      for (const [leader, leaderRequests] of requests) {
        this.#inflightNodes.add(leader)
        this.#consumer.fetch({ ...this.#options, node: leader, topics: leaderRequests }, (error, response) => {
          this.#inflightNodes.delete(leader)

          if (error) {
            this.destroy(error)
            return
          }

          if (this.#shouldClose || this.closed || this.destroyed) {
            // When it's the last inflight, we finally close the stream.
            // This is done to avoid the user exiting from consmuming metrics like for-await and still see the process up.
            if (this.#inflightNodes.size === 0) {
              this.#handlePostCloseCallback()
            }

            return
          }

          this.#push(metadata, topicIds, response)
        })
      }
    })
  }

  #push (metadata: ClusterMetadata, topicIds: Map<string, string>, response: FetchResponse) {
    const autocommit = this.#autocommitEnabled
    let canPush = true

    const keyDeserializer = this.#keyDeserializer
    const valueDeserializer = this.#valueDeserializer
    const headerKeyDeserializer = this.#headerKeyDeserializer
    const headerValueDeserializer = this.#headerValueDeserializer

    // Parse results
    for (const topicResponse of response.responses) {
      const topic = topicIds.get(topicResponse.topicId)!

      for (const { records, partitionIndex: partition } of topicResponse.partitions) {
        if (!records) {
          continue
        }

        const firstTimestamp = records.firstTimestamp
        const leaderEpoch = metadata.topics.get(topic)!.partitions[partition].leaderEpoch

        for (const record of records.records) {
          const key = keyDeserializer(record.key)
          const value = valueDeserializer(record.value)
          const headers = new Map()
          for (const [headerKey, headerValue] of record.headers) {
            headers.set(headerKeyDeserializer(headerKey), headerValueDeserializer(headerValue))
          }

          canPush = this.push({
            key,
            value,
            headers,
            topic,
            partition,
            timestamp: firstTimestamp + record.timestampDelta,
            offset: records.firstOffset + BigInt(record.offsetDelta),
            commit: autocommit
              ? null
              : this.#commit.bind(this, topic, partition, records.firstOffset + BigInt(record.offsetDelta), leaderEpoch)
          } as Message)
        }

        // Track the last read offset
        const lastOffset = records.firstOffset + BigInt(records.lastOffsetDelta)
        this.#offsetsToFetch.set(`${topic}:${partition}`, lastOffset + 1n)

        // Autocommit if needed
        if (autocommit) {
          this.#offsetsToCommit.set(`${topic}:${partition}`, { topic, partition, offset: lastOffset, leaderEpoch })
        }
      }
    }

    if (this.#autocommitEnabled && !this.#autocommitInterval) {
      this.#autocommit()
    }

    if (canPush && !(this.#shouldClose || this.closed || this.destroyed)) {
      process.nextTick(() => {
        this.#fetch()
      })
    }
  }

  // This could optimized to only schedule once per tick on a topic-partition and only commit the latest offset
  #commit (
    topic: string,
    partition: number,
    offset: bigint,
    leaderEpoch: number,
    callback?: CallbackWithPromise<void>
  ): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    this.#consumer.commit({ offsets: [{ topic, partition, offset, leaderEpoch }] }, callback)

    return callback[kCallbackPromise]!
  }

  #autocommit () {
    if (this.#offsetsToCommit.size === 0) {
      return
    }

    this.#consumer.commit({ offsets: Array.from(this.#offsetsToCommit.values()) }, error => {
      if (error) {
        this.destroy(error)
        return
      }

      this.#offsetsToCommit.clear()
    })
  }

  #restoreOffsets (offsets: Offsets, commits: Offsets, callback: (error?: Error) => void) {
    for (const [topic, partitions] of offsets) {
      for (let i = 0; i < partitions.length; i++) {
        if (!this.#offsetsToFetch.has(`${topic}:${i}`)) {
          this.#offsetsToFetch.set(`${topic}:${i}`, partitions[i])
        }
      }
    }

    for (const [topic, partitions] of commits) {
      for (let i = 0; i < partitions.length; i++) {
        const offset = partitions[i]

        if (offset >= 0n) {
          this.#offsetsToFetch.set(`${topic}:${i}`, offset + 1n)
        } else if (this.#fallbackMode === MessagesStreamFallbackModes.FAIL) {
          callback(
            new UserError(
              `Topic ${topic} has no committed offset on partition ${i} for group ${this.#consumer.groupId}.`,
              { topic, partition: i, groupId: this.#consumer.groupId }
            )
          )
          return
        }
      }
    }

    callback()
  }

  #assignmentsForTopic (topic: string): GroupAssignment | undefined {
    return this.#consumer.assignments!.find(assignment => assignment.topic === topic)
  }

  // This is done to avoid the user exiting from consmuming metrics like for-await and still see the process up.
  #handlePostCloseCallback () {
    if (this.#nullPushed) {
      return
    }

    this.#nullPushed = true
    this.push(null)
  }
}
