import { type ValidateFunction } from 'ajv'
import {
  type CallbackWithPromise,
  createPromisifiedCallback,
  kCallbackPromise,
  runConcurrentCallbacks
} from '../../apis/callbacks.ts'
import { type FetchRequest, type FetchResponse } from '../../apis/consumer/fetch-v17.ts'
import { type HeartbeatRequest, type HeartbeatResponse } from '../../apis/consumer/heartbeat-v4.ts'
import {
  type JoinGroupRequest,
  type JoinGroupRequestProtocol,
  type JoinGroupResponse
} from '../../apis/consumer/join-group-v9.ts'
import { type LeaveGroupRequest, type LeaveGroupResponse } from '../../apis/consumer/leave-group-v5.ts'
import {
  type ListOffsetsRequest,
  type ListOffsetsRequestTopic,
  type ListOffsetsResponse
} from '../../apis/consumer/list-offsets-v9.ts'
import {
  type OffsetCommitRequest,
  type OffsetCommitRequestTopic,
  type OffsetCommitResponse
} from '../../apis/consumer/offset-commit-v9.ts'
import {
  type OffsetFetchRequest,
  type OffsetFetchRequestTopic,
  type OffsetFetchResponse
} from '../../apis/consumer/offset-fetch-v9.ts'
import {
  type SyncGroupRequest,
  type SyncGroupRequestAssignment,
  type SyncGroupResponse
} from '../../apis/consumer/sync-group-v5.ts'
import { FetchIsolationLevels, FindCoordinatorKeyTypes } from '../../apis/enumerations.ts'
import { type FindCoordinatorRequest, type FindCoordinatorResponse } from '../../apis/metadata/find-coordinator-v6.ts'
import {
  consumerCommitsChannel,
  consumerConsumesChannel,
  consumerFetchesChannel,
  consumerGroupChannel,
  consumerHeartbeatChannel,
  consumerOffsetsChannel,
  createDiagnosticContext
} from '../../diagnostic.ts'
import { type GenericError, type ProtocolError, UserError } from '../../errors.ts'
import { type ConnectionPool } from '../../network/connection-pool.ts'
import { type Connection } from '../../network/connection.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import {
  Base,
  kAfterCreate,
  kCheckNotClosed,
  kClosed,
  kCreateConnectionPool,
  kFetchConnections,
  kFormatValidationErrors,
  kGetApi,
  kGetBootstrapConnection,
  kGetConnection,
  kMetadata,
  kOptions,
  kPerformDeduplicated,
  kPerformWithRetry,
  kPrometheus,
  kValidateOptions
} from '../base/base.ts'
import { defaultBaseOptions } from '../base/options.ts'
import { type ClusterMetadata } from '../base/types.ts'
import { ensureMetric, type Gauge } from '../metrics.ts'
import { MessagesStream } from './messages-stream.ts'
import {
  commitOptionsValidator,
  consumeOptionsValidator,
  consumerOptionsValidator,
  defaultConsumerOptions,
  fetchOptionsValidator,
  groupIdAndOptionsValidator,
  groupOptionsValidator,
  listCommitsOptionsValidator,
  listOffsetsOptionsValidator
} from './options.ts'
import { roundRobinAssigner } from './partitions-assigners.ts'
import { TopicsMap } from './topics-map.ts'
import {
  type CommitOptions,
  type ConsumeOptions,
  type ConsumerOptions,
  type ExtendedGroupProtocolSubscription,
  type FetchOptions,
  type GroupAssignment,
  type GroupOptions,
  type GroupPartitionsAssigner,
  type GroupProtocolSubscription,
  type ListCommitsOptions,
  type ListOffsetsOptions,
  type Offsets
} from './types.ts'

export class Consumer<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> extends Base<
  ConsumerOptions<Key, Value, HeaderKey, HeaderValue>
> {
  groupId: string
  generationId: number
  memberId: string | null
  topics: TopicsMap
  assignments: GroupAssignment[] | null

  #members: Map<string, ExtendedGroupProtocolSubscription>
  #membershipActive: boolean
  #isLeader: boolean
  #protocol: string | null
  #coordinatorId: number | null
  #heartbeatInterval: NodeJS.Timeout | null
  #streams: Set<MessagesStream<Key, Value, HeaderKey, HeaderValue>>
  #partitionsAssigner: GroupPartitionsAssigner;
  /*
    The following requests are blocking in Kafka:

    FetchRequest (soprattutto con maxWaitMs)
    JoinGroupRequest
    SyncGroupRequest
    OffsetCommitRequest
    ProduceRequest
    ListOffsetsRequest
    ListGroupsRequest
    DescribeGroupsRequest

    In order to avoid consumer group problems, we separate FetchRequest only on a separate connection.
  */
  [kFetchConnections]: ConnectionPool

  // Metrics
  #metricActiveStreams: Gauge | undefined

  constructor (options: ConsumerOptions<Key, Value, HeaderKey, HeaderValue>) {
    super(options)

    this[kOptions] = Object.assign({}, defaultBaseOptions, defaultConsumerOptions, options)
    this[kValidateOptions](options, consumerOptionsValidator, '/options')

    this.groupId = options.groupId
    this.generationId = 0
    this.memberId = null
    this.topics = new TopicsMap()
    this.assignments = null

    this.#members = new Map()
    this.#membershipActive = false
    this.#isLeader = false
    this.#protocol = null
    this.#coordinatorId = null
    this.#heartbeatInterval = null
    this.#streams = new Set()
    this.#partitionsAssigner = this[kOptions].partitionAssigner ?? roundRobinAssigner

    this.#validateGroupOptions(this[kOptions], groupIdAndOptionsValidator)

    // Initialize connection pool
    this[kFetchConnections] = this[kCreateConnectionPool]()

    if (this[kPrometheus]) {
      ensureMetric<Gauge>(this[kPrometheus], 'Gauge', 'kafka_consumers', 'Number of active Kafka consumers').inc()

      this.#metricActiveStreams = ensureMetric<Gauge>(
        this[kPrometheus],
        'Gauge',
        'kafka_consumers_streams',
        'Number of active Kafka consumers streams'
      )

      this.topics.setMetric(
        ensureMetric<Gauge>(this[kPrometheus], 'Gauge', 'kafka_consumers_topics', 'Number of topics being consumed')
      )
    }

    this[kAfterCreate]('consumer')
  }

  get streamsCount (): number {
    return this.#streams.size
  }

  close (force: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void
  close (force?: boolean): Promise<void>
  close (force?: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (typeof force === 'function') {
      callback = force
      force = false
    }

    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this[kClosed]) {
      callback(null)
      return callback[kCallbackPromise]
    }

    this[kClosed] = true

    const closer = this.#membershipActive
      ? this.#leaveGroup.bind(this)
      : function noopCloser (_: boolean, callback: CallbackWithPromise<void>) {
        callback(null)
      }

    closer(force as boolean, error => {
      if (error) {
        this[kClosed] = false
        callback(error)
        return
      }

      this[kFetchConnections].close(error => {
        if (error) {
          this[kClosed] = false
          callback(error)
          return
        }

        super.close(error => {
          if (error) {
            this[kClosed] = false
            callback(error)
            return
          }

          this.topics.clear()

          if (this[kPrometheus]) {
            ensureMetric<Gauge>(this[kPrometheus], 'Gauge', 'kafka_consumers', 'Number of active Kafka consumers').dec()
          }

          callback(null)
        })
      })
    })

    return callback[kCallbackPromise]
  }

  consume (
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<MessagesStream<Key, Value, HeaderKey, HeaderValue>>
  ): void
  consume (
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>
  ): Promise<MessagesStream<Key, Value, HeaderKey, HeaderValue>>
  consume (
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<MessagesStream<Key, Value, HeaderKey, HeaderValue>>
  ): void | Promise<MessagesStream<Key, Value, HeaderKey, HeaderValue>> {
    if (!callback) {
      callback = createPromisifiedCallback<MessagesStream<Key, Value, HeaderKey, HeaderValue>>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, consumeOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as MessagesStream<Key, Value, HeaderKey, HeaderValue>)
      return callback[kCallbackPromise]
    }

    options.autocommit ??= this[kOptions].autocommit! ?? true
    options.maxBytes ??= this[kOptions].maxBytes!
    options.deserializers = Object.assign({}, options.deserializers, this[kOptions].deserializers)
    options.highWaterMark ??= this[kOptions].highWaterMark!

    this.#consume(options, callback)

    return callback![kCallbackPromise]
  }

  fetch (options: FetchOptions<Key, Value, HeaderKey, HeaderValue>, callback: CallbackWithPromise<FetchResponse>): void
  fetch (options: FetchOptions<Key, Value, HeaderKey, HeaderValue>): Promise<FetchResponse>
  fetch (
    options: FetchOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<FetchResponse>
  ): void | Promise<FetchResponse> {
    if (!callback) {
      callback = createPromisifiedCallback<FetchResponse>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, fetchOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as FetchResponse)
      return callback[kCallbackPromise]
    }

    consumerFetchesChannel.traceCallback(
      this.#fetch,
      1,
      createDiagnosticContext({ client: this, operation: 'fetch', options }),
      this,
      options,
      callback
    )

    return callback![kCallbackPromise]
  }

  commit (options: CommitOptions, callback: CallbackWithPromise<void>): void
  commit (options: CommitOptions): Promise<void>
  commit (options: CommitOptions, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, commitOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError)
      return callback[kCallbackPromise]
    }

    consumerCommitsChannel.traceCallback(
      this.#commit,
      1,
      createDiagnosticContext({ client: this, operation: 'commit', options }),
      this,
      options,
      callback
    )

    return callback![kCallbackPromise]
  }

  listOffsets (options: ListOffsetsOptions, callback: CallbackWithPromise<Offsets>): void
  listOffsets (options: ListOffsetsOptions): Promise<Offsets>
  listOffsets (options: ListOffsetsOptions, callback?: CallbackWithPromise<Offsets>): void | Promise<Offsets> {
    if (!callback) {
      callback = createPromisifiedCallback<Offsets>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, listOffsetsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as Offsets)
      return callback[kCallbackPromise]
    }

    consumerOffsetsChannel.traceCallback(
      this.#listOffsets,
      1,
      createDiagnosticContext({ client: this, operation: 'listOffsets', options }),
      this,
      options,
      callback
    )

    return callback![kCallbackPromise]
  }

  listCommittedOffsets (options: ListCommitsOptions, callback: CallbackWithPromise<Offsets>): void
  listCommittedOffsets (options: ListCommitsOptions): Promise<Offsets>
  listCommittedOffsets (options: ListCommitsOptions, callback?: CallbackWithPromise<Offsets>): void | Promise<Offsets> {
    if (!callback) {
      callback = createPromisifiedCallback<Offsets>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, listCommitsOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as Offsets)
      return callback[kCallbackPromise]
    }

    consumerOffsetsChannel.traceCallback(
      this.#listCommittedOffsets,
      1,
      createDiagnosticContext({ client: this, operation: 'listCommittedOffsets', options }),
      this,
      options,
      callback
    )

    return callback![kCallbackPromise]
  }

  findGroupCoordinator (callback: CallbackWithPromise<number>): void
  findGroupCoordinator (): Promise<number>
  findGroupCoordinator (callback?: CallbackWithPromise<number>): void | Promise<number> {
    if (!callback) {
      callback = createPromisifiedCallback<number>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    if (this.#coordinatorId) {
      callback(null, this.#coordinatorId)
      return callback[kCallbackPromise]
    }

    this.#findGroupCoordinator(callback)

    return callback[kCallbackPromise]
  }

  joinGroup (options: GroupOptions, callback: CallbackWithPromise<string>): void
  joinGroup (options: GroupOptions): Promise<string>
  joinGroup (options: GroupOptions, callback?: CallbackWithPromise<string>): void | Promise<string> {
    if (!callback) {
      callback = createPromisifiedCallback<string>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    const validationError = this[kValidateOptions](options, groupOptionsValidator, '/options', false)
    if (validationError) {
      callback(validationError, undefined as unknown as string)
      return callback[kCallbackPromise]
    }

    options.sessionTimeout ??= this[kOptions].sessionTimeout!
    options.rebalanceTimeout ??= this[kOptions].rebalanceTimeout!
    options.heartbeatInterval ??= this[kOptions].heartbeatInterval!
    options.protocols ??= this[kOptions].protocols!

    this.#validateGroupOptions(options)

    this.#membershipActive = true
    this.#joinGroup(options as Required<GroupOptions>, callback)

    return callback[kCallbackPromise]
  }

  leaveGroup (force?: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void
  leaveGroup (force?: boolean): Promise<void>
  leaveGroup (force?: boolean | CallbackWithPromise<void>, callback?: CallbackWithPromise<void>): void | Promise<void> {
    if (typeof force === 'function') {
      callback = force
      force = false
    }

    if (!callback) {
      callback = createPromisifiedCallback<void>()
    }

    if (this[kCheckNotClosed](callback)) {
      return callback[kCallbackPromise]
    }

    this.#membershipActive = false
    this.#leaveGroup(force as boolean, error => {
      if (error) {
        this.#membershipActive = true
        callback(error)
        return
      }

      callback(null)
    })

    return callback[kCallbackPromise]
  }

  #consume (
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<MessagesStream<Key, Value, HeaderKey, HeaderValue>>
  ): void {
    consumerConsumesChannel.traceCallback(
      this.#performConsume,
      2,
      createDiagnosticContext({ client: this, operation: 'consume', options }),
      this,
      options,
      true,
      callback
    )
  }

  #fetch (
    options: FetchOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<FetchResponse>
  ): void {
    this[kPerformWithRetry]<FetchResponse>(
      'fetch',
      retryCallback => {
        this[kMetadata]({ topics: this.topics.current }, (error, metadata: ClusterMetadata) => {
          if (error) {
            retryCallback(error, undefined as unknown as FetchResponse)
            return
          }

          const broker = metadata.brokers.get(options.node)

          if (!broker) {
            retryCallback(
              new UserError(`Cannot find broker with node id ${options.node}`),
              undefined as unknown as FetchResponse
            )
            return
          }

          this[kFetchConnections].get(broker, (error, connection) => {
            if (error) {
              retryCallback(error, undefined as unknown as FetchResponse)
              return
            }

            this[kGetApi]<FetchRequest, FetchResponse>('Fetch', (error, api) => {
              if (error) {
                retryCallback(error, undefined as unknown as FetchResponse)
                return
              }

              api(
                connection,
                options.maxWaitTime ?? this[kOptions].maxWaitTime!,
                options.minBytes ?? this[kOptions].minBytes!,
                options.maxBytes ?? this[kOptions].maxBytes!,
                FetchIsolationLevels[options.isolationLevel ?? this[kOptions].isolationLevel!],
                0,
                0,
                options.topics,
                [],
                '',
                retryCallback
              )
            })
          })
        })
      },
      callback,
      0
    )
  }

  #commit (options: CommitOptions, callback: CallbackWithPromise<void>): void {
    this.#performGroupOperation<OffsetCommitResponse>(
      'commit',
      (connection, groupCallback) => {
        const topics = new Map<string, OffsetCommitRequestTopic>()

        for (const { topic, partition, offset, leaderEpoch } of options.offsets) {
          let topicOffsets = topics.get(topic)
          if (!topicOffsets) {
            topicOffsets = { name: topic, partitions: [] }
            topics.set(topic, topicOffsets)
          }

          topicOffsets.partitions.push({
            partitionIndex: partition,
            committedOffset: offset,
            committedLeaderEpoch: leaderEpoch,
            committedMetadata: null
          })
        }

        this[kGetApi]<OffsetCommitRequest, OffsetCommitResponse>('OffsetCommit', (error, api) => {
          if (error) {
            groupCallback(error, undefined as unknown as OffsetCommitResponse)
            return
          }

          api(
            connection,
            this.groupId,
            this.generationId,
            this.memberId!,
            null,
            Array.from(topics.values()),
            groupCallback
          )
        })
      },
      error => {
        callback(error)
      }
    )
  }

  #listOffsets (options: ListOffsetsOptions, callback: CallbackWithPromise<Offsets>): void {
    this[kMetadata]({ topics: options.topics }, (error, metadata) => {
      if (error) {
        callback(error, undefined as unknown as Offsets)
        return
      }

      const requests = new Map<number, Map<string, ListOffsetsRequestTopic>>()

      for (const name of options.topics) {
        const topic = metadata.topics.get(name)!

        for (let i = 0; i < topic.partitionsCount; i++) {
          const partition = topic.partitions[i]
          const { leader, leaderEpoch } = partition

          let leaderRequests = requests.get(leader)
          if (!leaderRequests) {
            leaderRequests = new Map()
            requests.set(leader, leaderRequests)
          }

          let topicRequests = leaderRequests.get(name)
          if (!topicRequests) {
            topicRequests = { name, partitions: [] }
            leaderRequests.set(name, topicRequests)
          }

          topicRequests.partitions.push({
            partitionIndex: i,
            currentLeaderEpoch: leaderEpoch,
            timestamp: options.timestamp ?? -1n
          })
        }
      }

      runConcurrentCallbacks<ListOffsetsResponse>(
        'Listing offsets failed.',
        requests,
        ([leader, requests], concurrentCallback) => {
          this[kPerformWithRetry]<ListOffsetsResponse>(
            'listOffsets',
            retryCallback => {
              this[kGetConnection](metadata.brokers.get(leader)!, (error, connection) => {
                if (error) {
                  retryCallback(error, undefined as unknown as ListOffsetsResponse)
                  return
                }

                this[kGetApi]<ListOffsetsRequest, ListOffsetsResponse>('ListOffsets', (error, api) => {
                  if (error) {
                    retryCallback(error, undefined as unknown as ListOffsetsResponse)
                    return
                  }

                  api(
                    connection,
                    -1,
                    FetchIsolationLevels[options.isolationLevel ?? this[kOptions].isolationLevel!],
                    Array.from(requests.values()),
                    retryCallback
                  )
                })
              })
            },
            concurrentCallback,
            0
          )
        },
        (error, responses) => {
          if (error) {
            callback(error, undefined as unknown as Offsets)
            return
          }

          const offsets: Offsets = new Map()

          for (const response of responses) {
            for (const { name: topic, partitions } of response.topics) {
              let topicOffsets = offsets.get(topic)

              if (!topicOffsets) {
                topicOffsets = Array(metadata.topics.get(topic)!.partitionsCount)
                offsets.set(topic, topicOffsets)
              }

              for (const { partitionIndex: index, offset } of partitions) {
                topicOffsets[index] = offset
              }
            }
          }

          callback(null, offsets)
        }
      )
    })
  }

  #listCommittedOffsets (options: ListCommitsOptions, callback: CallbackWithPromise<Offsets>): void {
    const topics: OffsetFetchRequestTopic[] = []

    for (const { topic: name, partitions } of options.topics) {
      topics.push({ name, partitionIndexes: partitions })
    }

    this.#performGroupOperation<OffsetFetchResponse>(
      'listCommits',
      (connection, groupCallback) => {
        this[kGetApi]<OffsetFetchRequest, OffsetFetchResponse>('OffsetFetch', (error, api) => {
          if (error) {
            groupCallback(error, undefined as unknown as OffsetFetchResponse)
            return
          }

          api(
            connection,
            // Note: once we start implementing KIP-848, the memberEpoch must be obtained
            [{ groupId: this.groupId, memberId: this.memberId!, memberEpoch: -1, topics }],
            false,
            groupCallback
          )
        })
      },
      (error, response) => {
        if (error) {
          callback(error, undefined as unknown as Offsets)
          return
        }

        const committed: Offsets = new Map()
        for (const responseGroup of response.groups) {
          for (const responseTopic of responseGroup.topics) {
            const topic = responseTopic.name

            const partitions = Array(responseTopic.partitions.length)
            for (const { partitionIndex: index, committedOffset } of responseTopic.partitions) {
              partitions[index] = committedOffset
            }

            committed.set(topic, partitions)
          }
        }

        callback(null, committed)
      }
    )
  }

  #findGroupCoordinator (callback: CallbackWithPromise<number>): void {
    if (this.#coordinatorId) {
      callback(null, this.#coordinatorId)
      return
    }

    consumerGroupChannel.traceCallback(
      this.#performFindGroupCoordinator,
      0,
      createDiagnosticContext({ client: this, operation: 'findGroupCoordinator' }),
      this,
      callback
    )
  }

  #joinGroup (options: Required<GroupOptions>, callback: CallbackWithPromise<string>): void {
    consumerGroupChannel.traceCallback(
      this.#performJoinGroup,
      1,
      createDiagnosticContext({ client: this, operation: 'joinGroup', options }),
      this,
      options,
      callback
    )
  }

  #leaveGroup (force: boolean, callback: CallbackWithPromise<void>): void {
    consumerGroupChannel.traceCallback(
      this.#performLeaveGroup,
      1,
      createDiagnosticContext({ client: this, operation: 'leaveGroup', force }),
      this,
      force,
      callback
    )
  }

  #syncGroup (callback: CallbackWithPromise<GroupAssignment[]>): void {
    consumerGroupChannel.traceCallback(
      this.#performSyncGroup,
      1,
      createDiagnosticContext({ client: this, operation: 'syncGroup' }),
      this,
      null,
      callback
    )
  }

  #heartbeat (options: Required<GroupOptions>): void {
    const eventPayload = { groupId: this.groupId, memberId: this.memberId, generationId: this.generationId }

    consumerHeartbeatChannel.traceCallback(
      this.#performDeduplicateGroupOperaton<HeartbeatResponse>,
      2,
      createDiagnosticContext({ client: this, operation: 'heartbeat' }),
      this,
      'heartbeat',
      (connection, groupCallback) => {
        // We have left the group in the meanwhile, abort
        if (!this.#membershipActive) {
          this.emitWithDebug('consumer:heartbeat', 'cancel', eventPayload)
          return
        }

        this.emitWithDebug('consumer:heartbeat', 'start', eventPayload)

        this[kGetApi]<HeartbeatRequest, HeartbeatResponse>('Heartbeat', (error, api) => {
          if (error) {
            groupCallback(error, undefined as unknown as HeartbeatResponse)
            return
          }

          api(connection, this.groupId, this.generationId, this.memberId!, null, groupCallback)
        })
      },
      error => {
        // The heartbeat has been aborted elsewhere, ignore the response
        if (this.#heartbeatInterval === null || !this.#membershipActive) {
          this.emitWithDebug('consumer:heartbeat', 'cancel', eventPayload)
          return
        }

        if (error) {
          this.#cancelHeartbeat()

          if (this.#getRejoinError(error)) {
            this[kPerformWithRetry](
              'rejoinGroup',
              retryCallback => {
                this.#joinGroup(options, retryCallback)
              },
              error => {
                if (error) {
                  this.emitWithDebug(null, 'error', error)
                }

                this.emitWithDebug('consumer', 'rejoin')
              },
              0
            )

            return
          }

          this.emitWithDebug('consumer:heartbeat', 'error', { ...eventPayload, error })

          // Note that here we purposely do not return, since it was not a group related problem we schedule another heartbeat
        } else {
          this.emitWithDebug('consumer:heartbeat', 'end', eventPayload)
        }

        this.#heartbeatInterval?.refresh()
      }
    )
  }

  #cancelHeartbeat (): void {
    clearTimeout(this.#heartbeatInterval!)
    this.#heartbeatInterval = null
  }

  #performConsume (
    options: ConsumeOptions<Key, Value, HeaderKey, HeaderValue>,
    trackTopics: boolean,
    callback: CallbackWithPromise<MessagesStream<Key, Value, HeaderKey, HeaderValue>>
  ): void {
    // Subscribe all topics
    let joinNeeded = this.memberId === null

    if (trackTopics) {
      for (const topic of options.topics) {
        if (this.topics.track(topic)) {
          joinNeeded = true
        }
      }
    }

    // If we need to (re)join the group, do that first and then try again
    if (joinNeeded) {
      this.joinGroup(options, error => {
        if (error) {
          callback(error, undefined as unknown as MessagesStream<Key, Value, HeaderKey, HeaderValue>)
          return
        }

        this.#performConsume(options, false, callback)
      })

      return
    }

    // Create the stream and start consuming
    const stream = new MessagesStream<Key, Value, HeaderKey, HeaderValue>(
      this,
      options as ConsumeOptions<Key, Value, HeaderKey, HeaderValue>
    )
    this.#streams.add(stream)

    this.#metricActiveStreams?.inc()
    stream.once('close', () => {
      this.#metricActiveStreams?.dec()
      this.#streams.delete(stream)
    })

    callback(null, stream)
  }

  #performFindGroupCoordinator (callback: CallbackWithPromise<number>): void {
    this[kPerformDeduplicated](
      'findGroupCoordinator',
      deduplicateCallback => {
        this[kPerformWithRetry]<FindCoordinatorResponse>(
          'findGroupCoordinator',
          retryCallback => {
            this[kGetBootstrapConnection]((error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as FindCoordinatorResponse)
                return
              }

              this[kGetApi]<FindCoordinatorRequest, FindCoordinatorResponse>('FindCoordinator', (error, api) => {
                if (error) {
                  retryCallback(error, undefined as unknown as FindCoordinatorResponse)
                  return
                }

                api(connection, FindCoordinatorKeyTypes.GROUP, [this.groupId], retryCallback)
              })
            })
          },
          (error, response) => {
            if (error) {
              deduplicateCallback(error, undefined as unknown as number)
              return
            }

            const groupInfo = response.coordinators.find(coordinator => coordinator.key === this.groupId)!
            this.#coordinatorId = groupInfo.nodeId
            deduplicateCallback(null, this.#coordinatorId)
          },
          0
        )
      },
      callback
    )
  }

  #performJoinGroup (options: Required<GroupOptions>, callback: CallbackWithPromise<string>): void {
    if (!this.#membershipActive) {
      callback(null, undefined as unknown as string)
      return
    }

    this.#cancelHeartbeat()

    const protocols: JoinGroupRequestProtocol[] = []
    for (const protocol of options.protocols) {
      protocols.push({
        name: protocol.name,
        metadata: this.#encodeProtocolSubscriptionMetadata(protocol, this.topics.current)
      })
    }

    this.#performDeduplicateGroupOperaton<JoinGroupResponse>(
      'joinGroup',
      (connection, groupCallback) => {
        this[kGetApi]<JoinGroupRequest, JoinGroupResponse>('JoinGroup', (error, api) => {
          if (error) {
            groupCallback(error, undefined as unknown as JoinGroupResponse)
            return
          }

          api(
            connection,
            this.groupId,
            options.sessionTimeout,
            options.rebalanceTimeout,
            this.memberId ?? '',
            null,
            'consumer',
            protocols,
            '',
            groupCallback
          )
        })
      },
      (error, response) => {
        if (!this.#membershipActive) {
          callback(null, undefined as unknown as string)
          return
        }

        if (error) {
          if (this.#getRejoinError(error)) {
            this.#performJoinGroup(options, callback)
            return
          }

          callback(error, undefined as unknown as string)
          return
        }

        this.generationId = response.generationId
        this.#isLeader = response.leader === this.memberId
        this.#protocol = response.protocolName!

        this.#members = new Map()
        for (const member of response.members) {
          this.#members.set(
            member.memberId,
            this.#decodeProtocolSubscriptionMetadata(member.memberId, member.metadata!)
          )
        }

        // Send a syncGroup request
        this.#syncGroup((error, response) => {
          if (!this.#membershipActive) {
            callback(null, undefined as unknown as string)
            return
          }

          if (error) {
            if (this.#getRejoinError(error)) {
              this.#performJoinGroup(options, callback)
              return
            }

            callback(error, undefined as unknown as string)
            return
          }

          this.assignments = response

          this.#cancelHeartbeat()
          this.#heartbeatInterval = setTimeout(() => {
            this.#heartbeat(options)
          }, options.heartbeatInterval)

          this.emitWithDebug('consumer', 'group:join', {
            groupId: this.groupId,
            memberId: this.memberId,
            generationId: this.generationId,
            isLeader: this.#isLeader,
            assignments: this.assignments
          })

          callback(null, this.memberId!)
        })
      }
    )
  }

  #performLeaveGroup (force: boolean, callback: CallbackWithPromise<void>): void {
    if (!this.memberId) {
      callback(null)
      return
    }

    // Remove streams that might have been exited in the meanwhile
    for (const stream of this.#streams) {
      if (stream.closed || stream.destroyed) {
        this.#streams.delete(stream)
      }
    }

    if (this.#streams.size) {
      if (!force) {
        callback(new UserError('Cannot leave group while consuming messages.'))
        return
      }

      runConcurrentCallbacks<void>(
        'Closing streams failed.',
        this.#streams,
        (stream, concurrentCallback) => {
          stream.close(concurrentCallback)
        },
        error => {
          if (error) {
            callback(error)
            return
          }

          // All streams are closed, try the operation again without force
          this.#performLeaveGroup(false, callback)
        }
      )

      return
    }

    this.#cancelHeartbeat()

    this.#performDeduplicateGroupOperaton<LeaveGroupResponse>(
      'leaveGroup',
      (connection, groupCallback) => {
        this[kGetApi]<LeaveGroupRequest, LeaveGroupResponse>('LeaveGroup', (error, api) => {
          if (error) {
            groupCallback(error, undefined as unknown as LeaveGroupResponse)
            return
          }

          api(connection, this.groupId, [{ memberId: this.memberId! }], groupCallback)
        })
      },
      error => {
        if (error) {
          const unknownMemberError = (error as GenericError).findBy<ProtocolError>?.('unknownMemberId', true)

          // This is to avoid throwing an error if a group join was cancelled.
          if (!unknownMemberError) {
            callback(error)
            return
          }
        }

        this.emitWithDebug('consumer', 'group:leave', {
          groupId: this.groupId,
          memberId: this.memberId,
          generationId: this.generationId
        })

        this.memberId = null
        this.generationId = 0
        this.assignments = null

        callback(null)
      }
    )
  }

  #performSyncGroup (
    assignments: SyncGroupRequestAssignment[] | null,
    callback: CallbackWithPromise<GroupAssignment[]>
  ): void {
    if (!this.#membershipActive) {
      callback(null, [])
      return
    }

    if (!Array.isArray(assignments)) {
      if (this.#isLeader) {
        // Get all the metadata for  the topics the consumer are listening to, then compute the assignments
        const topicsSubscriptions = new Map<string, ExtendedGroupProtocolSubscription[]>()

        for (const subscription of this.#members.values()) {
          for (const topic of subscription.topics!) {
            let topicSubscriptions = topicsSubscriptions.get(topic)

            if (!topicSubscriptions) {
              topicSubscriptions = []
              topicsSubscriptions.set(topic, topicSubscriptions)
            }

            topicSubscriptions.push(subscription)
          }
        }

        this[kMetadata]({ topics: Array.from(topicsSubscriptions.keys()) }, (error, metadata) => {
          if (error) {
            callback(error, undefined as unknown as GroupAssignment[])
            return
          }

          this.#performSyncGroup(this.#createAssignments(metadata), callback)
        })

        return
      } else {
        // Non leader simply do not send any assignments and wait
        assignments = []
      }
    }

    this.#performDeduplicateGroupOperaton<SyncGroupResponse>(
      'syncGroup',
      (connection, groupCallback) => {
        this[kGetApi]<SyncGroupRequest, SyncGroupResponse>('SyncGroup', (error, api) => {
          if (error) {
            groupCallback(error, undefined as unknown as SyncGroupResponse)
            return
          }

          api(
            connection,
            this.groupId,
            this.generationId,
            this.memberId!,
            null,
            'consumer',
            this.#protocol!,
            assignments!,
            groupCallback
          )
        })
      },
      (error, response) => {
        if (!this.#membershipActive) {
          callback(null, undefined as unknown as GroupAssignment[])
          return
        }

        if (error) {
          callback(error, undefined as unknown as GroupAssignment[])
          return
        }

        // Read the assignment back
        const reader = Reader.from(response.assignment)

        const assignments: GroupAssignment[] = reader.readArray(
          r => {
            return {
              topic: r.readString(),
              partitions: r.readArray(r => r.readInt32(), true, false)
            }
          },
          true,
          false
        )

        callback(error, assignments)
      }
    )
  }

  #performDeduplicateGroupOperaton<ReturnType> (
    operationId: string,
    operation: (connection: Connection, callback: CallbackWithPromise<ReturnType>) => void,
    callback: CallbackWithPromise<ReturnType>
  ): void | Promise<ReturnType> {
    return this[kPerformDeduplicated](
      operationId,
      deduplicateCallback => {
        this.#performGroupOperation<ReturnType>(operationId, operation, deduplicateCallback)
      },
      callback
    )
  }

  #performGroupOperation<ReturnType> (
    operationId: string,
    operation: (connection: Connection, callback: CallbackWithPromise<ReturnType>) => void,
    callback: CallbackWithPromise<ReturnType>
  ): void | Promise<ReturnType> {
    this.#findGroupCoordinator((error, coordinatorId) => {
      if (error) {
        callback(error, undefined as unknown as ReturnType)
        return
      }

      this[kMetadata]({ topics: this.topics.current }, (error: Error | null, metadata: ClusterMetadata) => {
        if (error) {
          callback(error, undefined as unknown as ReturnType)
          return
        }

        this[kPerformWithRetry]<ReturnType>(
          operationId,
          retryCallback => {
            this[kGetConnection](metadata.brokers.get(coordinatorId)!, (error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as ReturnType)
                return
              }

              operation(connection, retryCallback)
            })
          },
          callback
        )
      })
    })
  }

  #validateGroupOptions (options: GroupOptions, validator?: ValidateFunction<unknown>): void {
    validator ??= groupOptionsValidator
    const valid = validator(options)

    if (!valid) {
      throw new UserError(this[kFormatValidationErrors](validator, '/options'))
    }
  }

  /*
    The following two methods follow:
    https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolSubscription.json
  */
  #encodeProtocolSubscriptionMetadata (metadata: GroupProtocolSubscription, topics: string[]): Buffer {
    return Writer.create()
      .appendInt16(metadata.version)
      .appendArray(topics, (w, t) => w.appendString(t, false), false, false)
      .appendBytes(typeof metadata.metadata === 'string' ? Buffer.from(metadata.metadata) : metadata.metadata, false)
      .buffer
  }

  #decodeProtocolSubscriptionMetadata (memberId: string, buffer: Buffer): ExtendedGroupProtocolSubscription {
    const reader = Reader.from(buffer)

    return {
      memberId,
      version: reader.readInt16(),
      topics: reader.readArray(r => r.readString(false), false, false),
      metadata: reader.readBytes(false)
    }
  }

  /*
    This follows:
    https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ConsumerProtocolAssignment.json
  */
  #encodeProtocolAssignment (assignments: GroupAssignment[]): Buffer {
    return Writer.create().appendArray(
      assignments,
      (w, { topic, partitions }) => {
        w.appendString(topic).appendArray(partitions, (w, a) => w.appendInt32(a), true, false)
      },
      true,
      false
    ).buffer
  }

  #createAssignments (metadata: ClusterMetadata): SyncGroupRequestAssignment[] {
    const partitionTracker: Map<string, { next: number; max: number }> = new Map()

    // First of all, layout topics-partitions in a list
    for (const [topic, partitions] of metadata.topics) {
      partitionTracker.set(topic, { next: 0, max: partitions.partitionsCount })
    }

    // We are the only member of the group, assign all partitions to us
    const membersSize = this.#members.size
    if (membersSize === 1) {
      const assignments: GroupAssignment[] = []

      for (const topic of this.topics.current) {
        const partitionsCount = metadata.topics.get(topic)!.partitionsCount
        const partitions: number[] = []
        for (let i = 0; i < partitionsCount; i++) {
          partitions.push(i)
        }

        assignments.push({ topic, partitions })
      }

      return [{ memberId: this.memberId!, assignment: this.#encodeProtocolAssignment(assignments) }]
    }

    const encodedAssignments: SyncGroupRequestAssignment[] = []
    for (const member of this.#partitionsAssigner(
      this.memberId!,
      this.#members,
      new Set(this.topics.current),
      metadata
    )) {
      encodedAssignments.push({
        memberId: member.memberId,
        assignment: this.#encodeProtocolAssignment(Array.from(member.assignments.values()))
      })
    }

    return encodedAssignments
  }

  #getRejoinError (error: Error): ProtocolError | null {
    const protocolError = (error as GenericError).findBy<ProtocolError>?.('needsRejoin', true)

    if (!protocolError) {
      return null
    }

    if (protocolError.rebalanceInProgress) {
      this.emitWithDebug('consumer', 'group:rebalance', { groupId: this.groupId })
    }

    if (protocolError.unknownMemberId) {
      this.memberId = null
    } else if (protocolError.memberId! && !this.memberId) {
      this.memberId = protocolError.memberId!
    }

    // This is only used in testing
    if (protocolError.cancelMembership) {
      this.#membershipActive = false
    }

    return protocolError
  }
}
