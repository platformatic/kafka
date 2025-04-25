import { type ValidateFunction } from 'ajv'
import { type FetchResponse, api as fetchV17 } from '../../apis/consumer/fetch.ts'
import { type HeartbeatResponse, api as heartbeatV4 } from '../../apis/consumer/heartbeat.ts'
import {
  type JoinGroupRequestProtocol,
  type JoinGroupResponse,
  api as joinGroupV9
} from '../../apis/consumer/join-group.ts'
import { type LeaveGroupResponse, api as leaveGroupV5 } from '../../apis/consumer/leave-group.ts'
import {
  type ListOffsetsRequestTopic,
  type ListOffsetsResponse,
  api as listOffsetsV9
} from '../../apis/consumer/list-offsets.ts'
import {
  type OffsetCommitRequestTopic,
  type OffsetCommitResponse,
  api as offsetCommitV9
} from '../../apis/consumer/offset-commit.ts'
import {
  type OffsetFetchRequestTopic,
  type OffsetFetchResponse,
  api as offsetFetchV9
} from '../../apis/consumer/offset-fetch.ts'
import {
  type SyncGroupRequestAssignment,
  type SyncGroupResponse,
  api as syncGroupV5
} from '../../apis/consumer/sync-group.ts'
import { FetchIsolationLevels, FindCoordinatorKeyTypes } from '../../apis/enumerations.ts'
import { type FindCoordinatorResponse, api as findCoordinatorV6 } from '../../apis/metadata/find-coordinator.ts'
import { type GenericError, type ProtocolError, UserError } from '../../errors.ts'
import { type ConnectionPool } from '../../network/connection-pool.ts'
import { type Connection } from '../../network/connection.ts'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import {
  Base,
  kBootstrapBrokers,
  kCheckNotClosed,
  kClosed,
  kConnections,
  kCreateConnectionPool,
  kFetchConnections,
  kFormatValidationErrors,
  kMetadata,
  kOptions,
  kPerformDeduplicated,
  kPerformWithRetry,
  kPrometheus,
  kValidateOptions
} from '../base/base.ts'
import { defaultBaseOptions } from '../base/options.ts'
import { type ClusterMetadata } from '../base/types.ts'
import {
  type CallbackWithPromise,
  createPromisifiedCallback,
  kCallbackPromise,
  runConcurrentCallbacks
} from '../callbacks.ts'
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
import { TopicsMap } from './topics-map.ts'
import {
  type CommitOptions,
  type ConsumeOptions,
  type ConsumerOptions,
  type ExtendedGroupProtocolSubscription,
  type FetchOptions,
  type GroupAssignment,
  type GroupOptions,
  type GroupProtocolSubscription,
  type ListCommitsOptions,
  type ListOffsetsOptions,
  type Offsets
} from './types.ts'

interface GroupRequestAssignment {
  memberId: string
  assignments: Map<string, GroupAssignment>
}

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
  #streams: Set<MessagesStream<Key, Value, HeaderKey, HeaderValue>>;
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

    this.#fetch(options, callback)
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

    this.#commit(options, callback)
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

    this.#listOffsets(options, callback)
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

    this.#listCommittedOffsets(options, callback)
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
    callback: CallbackWithPromise<MessagesStream<Key, Value, HeaderKey, HeaderValue>>,
    trackTopics: boolean = true
  ): void | Promise<MessagesStream<Key, Value, HeaderKey, HeaderValue>> {
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

        this.#consume(options, callback, false)
      })

      return callback![kCallbackPromise]
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

    return callback![kCallbackPromise]
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

            fetchV17(
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

        offsetCommitV9(
          connection,
          this.groupId,
          this.generationId,
          this.memberId!,
          null,
          Array.from(topics.values()),
          groupCallback
        )
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
              this[kConnections].get(metadata.brokers.get(leader)!, (error, connection) => {
                if (error) {
                  retryCallback(error, undefined as unknown as ListOffsetsResponse)
                  return
                }

                listOffsetsV9(
                  connection,
                  -1,
                  FetchIsolationLevels[options.isolationLevel ?? this[kOptions].isolationLevel!],
                  Array.from(requests.values()),
                  retryCallback
                )
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
        offsetFetchV9(
          connection,
          // Note: once we start implementing KIP-848, the memberEpoch must be obtained
          [{ groupId: this.groupId, memberId: this.memberId!, memberEpoch: -1, topics }],
          false,
          groupCallback
        )
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

    this[kPerformDeduplicated](
      'findGroupCoordinator',
      deduplicateCallback => {
        this[kPerformWithRetry]<FindCoordinatorResponse>(
          'findGroupCoordinator',
          retryCallback => {
            this[kConnections].getFirstAvailable(this[kBootstrapBrokers], (error, connection) => {
              if (error) {
                retryCallback(error, undefined as unknown as FindCoordinatorResponse)
                return
              }

              findCoordinatorV6(connection, FindCoordinatorKeyTypes.GROUP, [this.groupId], retryCallback)
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

  #joinGroup (options: Required<GroupOptions>, callback: CallbackWithPromise<string>): void {
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
        joinGroupV9(
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
      },
      (error, response) => {
        if (!this.#membershipActive) {
          callback(null, undefined as unknown as string)
          return
        }

        if (error) {
          if (this.#getRejoinError(error)) {
            this.#joinGroup(options, callback)
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
        this.#syncGroup(null, (error, response) => {
          if (!this.#membershipActive) {
            callback(null, undefined as unknown as string)
            return
          }

          if (error) {
            if (this.#getRejoinError(error)) {
              this.#joinGroup(options, callback)
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

  #leaveGroup (force: boolean, callback: CallbackWithPromise<void>): void {
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
          this.#leaveGroup(false, callback)
        }
      )

      return
    }

    this.#cancelHeartbeat()

    this.#performDeduplicateGroupOperaton<LeaveGroupResponse>(
      'leaveGroup',
      (connection, groupCallback) => {
        leaveGroupV5(connection, this.groupId, [{ memberId: this.memberId! }], groupCallback)
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

  #syncGroup (assignments: SyncGroupRequestAssignment[] | null, callback: CallbackWithPromise<GroupAssignment[]>): void {
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

          this.#syncGroup(this.#roundRobinAssignments(metadata), callback)
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
        syncGroupV5(
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

  #heartbeat (options: Required<GroupOptions>): void {
    const eventPayload = { groupId: this.groupId, memberId: this.memberId, generationId: this.generationId }

    this.#performDeduplicateGroupOperaton<HeartbeatResponse>(
      'heartbeat',
      (connection, groupCallback) => {
        // We have left the group in the meanwhile, abort
        if (!this.#membershipActive) {
          this.emitWithDebug('consumer:heartbeat', 'cancel', eventPayload)
          return
        }

        this.emitWithDebug('consumer:heartbeat', 'start', eventPayload)
        heartbeatV4(connection, this.groupId, this.generationId, this.memberId!, null, groupCallback)
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

  #performDeduplicateGroupOperaton<ReturnType>(
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

  #performGroupOperation<ReturnType>(
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
            this[kConnections].get(metadata.brokers.get(coordinatorId)!, (error, connection) => {
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

  #roundRobinAssignments (metadata: ClusterMetadata): SyncGroupRequestAssignment[] {
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

    // Flat the list of members and subscribed topics
    const members: GroupRequestAssignment[] = []
    const subscribedTopics = new Set<string>()
    for (const [memberId, subscription] of this.#members) {
      members.push({ memberId, assignments: new Map() })

      for (const topic of subscription.topics!) {
        subscribedTopics.add(topic)
      }
    }

    // Assign topic-partitions in round robin
    let currentMember = 0
    for (const topic of subscribedTopics) {
      const partitionsCount = metadata.topics.get(topic)!.partitionsCount

      for (let i = 0; i < partitionsCount; i++) {
        const member = members[currentMember++ % membersSize]
        let topicAssignments = member.assignments.get(topic)

        if (!topicAssignments) {
          topicAssignments = { topic, partitions: [] }
          member.assignments.set(topic, topicAssignments)
        }

        topicAssignments?.partitions.push(i)
      }
    }

    const assignments: SyncGroupRequestAssignment[] = []
    for (const member of members) {
      assignments.push({
        memberId: member.memberId,
        assignment: this.#encodeProtocolAssignment(Array.from(member.assignments.values()))
      })
    }

    return assignments
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
